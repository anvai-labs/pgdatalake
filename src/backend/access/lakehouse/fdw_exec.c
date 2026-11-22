/*
 * lakehouse FDW — execution hooks (specs/lakehouse/06-execution.md). Split out of fdw.c — see
 * lakehouse_fdw.h for the module map and HANDOFF for the decomposition
 * plan. Include surface deliberately mirrors fdw.c (superset is
 * harmless in C).
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_collation_d.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/explain_format.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/primnodes.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/paths.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "parser/parse_collate.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/json.h"
#include "port/atomics.h"

#include "lakehouse_fdw.h"
#include "batch_fill.h"
#include "pg_datalake_core.h"

void
lakehouse_BeginForeignScan(ForeignScanState *node, int eflags)
{
	Relation	rel = node->ss.ss_currentRelation;
	LakehouseOptions opts;
	LakehouseExecState *state;
	TupleDesc	tupdesc;
	StringInfoData proj;
	bool		first_col = true;

	/*
	 * COUNT(*) elision (spec 15 §6.6) is GATED OFF (spec 16, "COUNT must be
	 * computed at execution"): the plan-time record_count Const only
	 * revalidated the SUM in Begin — never delete eligibility — so a
	 * same-total snapshot change with deletes overcounted. The successor is
	 * an execution-time one-row upper path.
	 */

	tupdesc = RelationGetDescr(rel);

	ExtractOptions(RelationGetRelid(rel),
				   ((ForeignScan *) node->ss.ps.plan)->checkAsUser, &opts);

	state = (LakehouseExecState *) palloc0(sizeof(LakehouseExecState));
	state->natts = tupdesc->natts;
	state->in_funcs = (FmgrInfo *) palloc0(tupdesc->natts * sizeof(FmgrInfo));
	state->typioparams = (Oid *) palloc0(tupdesc->natts * sizeof(Oid));
	state->slot_index = (int *) palloc0(tupdesc->natts * sizeof(int));
	state->maps = (LakehouseBatchMap *) palloc0(tupdesc->natts * sizeof(LakehouseBatchMap));
	state->row_values = (Datum *) palloc(tupdesc->natts * sizeof(Datum));
	state->row_nulls = (bool *) palloc(tupdesc->natts * sizeof(bool));
	state->batch_valid = false;

	/*
	 * projection: visible attribute names, in attribute order — a counted
	 * JSON ARRAY, not comma-separated: column names may contain commas
	 * (13-review #8b)
	 */
	initStringInfo(&proj);
	appendStringInfoChar(&proj, '[');
	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		if (attr->attisdropped)
		{
			state->slot_index[i] = -1;
			continue;
		}
		if (!first_col)
			appendStringInfoChar(&proj, ',');
		escape_json(&proj, NameStr(attr->attname)); /* emits its own quotes */
		first_col = false;
		getTypeInputInfo(attr->atttypid,
						 &state->in_funcs[i].fn_oid,
						 &state->typioparams[i]);
		fmgr_info(state->in_funcs[i].fn_oid, &state->in_funcs[i]);
	}
	appendStringInfoChar(&proj, ']');

	/* renumber projection columns (dropped slots don't occupy positions) */
	{
		int			proj_col = 0;

		for (int i = 0; i < tupdesc->natts; i++)
			if (state->slot_index[i] >= 0)
				state->slot_index[i] = proj_col++;
	}
	state->projection_csv = proj.data;

	/*
	 * slice 15/19: translated-quals JSON + time travel selector ride in
	 * fdw_private (plan -> executor) as makeString elements.
	 */
	state->parallel = IsParallelWorker();
	if (node->ss.ps.plan != NULL)
	{
		ForeignScan *fscan = (ForeignScan *) node->ss.ps.plan;

		if (fscan->fdw_private != NIL)
		{
			char	   *pred_private = strVal(linitial(fscan->fdw_private));

			state->predicate_json = pred_private[0] != '\0'
				? pstrdup(pred_private)
				: NULL;
			if (list_length(fscan->fdw_private) >= 2)
			{
				char	   *sel_private = strVal(lsecond(fscan->fdw_private));

				state->rte_selector = sel_private[0] != '\0'
					? pstrdup(sel_private)
					: NULL;
			}
		}
	}
	state->opts = opts;
	state->opts.endpoint = opts.endpoint ? pstrdup(opts.endpoint) : NULL;
	state->opts.warehouse = opts.warehouse ? pstrdup(opts.warehouse) : NULL;
	state->opts.token = opts.token ? pstrdup(opts.token) : NULL;
	state->opts.namespace = pstrdup(opts.namespace);
	state->opts.table = pstrdup(opts.table);
	state->opts.s3_endpoint = opts.s3_endpoint ? pstrdup(opts.s3_endpoint) : NULL;
	state->opts.s3_access_key = opts.s3_access_key ? pstrdup(opts.s3_access_key) : NULL;
	state->opts.s3_secret_key = opts.s3_secret_key ? pstrdup(opts.s3_secret_key) : NULL;

	/* predicate_json must be set on opts BEFORE OpenSession (slice 15) */
	if (state->predicate_json != NULL)
		state->opts.predicate_json = state->predicate_json;
	state->session = lakehouse_OpenSession(&state->opts, state->rte_selector);
	state->task_count = pg_datalake_scan_task_count(state->session);
	state->rows = NULL;
	state->current_task = 0;

	node->fdw_state = state;
}

TupleTableSlot *
lakehouse_IterateForeignScan(ForeignScanState *node)
{
	LakehouseExecState *state = (LakehouseExecState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	TupleDesc	tupdesc = node->ss.ss_currentRelation->rd_att;

	for (;;)
	{
		if (state->rows == NULL)
		{
			char		errbuf[1024];
			const char *file;

			if (state->shared != NULL)
			{
				/* parallel: atomically claim the next task (slice 17) */
				uint64		claimed =
					pg_atomic_fetch_add_u64(&state->shared->next_task,
											1);

				if (claimed >= state->task_count)
					return ExecClearTuple(slot);
				state->current_task = claimed;
			}
			else if (state->current_task >= state->task_count)
				return ExecClearTuple(slot);

			errbuf[0] = '\0';
			file = pg_datalake_scan_task_file(state->session,
											  state->current_task);
			state->rows = pg_datalake_rows_open(state->session,
												state->current_task,
												state->projection_csv,
												errbuf,
												sizeof(errbuf));
			if (state->rows == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_IO_ERROR),
						 errmsg("lakehouse: could not open task %llu (%s): %s",
								(unsigned long long) state->current_task,
								file, errbuf)));
		}

		/*
		 * M3-R1: batch cursor — one pg_datalake_rows_batch_next call per
		 * arrow batch; rows are served from the materialized typed views with
		 * Datums built directly (batch_fill.c). The explicit ROW/EOF/ERROR
		 * tri-state carries over: EOF advances the task, ERROR aborts the
		 * statement (13-review #8 — a caught panic is never mistaken for
		 * EOF).
		 */
		{
			char		next_err[1024];
			int			nst;

			if (!state->batch_valid || state->batch_cursor >= state->batch.live_count)
			{
				next_err[0] = '\0';
				nst = pg_datalake_rows_batch_next(state->rows, &state->batch,
												  next_err, sizeof(next_err));
				if (nst == 2)
					ereport(ERROR,
							(errcode(ERRCODE_DATA_EXCEPTION),
							 errmsg("lakehouse: row read failed in task %llu: %s",
									(unsigned long long) state->current_task,
									next_err[0] != '\0' ? next_err : "unknown error")));
				if (nst == 1)
				{
					/* task exhausted: advance to the next one */
					pg_datalake_rows_close(state->rows);
					state->rows = NULL;
					state->batch_valid = false;
					state->current_task++;
					continue;
				}
				state->batch_valid = true;
				state->batch_cursor = 0;
				/* per-batch fast/fallback decision for every attribute */
				for (int i = 0; i < tupdesc->natts; i++)
				{
					int			pcol = state->slot_index[i];

					if (pcol < 0)
						continue;
					lakehouse_batch_map_column(TupleDescAttr(tupdesc, i),
											   &state->batch.cols[pcol],
											   state->batch.rows,
											   &state->in_funcs[i],
											   state->typioparams[i],
											   &state->maps[i]);
				}
			}

			{
				uint32		r = state->batch.live_idx[state->batch_cursor];
				HeapTuple	tuple;

				for (int i = 0; i < tupdesc->natts; i++)
				{
					int			pcol = state->slot_index[i];

					if (pcol < 0)
					{
						state->row_values[i] = PointerGetDatum(NULL);
						state->row_nulls[i] = true;
						continue;
					}
					{
						const		PgDatalakeBatchColumn *col =
							&state->batch.cols[pcol];

						if (col->nulls[r])
						{
							state->row_values[i] = PointerGetDatum(NULL);
							state->row_nulls[i] = true;
							continue;
						}
						if (state->maps[i].target != LAKE_FAST_NONE)
						{
							state->row_values[i] =
								lakehouse_batch_col_datum(&state->maps[i], col, r,
														  TupleDescAttr(tupdesc, i)->attname.data);
							state->row_nulls[i] = false;
						}
						else
						{
							char		cell_err[1024];
							const char *text;

							cell_err[0] = '\0';
							text = pg_datalake_rows_batch_cell_text(state->rows,
																	pcol, r,
																	cell_err,
																	sizeof(cell_err));

							if (text == NULL)
								ereport(ERROR,
										(errcode(ERRCODE_DATA_EXCEPTION),
										 errmsg("lakehouse: row read failed in task %llu: %s",
												(unsigned long long) state->current_task,
												cell_err[0] != '\0' ? cell_err : "unknown error")));
							/* PG19 input functions take writable strings. */
							state->row_values[i] =
								InputFunctionCall(&state->in_funcs[i],
#if PG_VERSION_NUM < 200000
												  pstrdup(text),
#else
												  text,
#endif
												  state->typioparams[i],
												  TupleDescAttr(tupdesc, i)->atttypmod);
							state->row_nulls[i] = false;
						}
					}
				}
				state->batch_cursor++;
				tuple = heap_form_tuple(tupdesc, state->row_values, state->row_nulls);
				return ExecStoreHeapTuple(tuple, slot, false);
			}
		}

	}
}

/*
 * EXPLAIN visibility (00-goals #3, doc 01 §4): show what the scan will
 * actually do — catalog endpoint, table identity, task count, rows.
 */
void
lakehouse_ExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	LakehouseExecState *state = (LakehouseExecState *) node->fdw_state;
	uint64		rows = 0;

	if (state == NULL)
		return;
	for (uint64 i = 0; i < state->task_count; i++)
		rows += (uint64) pg_datalake_scan_task_rows(state->session, i);

	ExplainPropertyText("Lakehouse table", state->opts.namespace[0]
						? psprintf("%s.%s", state->opts.namespace,
								   state->opts.table)
						: state->opts.table, es);
	ExplainPropertyText("Lakehouse endpoint", state->opts.endpoint, es);
	{
		uint64		before =
			pg_datalake_scan_files_before_pruning(state->session);
		uint64		after = pg_datalake_scan_files_after_pruning(state->session);

		ExplainPropertyInteger("Iceberg files examined", NULL,
							   (int64) before, es);
		if (after < before)
			ExplainPropertyInteger("Iceberg files pruned", NULL,
								   (int64) (before - after), es);
	}
	ExplainPropertyInteger("Iceberg scan tasks", NULL,
						   (int64) state->task_count, es);
	ExplainPropertyInteger("Iceberg rows (parquet metadata)", NULL,
						   (int64) rows, es);
	ExplainPropertyText("Projection", state->projection_csv[0]
						? state->projection_csv : "(all columns)", es);
	ExplainPropertyText("Pushdown", "projection only; quals evaluated locally (M1)",
						es);
}

void
lakehouse_ReScanForeignScan(ForeignScanState *node)
{
	LakehouseExecState *state = (LakehouseExecState *) node->fdw_state;

	/*
	 * Rewind only: the planned session is REUSED. Merge/NestLoop join inner
	 * rescans call this many times per statement — a full re-plan here (the
	 * original implementation) costs a catalog round-trip + manifest fetch
	 * PER RESCAN and made join-shaped TPC-DS queries take minutes (caught by
	 * the M1 gate, slice 23). Within one statement the remote snapshot cannot
	 * move underneath us in any visible way: the scan is pinned to the plan's
	 * snapshot at open time.
	 */
	if (state->rows != NULL)
	{
		pg_datalake_rows_close(state->rows);
		state->rows = NULL;
	}
	state->batch_valid = false;
	if (state->session == NULL)
	{
		state->session = lakehouse_OpenSession(&state->opts, state->rte_selector);
		state->task_count = pg_datalake_scan_task_count(state->session);
	}
	state->current_task = 0;
}

bool
lakehouse_IsForeignScanParallelSafe(PlannerInfo *root, RelOptInfo *baserel,
									RangeTblEntry *rte)
{
#ifdef LAKEHOUSE_STOCK_FDW
	/* The initial stock profile qualifies a single execution-time pin. */
	return false;
#else
	/* process-local reads over object storage: always safe (doc 06 §3) */
	return true;
#endif
}

Size
lakehouse_EstimateDSMForeignScan(ForeignScanState *node, ParallelContext *pcxt)
{
	return sizeof(LakehouseShared);
}

/*
 * Re-open THIS node's session pinned to the leader's concrete snapshot
 * (13-review #7): every process must plan the SAME immutable snapshot,
 * or task index N can mean different files under a concurrent commit.
 * Cheap when ids already match (no commit raced us).
 */
static void
lakehouse_repin_session(LakehouseExecState *state, uint64 snapshot_id)
{
	char		sel[64];

	Assert(state != NULL);
	if (state->session != NULL &&
		(uint64) pg_datalake_scan_snapshot_id(state->session) == snapshot_id)
		return;
	snprintf(sel, sizeof(sel), "{\"kind\":\"id\",\"value\":%llu}",
			 (unsigned long long) snapshot_id);
	if (state->session != NULL)
	{
		pg_datalake_scan_free(state->session);
		state->session = NULL;
	}
	state->rte_selector = pstrdup(sel);
	state->session = lakehouse_OpenSession(&state->opts, state->rte_selector);
	state->task_count = pg_datalake_scan_task_count(state->session);
	state->rows = NULL;
	state->current_task = 0;
}

void
lakehouse_InitializeDSMForeignScan(ForeignScanState *node,
								   ParallelContext *pcxt, void *coordinate)
{
	LakehouseShared *shared = (LakehouseShared *) coordinate;

	pg_atomic_init_u64(&shared->next_task, 0);

	/*
	 * Begin (per-execution pin — the plancache-fresh boundary) ran before
	 * this hook everywhere: children initialize before the Gather parent. The
	 * leader's session therefore holds the just-resolved snapshot; record it
	 * for the workers, who re-pin in their own Initialize hook (13-review
	 * #7).
	 */
	{
		LakehouseExecState *state = (LakehouseExecState *) node->fdw_state;

		shared->snapshot_id = (state != NULL && state->session != NULL)
			? (uint64) pg_datalake_scan_snapshot_id(state->session)
			: 0;
		if (state != NULL)
		{
			state->shared = shared;
			/* ids already match by construction on the leader */
		}
	}
}

void
lakehouse_InitializeWorkerForeignScan(ForeignScanState *node, shm_toc *toc,
									  void *coordinate)
{
	LakehouseShared *shared = (LakehouseShared *) coordinate;
	LakehouseExecState *state = (LakehouseExecState *) node->fdw_state;

	/*
	 * Begin already ran here (children init before the Gather parent in
	 * workers too), so fdw_state carries this node's real state: attach the
	 * node's queue and re-pin the session to the leader's snapshot. (An early
	 * state is only a defensive fallback.)
	 */
	if (state == NULL)
	{
		state = (LakehouseExecState *) palloc0(sizeof(LakehouseExecState));
		node->fdw_state = state;
	}
	state->shared = shared;
	if (shared->snapshot_id != 0)
		lakehouse_repin_session(state, shared->snapshot_id);
}

void
lakehouse_ShutdownForeignScan(ForeignScanState *node)
{
	/* clear THIS node's coordination only (13-review #7) */
	LakehouseExecState *state = (LakehouseExecState *) node->fdw_state;

	if (state != NULL)
		state->shared = NULL;
}

void
lakehouse_EndForeignScan(ForeignScanState *node)
{
	LakehouseExecState *state = (LakehouseExecState *) node->fdw_state;

	if (state == NULL)
		return;
	if (state->rows != NULL)
	{
		pg_datalake_rows_close(state->rows);
		state->rows = NULL;
	}
	if (state->session != NULL)
	{
		pg_datalake_scan_free(state->session);
		state->session = NULL;
	}
}
