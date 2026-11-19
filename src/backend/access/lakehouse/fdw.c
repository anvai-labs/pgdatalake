/*-------------------------------------------------------------------------
 *
 * fdw.c
 *		Lakehouse foreign-data wrapper handler (specs/lakehouse/
 *		01-architecture.md D1: external tables attach through an FDW and
 *		show up as ordinary pg_class rows — one catalog, one visibility).
 *
 * M1 slice 11: plan/iterate over REST-catalog tables through the FFI.
 * Planning opens a session to size the estimate, Begin re-opens it
 * (fail-fast per doc 03 §5), Iterate drives pg_datalake_rows_* over each
 * task's parquet file, converting cells with the attribute input
 * functions (doc 05 type mapping completes in the type-matrix slice).
 * No pushdown yet: quals are evaluated locally by the executor.
 *
 * Portions Copyright (c) 2026, Anvai Labs
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "fmgr.h"
#include "commands/defrem.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "optimizer/planmain.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/restrictinfo.h"
#include "utils/lsyscache.h"
#include "utils/fmgrprotos.h"
#include "tcop/tcopprot.h"
#include "miscadmin.h"
#include "optimizer/optimizer.h"
#include "utils/builtins.h"
#include "utils/rel.h"

#include "pg_datalake_core.h"

/* options for one foreign table (server opts + table opts, table wins) */
typedef struct LakehouseOptions
{
	char	   *endpoint;
	char	   *warehouse;
	char	   *token;
	char	   *namespace;		/* levels before the last '.' of the table opt */
	char	   *table;			/* last '.'-level of the table option */
	char	   *s3_endpoint;
	char	   *s3_access_key;
	char	   *s3_secret_key;
}			LakehouseOptions;

/* per-executor-node state */
typedef struct LakehouseExecState
{
	LakehouseOptions opts;
	ScanSession *session;
	RowsHandle *rows;
	uint64		current_task;
	uint64		task_count;
	char	   *projection_csv; /* visible attribute names, comma-joined */
	int			natts;
	FmgrInfo   *in_funcs;		/* per visible attribute */
	Oid		   *typioparams;
	int		   *slot_index;		/* attribute slot per projection column */
}			LakehouseExecState;

static void lakehouse_GetForeignRelSize(PlannerInfo *root,
										RelOptInfo *baserel,
										Oid foreigntableid);
static void lakehouse_GetForeignPaths(PlannerInfo *root,
									  RelOptInfo *baserel,
									  Oid foreigntableid);
static ForeignScan *lakehouse_GetForeignPlan(PlannerInfo *root,
											 RelOptInfo *baserel,
											 Oid foreigntableid,
											 ForeignPath *best_path,
											 List *tlist,
											 List *scan_clauses,
											 Plan *outer_plan);
static void lakehouse_BeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *lakehouse_IterateForeignScan(ForeignScanState *node);
static void lakehouse_ReScanForeignScan(ForeignScanState *node);
static void lakehouse_EndForeignScan(ForeignScanState *node);

PG_FUNCTION_INFO_V1(lakehouse_fdw_handler);

Datum
lakehouse_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *routine = makeNode(FdwRoutine);

	routine->GetForeignRelSize = lakehouse_GetForeignRelSize;
	routine->GetForeignPaths = lakehouse_GetForeignPaths;
	routine->GetForeignPlan = lakehouse_GetForeignPlan;
	routine->BeginForeignScan = lakehouse_BeginForeignScan;
	routine->IterateForeignScan = lakehouse_IterateForeignScan;
	routine->ReScanForeignScan = lakehouse_ReScanForeignScan;
	routine->EndForeignScan = lakehouse_EndForeignScan;

	PG_RETURN_POINTER(routine);
}

/*
 * Merge server + table options (table wins). The Iceberg namespace is the
 * part before the last '.' of the table option; the table name is the rest
 * (doc 03 §3: last namespace level == Postgres schema).
 */
static void
ExtractOptions(Oid foreigntableid, LakehouseOptions * opts)
{
	ForeignServer *server;
	ForeignTable *table;
	ListCell   *lc;
	char	   *table_opt = NULL;

	memset(opts, 0, sizeof(*opts));
	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);

	foreach(lc, server->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "endpoint") == 0)
			opts->endpoint = defGetString(def);
		else if (strcmp(def->defname, "warehouse") == 0)
			opts->warehouse = defGetString(def);
		else if (strcmp(def->defname, "token") == 0)
			opts->token = defGetString(def);
		else if (strcmp(def->defname, "s3_endpoint") == 0)
			opts->s3_endpoint = defGetString(def);
		else if (strcmp(def->defname, "s3_access_key") == 0)
			opts->s3_access_key = defGetString(def);
		else if (strcmp(def->defname, "s3_secret_key") == 0)
			opts->s3_secret_key = defGetString(def);
	}
	foreach(lc, table->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "table") == 0)
			table_opt = defGetString(def);
		else if (strcmp(def->defname, "s3_endpoint") == 0)
			opts->s3_endpoint = defGetString(def);
		else if (strcmp(def->defname, "s3_access_key") == 0)
			opts->s3_access_key = defGetString(def);
		else if (strcmp(def->defname, "s3_secret_key") == 0)
			opts->s3_secret_key = defGetString(def);
	}
	if (table_opt == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				 errmsg("lakehouse foreign table requires a \"table\" option")));
	{
		/* namespace = everything before the last '.'; table = after it */
		const char *last_dot = strrchr(table_opt, '.');

		if (last_dot != NULL)
		{
			opts->namespace = pnstrdup(table_opt, last_dot - table_opt);
			opts->table = pstrdup(last_dot + 1);
		}
		else
		{
			opts->namespace = pstrdup("");
			opts->table = pstrdup(table_opt);
		}
	}
}

static ScanSession *
OpenSession(LakehouseOptions * opts)
{
	char		errbuf[1024];
	ScanSession *session;

	errbuf[0] = '\0';
	session = pg_datalake_scan_new(opts->endpoint,
								   opts->warehouse,
								   opts->token ? opts->token : "",
								   opts->namespace,
								   opts->table,
								   opts->s3_endpoint,
								   opts->s3_access_key,
								   opts->s3_secret_key,
								   errbuf,
								   sizeof(errbuf));
	if (session == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg("lakehouse: could not plan scan: %s", errbuf)));
	return session;
}

static void
lakehouse_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
							Oid foreigntableid)
{
	LakehouseOptions opts;
	ScanSession *session;
	uint64		task_count;
	double		total_rows = 0;

	ExtractOptions(foreigntableid, &opts);
	session = OpenSession(&opts);
	task_count = pg_datalake_scan_task_count(session);
	for (uint64 i = 0; i < task_count; i++)
		total_rows += (double) pg_datalake_scan_task_rows(session, i);
	pg_datalake_scan_free(session);

	/* estimates; actual rows are known only after execution */
	baserel->rows = clamp_row_est(total_rows);
	baserel->pages = clamp_row_est(total_rows / 200.0 + 10);
}

static void
lakehouse_GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
						  Oid foreigntableid)
{
	ForeignPath *path;

	/*
	 * One simple scan path; no pushdown yet (quals evaluated locally by the
	 * executor — doc 06 §4 pushdown lands as its own slice).
	 */
	path = create_foreignscan_path(root, baserel, NULL,
								   baserel->rows, 0,
								   0.0,
								   10.0 * baserel->pages +
								   10.0 * baserel->rows * cpu_operator_cost,
								   NIL, NULL, NULL, NIL, NIL);
	add_path(baserel, &path->path);
}

static ForeignScan *
lakehouse_GetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,
						 Oid foreigntableid, ForeignPath *best_path,
						 List *tlist, List *scan_clauses, Plan *outer_plan)
{
	/* keep all quals local (no pushdown yet, doc 06 §4) */
	scan_clauses = extract_actual_clauses(scan_clauses, false);
	return make_foreignscan(tlist, NIL, baserel->relid,
							NIL, NIL, NIL, scan_clauses, outer_plan);
}

static void
lakehouse_BeginForeignScan(ForeignScanState *node, int eflags)
{
	Relation	rel = node->ss.ss_currentRelation;
	LakehouseOptions opts;
	LakehouseExecState *state;
	TupleDesc	tupdesc = RelationGetDescr(rel);
	StringInfoData proj;
	bool		first_col = true;

	ExtractOptions(RelationGetRelid(rel), &opts);

	state = (LakehouseExecState *) palloc0(sizeof(LakehouseExecState));
	state->natts = tupdesc->natts;
	state->in_funcs = (FmgrInfo *) palloc0(tupdesc->natts * sizeof(FmgrInfo));
	state->typioparams = (Oid *) palloc0(tupdesc->natts * sizeof(Oid));
	state->slot_index = (int *) palloc0(tupdesc->natts * sizeof(int));

	/* projection: visible attribute names, in attribute order */
	initStringInfo(&proj);
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
		appendStringInfoString(&proj, NameStr(attr->attname));
		first_col = false;
		getTypeInputInfo(attr->atttypid,
						 &state->in_funcs[i].fn_oid,
						 &state->typioparams[i]);
		fmgr_info(state->in_funcs[i].fn_oid, &state->in_funcs[i]);
	}
	/* renumber projection columns (dropped slots don't occupy positions) */
	{
		int			proj_col = 0;

		for (int i = 0; i < tupdesc->natts; i++)
			if (state->slot_index[i] >= 0)
				state->slot_index[i] = proj_col++;
	}
	state->projection_csv = proj.data;
	state->opts = opts;
	state->opts.endpoint = opts.endpoint ? pstrdup(opts.endpoint) : NULL;
	state->opts.warehouse = opts.warehouse ? pstrdup(opts.warehouse) : NULL;
	state->opts.token = opts.token ? pstrdup(opts.token) : NULL;
	state->opts.namespace = pstrdup(opts.namespace);
	state->opts.table = pstrdup(opts.table);
	state->opts.s3_endpoint = opts.s3_endpoint ? pstrdup(opts.s3_endpoint) : NULL;
	state->opts.s3_access_key = opts.s3_access_key ? pstrdup(opts.s3_access_key) : NULL;
	state->opts.s3_secret_key = opts.s3_secret_key ? pstrdup(opts.s3_secret_key) : NULL;

	state->session = OpenSession(&state->opts);
	state->task_count = pg_datalake_scan_task_count(state->session);
	state->rows = NULL;
	state->current_task = 0;

	node->fdw_state = state;
}

static TupleTableSlot *
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

			if (state->current_task >= state->task_count)
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

		if (pg_datalake_rows_next(state->rows))
		{
			Datum	   *values = (Datum *) palloc(tupdesc->natts * sizeof(Datum));
			bool	   *nulls = (bool *) palloc(tupdesc->natts * sizeof(bool));
			HeapTuple	tuple;

			for (int i = 0; i < tupdesc->natts; i++)
			{
				int			pcol = state->slot_index[i];

				if (pcol < 0)
				{
					values[i] = PointerGetDatum(NULL);
					nulls[i] = true;
					continue;
				}
				if (pg_datalake_rows_col_null(state->rows, pcol))
				{
					values[i] = PointerGetDatum(NULL);
					nulls[i] = true;
					continue;
				}
				values[i] = InputFunctionCall(&state->in_funcs[i],
											  pg_datalake_rows_col_text(state->rows,
																		pcol),
											  state->typioparams[i], -1);
				nulls[i] = false;
			}
			tuple = heap_form_tuple(tupdesc, values, nulls);
			pfree(values);
			pfree(nulls);
			return ExecStoreHeapTuple(tuple, slot, false);
		}

		/* task exhausted: advance to the next one */
		pg_datalake_rows_close(state->rows);
		state->rows = NULL;
		state->current_task++;
	}
}

static void
lakehouse_ReScanForeignScan(ForeignScanState *node)
{
	LakehouseExecState *state = (LakehouseExecState *) node->fdw_state;

	if (state->rows != NULL)
	{
		pg_datalake_rows_close(state->rows);
		state->rows = NULL;
	}
	if (state->session != NULL)
	{
		pg_datalake_scan_free(state->session);
	}
	/* fresh plan: the remote snapshot may have moved since the last scan */
	state->session = OpenSession(&state->opts);
	state->task_count = pg_datalake_scan_task_count(state->session);
	state->current_task = 0;
}

static void
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
