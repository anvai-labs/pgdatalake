/*---------------------------------------------------------------------------
 * ANALYZE support for lakehouse foreign tables (spec 19 §6.5, family E
 * residual): populates pg_class.reltuples + pg_statistic (per-column NDV,
 * MCV, histogram) so join and qual selectivity stop riding core's
 * DEFAULT_NUM_DISTINCT(200) fallback.
 *
 * The 2026-11-26 q4 profile showed the residual gap exactly: with relsize
 * (metadata rows) the CTE build side planned sanely, but the outer six-way
 * CTE self-join stayed Nested Loop because the filtered scan estimates
 * (~150 rows) used the default NDV — real NDV is ~285k per (channel, year)
 * combination. This module gives the standard ANALYZE machinery its rows.
 *
 * Sampling: reservoir (Algorithm R) over rows of whole parquet tasks in
 * randomized task order, reading until rows_seen >= 4×targrows or the
 * table is exhausted. Whole-task reads keep the batch fast path (one
 * map_column call per batch); task randomization removes file-order bias.
 * Data materialization mirrors lakehouse_BeginForeignScan (batch_fill
 * fast path with input-function fallback) — byte-identical text semantics.
 *
 * Failure contract: same fail-fast class as OpenSession (doc 03 §5) — an
 * unreachable endpoint ereports at elevel and ANALYZE reports the failure
 * (ANALYZE is explicit maintenance, not query hot-path: no silent decline).
 *---------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/table.h"
#include "commands/vacuum.h"
#include "fmgr.h"
#include "foreign/fdwapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "batch_fill.h"
#include "lakehouse_fdw.h"
#include "pg_datalake_core.h"

#define ANALYZE_ROWS_FACTOR 4

static void analyze_setup_column_map(Relation rel,
									 FmgrInfo **in_funcs, Oid **typioparams,
									 int **slot_index,
									 LakehouseBatchMap **maps,
									 char **projection_json);

/*
 * Build the per-attribute input plumbing and the JSON projection exactly
 * like BeginForeignScan does (attribute order, escape_json per name,
 * dropped attributes mapped to -1).
 */
static void
analyze_setup_column_map(Relation rel,
						 FmgrInfo **in_funcs, Oid **typioparams,
						 int **slot_index, LakehouseBatchMap **maps,
						 char **projection_json)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	StringInfoData proj;
	int			proj_col = 0;

	*in_funcs = (FmgrInfo *) palloc0(tupdesc->natts * sizeof(FmgrInfo));
	*typioparams = (Oid *) palloc0(tupdesc->natts * sizeof(Oid));
	*slot_index = (int *) palloc0(tupdesc->natts * sizeof(int));
	*maps = (LakehouseBatchMap *) palloc0(tupdesc->natts *
										  sizeof(LakehouseBatchMap));

	initStringInfo(&proj);
	appendStringInfoChar(&proj, '[');
	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		if (attr->attisdropped)
		{
			(*slot_index)[i] = -1;
			continue;
		}
		if (proj_col > 0)
			appendStringInfoChar(&proj, ',');
		escape_json(&proj, NameStr(attr->attname));
		(*slot_index)[i] = proj_col++;
		getTypeInputInfo(attr->atttypid,
						 &(*in_funcs)[i].fn_oid,
						 &(*typioparams)[i]);
		fmgr_info((*in_funcs)[i].fn_oid, &(*in_funcs)[i]);
	}
	appendStringInfoChar(&proj, ']');
	*projection_json = proj.data;
}

/*
 * Estimate the table size in page-equivalents for the planner (same
 * ~100-rows-per-page model as GetForeignPaths' relsize slice).
 */
bool
lakehouse_AnalyzeForeignTable(Relation rel, AcquireSampleRowsFunc *func,
							  BlockNumber *totalpages)
{
	Oid			relid = RelationGetRelid(rel);
	LakehouseOptions opts;
	ScanSession *session;
	int64		rows = 0;

	ExtractOptions(relid, GetUserId(), &opts);
	session = lakehouse_OpenSession(&opts, NULL);
	for (uint64 i = 0; i < pg_datalake_scan_task_count(session); i++)
		rows += pg_datalake_scan_task_rows(session, i);
	pg_datalake_scan_free(session);

	*totalpages = (BlockNumber) Max(1, (int) Min(rows / 100,
												 (double) INT_MAX));
	*func = lakehouse_AcquireSampleRows;
	return true;
}

/*
 * Standard FDW sample contract: fill rows[] with up to targrows tuples and
 * return how many were acquired. Rows are HeapTuples over rel's descriptor.
 */
int
lakehouse_AcquireSampleRows(Relation rel, int elevel,
							HeapTuple *rows, int targrows,
							double *totalrows, double *totaldeadrows)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	Oid			relid = RelationGetRelid(rel);
	LakehouseOptions opts;
	ScanSession *volatile session = NULL;
	RowsHandle *volatile rh = NULL;
	FmgrInfo   *in_funcs;
	Oid		   *typioparams;
	int		   *slot_index;
	LakehouseBatchMap *maps;
	Datum	   *row_values;
	bool	   *row_nulls;
	char	   *projection_json;
	char		errbuf[1024];
	uint64		ntasks;
	int64		total_rows = 0;
	uint64		streamed = 0;
	int			nrows = 0;
	PgDatalakeRowBatch batch;
	bool		batch_valid = false;
	uint32		batch_cursor = 0;
	uint64		task_pos = 0;
	uint64	   *task_order;
	uint64		stop_at;

	/*
	 * The acquire hook receives core's elevel (DEBUG2 for plain ANALYZE) but
	 * task-level IO failures must longjmp, not return: at elevel < ERROR
	 * ereport() returns and the caller would dereference NULL handles
	 * (executor precedent: hardcoded ERROR so "a caught panic is never
	 * mistaken for EOF", fdw_exec.c 13-review #8). ANALYZE is explicit
	 * maintenance — fail fast.
	 */
	(void) elevel;

	PG_TRY();
	{
		ExtractOptions(relid, GetUserId(), &opts);
		errbuf[0] = '\0';
		session = lakehouse_OpenSession(&opts, NULL);

		ntasks = pg_datalake_scan_task_count(session);
		for (uint64 i = 0; i < ntasks; i++)
			total_rows += pg_datalake_scan_task_rows(session, i);

		/* randomized task order: whole-task reads without file-order bias */
		task_order = (uint64 *) palloc(sizeof(uint64) * ntasks);
		for (uint64 i = 0; i < ntasks; i++)
			task_order[i] = i;
		for (uint64 i = ntasks; i > 1; i--)
		{
			uint64		j = (uint64) (random() % (long) i);
			uint64		tmp = task_order[i - 1];

			task_order[i - 1] = task_order[j];
			task_order[j] = tmp;
		}

		analyze_setup_column_map(rel, &in_funcs, &typioparams, &slot_index,
								 &maps, &projection_json);
		row_values = (Datum *) palloc(tupdesc->natts * sizeof(Datum));
		row_nulls = (bool *) palloc(tupdesc->natts * sizeof(bool));

		/*
		 * Stream rows in randomized task order until 4x the target is seen
		 * (or the table is exhausted), filling first and then replacing via
		 * reservoir (Algorithm R) — the replacement branch keeps the sample
		 * unbiased regardless of where the 4x stop cut the stream.
		 */
		stop_at = (uint64) targrows * ANALYZE_ROWS_FACTOR;

		while (task_pos < ntasks &&
			   (nrows == 0 || streamed < stop_at))
		{
			if (rh == NULL)
			{
				errbuf[0] = '\0';
				rh = pg_datalake_rows_open(session, task_order[task_pos],
										   projection_json,
										   errbuf, sizeof(errbuf));
				if (rh == NULL)
					ereport(ERROR,
							(errcode(ERRCODE_DATA_EXCEPTION),
							 errmsg("lakehouse: ANALYZE could not open task %llu: %s",
									(unsigned long long) task_order[task_pos],
									errbuf[0] != '\0' ? errbuf : "unknown error")));
				batch_valid = false;
				batch_cursor = 0;
			}

			/*
			 * Executor-shaped advance (fdw_exec.c IterateForeignScan):
			 * batch_next on the LIVE handle serves the next arrow batch; EOF
			 * closes the task and moves on. A caught failure is never
			 * mistaken for EOF.
			 */
			if (!batch_valid || batch_cursor >= batch.live_count)
			{
				int			nst;

				errbuf[0] = '\0';
				nst = pg_datalake_rows_batch_next(rh, &batch,
												  errbuf, sizeof(errbuf));
				if (nst == 2)
					ereport(ERROR,
							(errcode(ERRCODE_DATA_EXCEPTION),
							 errmsg("lakehouse: ANALYZE row read failed in task %llu: %s",
									(unsigned long long) task_order[task_pos],
									errbuf[0] != '\0' ? errbuf : "unknown error")));
				if (nst == 1)
				{
					pg_datalake_rows_close(rh);
					rh = NULL;
					task_pos++;
					continue;
				}
				batch_valid = true;
				batch_cursor = 0;
				for (int i = 0; i < tupdesc->natts; i++)
				{
					int			pcol = slot_index[i];

					if (pcol < 0)
						continue;
					lakehouse_batch_map_column(TupleDescAttr(tupdesc, i),
											   &batch.cols[pcol],
											   batch.rows,
											   &in_funcs[i],
											   typioparams[i],
											   &maps[i]);
				}
			}

			{
				uint32		r = batch.live_idx[batch_cursor];

				for (int i = 0; i < tupdesc->natts; i++)
				{
					int			pcol = slot_index[i];

					if (pcol < 0)
					{
						row_values[i] = PointerGetDatum(NULL);
						row_nulls[i] = true;
						continue;
					}
					{
						const PgDatalakeBatchColumn *col =
							&batch.cols[pcol];

						if (col->nulls[r])
						{
							row_values[i] = PointerGetDatum(NULL);
							row_nulls[i] = true;
							continue;
						}
						if (maps[i].target != LAKE_FAST_NONE)
						{
							row_values[i] =
								lakehouse_batch_col_datum(&maps[i], col, r,
														  TupleDescAttr(tupdesc, i)->attname.data);
							row_nulls[i] = false;
						}
						else
						{
							char		cell_err[1024];
							const char *text;

							cell_err[0] = '\0';
							text = pg_datalake_rows_batch_cell_text(rh,
																	pcol, r,
																	cell_err,
																	sizeof(cell_err));

							if (text == NULL)
								ereport(ERROR,
										(errcode(ERRCODE_DATA_EXCEPTION),
										 errmsg("lakehouse: ANALYZE row read failed in task %llu: %s",
												(unsigned long long) task_order[task_pos],
												cell_err[0] != '\0' ? cell_err : "unknown error")));
							row_values[i] =
								InputFunctionCall(&in_funcs[i],
#if PG_VERSION_NUM < 200000
												  pstrdup(text),
#else
												  text,
#endif
												  typioparams[i],
												  TupleDescAttr(tupdesc, i)->atttypmod);
							row_nulls[i] = false;
						}
					}
				}
				batch_cursor++;
				streamed++;

				/* reservoir (Algorithm R): unbiased over the stream */
				if (nrows < targrows)
					rows[nrows++] = heap_form_tuple(tupdesc, row_values,
													row_nulls);
				else
				{
					long		j = random() % (long) streamed;

					if (j < targrows)
					{
						heap_freetuple(rows[j]);
						rows[j] = heap_form_tuple(tupdesc, row_values,
												  row_nulls);
					}
				}
			}
		}
	}
	PG_CATCH();
	{
		if (rh != NULL)
			pg_datalake_rows_close(rh);
		pg_datalake_scan_free(session);
		PG_RE_THROW();
	}
	PG_END_TRY();

	ereport(DEBUG1,
			(errmsg("lakehouse ANALYZE \"%s\": sampled %d of %lld rows",
					RelationGetRelationName(rel), nrows,
					(long long) total_rows)));

	*totalrows = (double) total_rows;
	*totaldeadrows = 0.0;

	return nrows;
}
