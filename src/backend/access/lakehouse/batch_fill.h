/*-------------------------------------------------------------------------
 * batch_fill.h
 *    Typed batch → Datum fast path (M3-R1, 13-review perf item #3).
 *
 * Maps one projected batch column against its pg attribute and builds
 * Datums directly from the typed buffers — no per-cell CString and no
 * InputFunctionCall on the hot path. Compatibility is decided PER BATCH
 * (parquet files of one table may differ under schema evolution); any
 * combination without an exact fast mapping falls back to the caller's
 * input-function path over pg_datalake_rows_batch_cell_text, which
 * produces byte-identical text to the old per-row renderer.
 *-------------------------------------------------------------------------
 */
#ifndef LAKEHOUSE_BATCH_FILL_H
#define LAKEHOUSE_BATCH_FILL_H

#include "postgres.h"

#include "utils/rel.h"			/* Form_pg_attribute */
#include "fmgr.h"				/* FmgrInfo */

#include "pg_datalake_core.h"

/* fast-path targets chosen per (kind, atttypid, typmod) */
typedef enum LakehouseFastTarget
{
	LAKE_FAST_NONE = 0,			/* input-function fallback */
	LAKE_FAST_BOOL,
	LAKE_FAST_INT8,
	LAKE_FAST_INT4,
	LAKE_FAST_INT2,
	LAKE_FAST_FLOAT8,
	LAKE_FAST_TIMESTAMP,		/* timestamp or timestamptz, precision 6 or
								 * unset */
	LAKE_FAST_DATE,
	LAKE_FAST_TEXT,
	LAKE_FAST_VARCHAR,
	LAKE_FAST_BYTEA,
	LAKE_FAST_BPCHAR,
	LAKE_FAST_NAME,
} LakehouseFastTarget;

typedef struct LakehouseBatchMap
{
	LakehouseFastTarget target;
	FmgrInfo   *in_func;		/* fallback input function */
	Oid			typioparam;
	int32		typmod;
} LakehouseBatchMap;

/*
 * Decide the target once per batch for one attribute. `nrows` is the
 * batch's total row count (range prechecks scan the whole buffer).
 */
extern void lakehouse_batch_map_column(Form_pg_attribute attr,
									   const PgDatalakeBatchColumn *col,
									   uint64 nrows,
									   FmgrInfo *in_func, Oid typioparam,
									   LakehouseBatchMap *map);

/*
 * Build the Datum for live batch row `r` of a MAPPED non-NULL cell.
 * Varlena results are palloc'd in the caller's current context.
 */
extern Datum lakehouse_batch_col_datum(const LakehouseBatchMap *map,
									   const PgDatalakeBatchColumn *col,
									   uint32 r, const char *attname);

#endif							/* LAKEHOUSE_BATCH_FILL_H */
