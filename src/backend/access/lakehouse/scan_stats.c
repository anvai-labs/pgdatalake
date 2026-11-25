/*---------------------------------------------------------------------------
 * Scan statistics collector (spec 19 §4b, layer L0 — physical stats).
 *
 * One row per completed foreign-scan executor, keyed by (dbid, relid,
 * endpoint, namespace.table, snapshot_id), flushed per transaction into
 * lakehouse.table_stats — an EXTENSION-INTERNAL table (REVOKE ALL FROM
 * PUBLIC; inserts run as the table owner, same RI-trigger pattern as
 * telemetry.c). All values come from structures the scan already parsed
 * (Iceberg manifests / Parquet footers) — ZERO additional origin
 * requests beyond the scan itself.
 *
 * The failure contract is identical to telemetry.c: collection at
 * executor teardown wrapped in PG_TRY (palloc/pstrdup/relcache can
 * ereport; core does not wrap FDW End hooks), flush at
 * XACT_EVENT_PRE_COMMIT, every failure swallowed — statistics never
 * break a user query. The buffer is capped with drop-oldest (a
 * many-scan transaction must not grow session memory unboundedly or
 * stall PRE_COMMIT with thousands of inserts).
 *
 * v1 scope and caveats: table-level rows plus per-column composed
 * stats (datatype class, nulls, value-count upper bound, bounds
 * presence, metric coverage, filterability tier). Column-row SETS are
 * written per executor and are not atomic as a set — a mid-batch
 * flush failure leaves a snapshot's fields partially recorded.
 * Retention/disable knobs are the follow-up. No retention or disable switch yet (telemetry has both) —
 * operator-facing knobs are the follow-up. Statements that error
 * before EndForeignScan lose their record (same caveat class as
 * telemetry). One row per EXECUTOR (the spec's "first scan per
 * snapshot" phrasing is reconciled to per-executor semantics here).
 * The flush inserts rows individually (parameterized — endpoint and
 * namespace.table are user-controlled FDW options and are bound, never
 * interpolated): rows are independently valid, so a mid-batch failure
 * commits rows 1..k-1 rather than dropping the batch; the cap bounds
 * the worst-case commit cost. Atomic batching is the follow-up.
 *---------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "pg_datalake_core.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

void		install_lakehouse_scan_stats(void);
void		lakehouse_scan_stats_record(Oid relid, const char *endpoint,
										const char *ns_table,
										int64 snapshot_id, int64 row_count,
										int files_total, int files_pruned,
										int scan_tasks, int64 total_bytes,
										bool has_deletes);
void		lakehouse_column_stats_record(Oid relid, const char *ns_table,
										  int64 snapshot_id, int64 rows,
										  ScanSession * session, Relation rel);


/*
 * owner of lakehouse.table_stats, resolved at record time (inside a
 * transaction) and cached for the commit-callback flush. v1: not
 * invalidated on DROP/re-CREATE within a session.
 */
static Oid	stats_owner = InvalidOid;

typedef struct ScanStatRecord
{
	Oid			dbid;
	Oid			relid;
	char	   *endpoint;		/* catalog endpoint, TopMemoryContext */
	char	   *ns_table;		/* namespace.table, TopMemoryContext */
	int64		snapshot_id;
	int64		row_count;		/* parquet-metadata rows */
	int			files_total;
	int			files_pruned;
	int			scan_tasks;
	int64		total_bytes;
	bool		has_deletes;
	TimestampTz captured_at;
} ScanStatRecord;

/*
 * bounded like telemetry: a many-scan transaction must not grow session
 * memory unboundedly or stall PRE_COMMIT with thousands of inserts
 */
#define SCAN_STATS_BUFFER_MAX 1000

static List *scan_stats_buffer = NIL;
static bool in_scan_stats_flush = false;

/*
 * composed per-column rows (spec 19 §4b): value_count is the explicit
 * UPPER bound (value_counts summed across files — values repeat across
 * files); the tier derives per the fixed §4b rules. Partition columns
 * (T0) are not derivable yet — no spec FFI — recorded as their
 * datatype tier with the partition refinement as follow-up.
 */
typedef struct ColumnStatRecord
{
	Oid			dbid;
	Oid			relid;
	char	   *ns_table;
	int64		snapshot_id;
	char	   *field_name;
	int64		field_id;
	char		datatype_class[24]; /* numeric|string|boolean|temporal|other|metrics-incomplete */
	int64		null_count;
	int64		value_count_upper;
	bool		has_bounds;
	bool		metrics_covered;	/* every planned file reported metrics */
	char		tier[3];		/* T1..T4 (T0 deferred) */
	TimestampTz captured_at;
} ColumnStatRecord;

static List *column_stats_buffer = NIL;

static void scan_stats_flush(void);
static void scan_stats_xact_callback(XactEvent event, void *arg);

static void
scan_stats_drop_buffer(void)
{
	ListCell   *lc;

	foreach(lc, scan_stats_buffer)
	{
		ScanStatRecord *rec = lfirst(lc);

		pfree(rec->endpoint);
		pfree(rec->ns_table);
		pfree(rec);
	}
	list_free(scan_stats_buffer);
	scan_stats_buffer = NIL;

	foreach(lc, column_stats_buffer)
	{
		ColumnStatRecord *crec = lfirst(lc);

		pfree(crec->ns_table);
		pfree(crec->field_name);
		pfree(crec);
	}
	list_free(column_stats_buffer);
	column_stats_buffer = NIL;
}

/*
 * called from lakehouse_EndForeignScan BEFORE the scan session is
 * freed; every failure swallowed
 */
void
lakehouse_scan_stats_record(Oid relid, const char *endpoint,
							const char *ns_table, int64 snapshot_id,
							int64 row_count, int files_total,
							int files_pruned, int scan_tasks,
							int64 total_bytes, bool has_deletes)
{
	MemoryContext ccxt = CurrentMemoryContext;

	/*
	 * statistics never break a user query: palloc/pstrdup/relcache can
	 * ereport (OOM), and core does not wrap FDW End hooks
	 */
	PG_TRY();
	{
		ScanStatRecord *rec;
		MemoryContext oldcxt;

		/*
		 * resolve + cache the stats-table owner while in a real transaction
		 * (relcache asserts transaction state)
		 */
		if (!OidIsValid(stats_owner))
		{
			Oid			table_relid = get_relname_relid("table_stats",
														get_namespace_oid("lakehouse",
																		  true));

			if (OidIsValid(table_relid))
			{
				Relation	rel = RelationIdGetRelation(table_relid);

				if (RelationIsValid(rel))
				{
					stats_owner = rel->rd_rel->relowner;
					RelationClose(rel);
				}
			}
		}
		if (!OidIsValid(stats_owner) || in_scan_stats_flush)
			return;

		/*
		 * allocate + link under TopMemoryContext: the ambient context here is
		 * the executor's query context, deleted before the PRE_COMMIT flush
		 * reads the buffer
		 */
		oldcxt = MemoryContextSwitchTo(TopMemoryContext);
		rec = (ScanStatRecord *) palloc(sizeof(ScanStatRecord));
		rec->dbid = MyDatabaseId;
		rec->relid = relid;
		rec->endpoint = pstrdup(endpoint ? endpoint : "");
		rec->ns_table = pstrdup(ns_table ? ns_table : "");
		rec->snapshot_id = snapshot_id;
		rec->row_count = row_count;
		rec->files_total = files_total;
		rec->files_pruned = files_pruned;
		rec->scan_tasks = scan_tasks;
		rec->total_bytes = total_bytes;
		rec->has_deletes = has_deletes;
		rec->captured_at = GetCurrentTimestamp();
		if (list_length(scan_stats_buffer) >= SCAN_STATS_BUFFER_MAX)
		{
			ScanStatRecord *oldest = lfirst(list_head(scan_stats_buffer));

			pfree(oldest->endpoint);
			pfree(oldest->ns_table);
			pfree(oldest);
			scan_stats_buffer = list_delete_first(scan_stats_buffer);
		}
		scan_stats_buffer = lappend(scan_stats_buffer, rec);

		/*
		 * column rows multiply by #fields: same cap bounds the insert count
		 * the PRE_COMMIT flush will perform
		 */
		while (list_length(column_stats_buffer) >= SCAN_STATS_BUFFER_MAX)
		{
			ColumnStatRecord *coldest =
				lfirst(list_head(column_stats_buffer));

			pfree(coldest->ns_table);
			pfree(coldest->field_name);
			pfree(coldest);
			column_stats_buffer = list_delete_first(column_stats_buffer);
		}
		MemoryContextSwitchTo(ccxt);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(ccxt);
		FlushErrorState();
	}
	PG_END_TRY();
}

/*
 * datatype class and filterability tier per the fixed §4b rules
 * (T2: composed NDV <= 100 AND NDV/rows <= 0.05; T1: >100 with bounds;
 * T3: boolean/temporal; T4: no bounds / unclassifiable)
 */
static void
classify_column(Oid typid, bool has_bounds, int64 values, int64 rows,
				char *class_out, char *tier_out)
{
	const char *class = "other";
	const char *tier = "T4";

	switch (typid)
	{
		case BOOLOID:
			class = "boolean";
			tier = has_bounds ? "T3" : "T4";
			break;
		case DATEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			class = "temporal";
			tier = has_bounds ? "T3" : "T4";
			break;
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
			class = "numeric";
			break;
		case VARCHAROID:
		case TEXTOID:
		case BPCHAROID:
			class = "string";
			break;
		default:
			class = "other";
			tier = "T4";
			break;
	}
	if (strcmp(class, "numeric") == 0 || strcmp(class, "string") == 0)
	{
		/*
		 * spec 19 §4b, literal: T2 iff composed NDV bound <= 100 AND <= 5%
		 * of rows (an all-null column qualifies: 0 values IS the empty
		 * value-set prune); T1 iff > 100 with bounds; else T4.
		 */
		if (values <= 100 && rows > 0 && values * 100 <= 5 * rows)
			tier = "T2";		/* enum-like: dictionary/value-set prune */
		else if (values > 100 && has_bounds)
			tier = "T1";		/* row-group prune */
		else
			tier = "T4";
	}
	strcpy(class_out, class);
	strcpy(tier_out, tier);
}

/* called from lakehouse_EndForeignScan; every failure swallowed */
void
lakehouse_column_stats_record(Oid relid, const char *ns_table,
							  int64 snapshot_id, int64 rows,
							  ScanSession * session, Relation rel)
{
	MemoryContext ccxt = CurrentMemoryContext;

	PG_TRY();
	{
		MemoryContext oldcxt;
		TupleDesc	tupdesc;
		uint64		field_count;

		if (session == NULL || rel == NULL || rows < 0 ||
			!OidIsValid(stats_owner) || in_scan_stats_flush)
			return;

		tupdesc = RelationGetDescr(rel);
		field_count = pg_datalake_scan_field_count(session);
		elog(DEBUG1, "col-stats trace: fields=%llu rows=%lld owner=%u",
			 (unsigned long long) field_count, (long long) rows,
			 stats_owner);

		oldcxt = MemoryContextSwitchTo(TopMemoryContext);
		for (uint64 idx = 0; idx < field_count; idx++)
		{
			const char *name = pg_datalake_scan_field_name(session, idx);
			int64		nulls = 0;
			int64		values = 0;
			bool		has_bounds = false;
			bool		metrics_covered = false;
			ColumnStatRecord *crec;
			Form_pg_attribute attr = NULL;

			if (name == NULL || !pg_datalake_scan_column_stats(session, idx,
															   &nulls,
															   &values,
															   &has_bounds,
															   &metrics_covered))
			{
				elog(DEBUG1, "col-stats trace: FFI false at idx %llu",
					 (unsigned long long) idx);
				break;
			}
			elog(DEBUG1, "col-stats trace: idx=%llu name=%s nulls=%lld vals=%lld cov=%d bounds=%d",
				 (unsigned long long) idx, name, (long long) nulls,
				 (long long) values, (int) metrics_covered, (int) has_bounds);

			/*
			 * resolve the attribute BY NAME: the Iceberg schema order need
			 * not match the foreign table's column order/subset (the reader
			 * resolves parquet columns by name too), and a positional pair
			 * mislabels datatype_class/tier while name/id/stats stay right.
			 * Unmatched fields (dropped/ renamed attrs) record class 'other',
			 * tier T4.
			 */
			for (int i = 0; i < tupdesc->natts; i++)
			{
				Form_pg_attribute a = TupleDescAttr(tupdesc, i);

				if (!a->attisdropped &&
					namestrcmp(&(a->attname), name) == 0)
				{
					attr = a;
					break;
				}
			}

			crec = (ColumnStatRecord *) palloc(sizeof(ColumnStatRecord));
			crec->dbid = MyDatabaseId;
			crec->relid = relid;
			crec->ns_table = pstrdup(ns_table);
			crec->snapshot_id = snapshot_id;
			crec->field_name = pstrdup(name);
			crec->field_id = pg_datalake_scan_field_id(session, idx);

			/*
			 * M4/coverage: value_counts are OPTIONAL per file — a sum over
			 * partial coverage is neither an upper bound nor a null floor, so
			 * a not-fully-covered field is recorded class 'other', tier T4
			 * (metrics incomplete) regardless of datatype.
			 */
			if (attr != NULL && metrics_covered)
				classify_column(attr->atttypid, has_bounds, values, rows,
								crec->datatype_class, crec->tier);
			else
			{
				strcpy(crec->datatype_class,
					   attr == NULL ? "other" : attr->attisdropped ? "other" :
					   "metrics-incomplete");
				strcpy(crec->tier, "T4");
			}

			crec->null_count = nulls;
			crec->value_count_upper = values;
			crec->has_bounds = has_bounds;
			crec->metrics_covered = metrics_covered;
			crec->captured_at = GetCurrentTimestamp();
			column_stats_buffer = lappend(column_stats_buffer, crec);
		}
		MemoryContextSwitchTo(ccxt);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(ccxt);
		FlushErrorState();
	}
	PG_END_TRY();
}

/* flush buffered rows as the table owner; failures cost the buffer */
static void
scan_stats_flush(void)
{
	ListCell   *lc;
	Oid			save_uid;
	int			save_sec_context;
	bool		connected = false;
	bool		snap_pushed = false;
	MemoryContext oldcxt;

	if ((scan_stats_buffer == NIL && column_stats_buffer == NIL) ||
		in_scan_stats_flush || !OidIsValid(stats_owner))
		return;

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	GetUserIdAndSecContext(&save_uid, &save_sec_context);
	SetUserIdAndSecContext(stats_owner, save_sec_context |
						   SECURITY_LOCAL_USERID_CHANGE);

	PG_TRY();
	{
		const char *stmt = "INSERT INTO lakehouse.table_stats "
			"(dbid, relid, endpoint, ns_table, snapshot_id, "
			"row_count, files_total, files_pruned, scan_tasks, "
			"total_bytes, has_deletes, captured_at) "
			"VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)";
		Oid			argtypes[12] = {OIDOID, OIDOID, TEXTOID, TEXTOID,
			INT8OID, INT8OID, INT4OID, INT4OID, INT4OID, INT8OID,
		BOOLOID, TIMESTAMPTZOID};
		Datum		vals[12];
		char		nulls[13] = {' ', ' ', ' ', ' ', ' ', ' ', ' ',
		' ', ' ', ' ', ' ', ' '};

		in_scan_stats_flush = true;

		if (SPI_connect() != SPI_OK_CONNECT)
			elog(ERROR, "scan stats flush: SPI_connect failed");
		connected = true;
		/* commit callbacks run without an active snapshot */
		PushActiveSnapshot(GetTransactionSnapshot());
		snap_pushed = true;

		/*
		 * parameterized per-row inserts: endpoint and namespace.table are
		 * user-controlled FDW options and are bound, never interpolated. Rows
		 * are independently valid, so a mid-batch failure commits rows 1..k-1
		 * rather than dropping the batch; the buffer cap bounds the
		 * worst-case commit cost. Atomic batching is the follow-up.
		 */
		foreach(lc, scan_stats_buffer)
		{
			ScanStatRecord *rec = lfirst(lc);

			vals[0] = ObjectIdGetDatum(rec->dbid);
			vals[1] = ObjectIdGetDatum(rec->relid);
			vals[2] = CStringGetTextDatum(rec->endpoint);
			vals[3] = CStringGetTextDatum(rec->ns_table);
			vals[4] = Int64GetDatum(rec->snapshot_id);
			vals[5] = Int64GetDatum(rec->row_count);
			vals[6] = Int32GetDatum(rec->files_total);
			vals[7] = Int32GetDatum(rec->files_pruned);
			vals[8] = Int32GetDatum(rec->scan_tasks);
			vals[9] = Int64GetDatum(rec->total_bytes);
			vals[10] = BoolGetDatum(rec->has_deletes);
			vals[11] = TimestampTzGetDatum(rec->captured_at);

			if (SPI_execute_with_args(stmt, 12, argtypes, vals,
									  nulls, false, 0) != SPI_OK_INSERT)
				elog(ERROR, "scan stats flush: insert failed");
		}

		/*
		 * composed per-column rows: parameterized, 13 params in exact column
		 * order (dbid, relid, ns_table, snapshot_id, field_name, field_id,
		 * datatype_class, null_count, value_count_upper, has_bounds,
		 * metrics_covered, tier, captured_at)
		 */
		{
			const char *cstmt = "INSERT INTO lakehouse.column_stats "
				"(dbid, relid, ns_table, snapshot_id, field_name, "
				"field_id, datatype_class, null_count, value_count_upper, "
				"has_bounds, metrics_covered, tier, captured_at) "
				"VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)";
			Oid			cargtypes[13] = {OIDOID, OIDOID, TEXTOID, INT8OID,
				TEXTOID, INT8OID, TEXTOID, INT8OID, INT8OID, BOOLOID,
			BOOLOID, TEXTOID, TIMESTAMPTZOID};
			Datum		cvals[13];
			char		cnulls[14] = {' ', ' ', ' ', ' ', ' ', ' ', ' ',
			' ', ' ', ' ', ' ', ' ', ' '};

			foreach(lc, column_stats_buffer)
			{
				ColumnStatRecord *crec = lfirst(lc);

				cvals[0] = ObjectIdGetDatum(crec->dbid);
				cvals[1] = ObjectIdGetDatum(crec->relid);
				cvals[2] = CStringGetTextDatum(crec->ns_table);
				cvals[3] = Int64GetDatum(crec->snapshot_id);
				cvals[4] = CStringGetTextDatum(crec->field_name);
				cvals[5] = Int64GetDatum(crec->field_id);
				cvals[6] = CStringGetTextDatum(crec->datatype_class);
				cvals[7] = Int64GetDatum(crec->null_count);
				cvals[8] = Int64GetDatum(crec->value_count_upper);
				cvals[9] = BoolGetDatum(crec->has_bounds);
				cvals[10] = BoolGetDatum(crec->metrics_covered);
				cvals[11] = CStringGetTextDatum(crec->tier);
				cvals[12] = TimestampTzGetDatum(crec->captured_at);

				if (SPI_execute_with_args(cstmt, 13, cargtypes, cvals,
										  cnulls, false, 0) != SPI_OK_INSERT)
					elog(ERROR, "scan stats flush: column insert failed");
			}
		}

		SPI_finish();
		connected = false;
		PopActiveSnapshot();
		snap_pushed = false;
		in_scan_stats_flush = false;
		SetUserIdAndSecContext(save_uid, save_sec_context);
		scan_stats_drop_buffer();
	}
	PG_CATCH();
	{
		/* best effort, but surface the cause once for diagnosis */
		MemoryContextSwitchTo(oldcxt);
		{
			ErrorData  *err = CopyErrorData();

			elog(WARNING, "scan stats flush failed: %s", err->message);
			FreeErrorData(err);
		}
		in_scan_stats_flush = false;
		SetUserIdAndSecContext(save_uid, save_sec_context);
		scan_stats_drop_buffer();
		if (snap_pushed)
			PopActiveSnapshot();
		if (connected)
			SPI_finish();
		FlushErrorState();
	}
	PG_END_TRY();
	MemoryContextSwitchTo(oldcxt);
}

static void
scan_stats_xact_callback(XactEvent event, void *arg)
{
	if (event == XACT_EVENT_PRE_COMMIT ||
		event == XACT_EVENT_PARALLEL_PRE_COMMIT)
		scan_stats_flush();
	else if (event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT ||
			 event == XACT_EVENT_PRE_PREPARE)
		scan_stats_drop_buffer();
	(void) arg;
}

void
install_lakehouse_scan_stats(void)
{
	static bool installed = false;

	if (installed)
		return;
	installed = true;
	RegisterXactCallback(scan_stats_xact_callback, NULL);
}
