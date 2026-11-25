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
 * executor teardown, flush at XACT_EVENT_PRE_COMMIT, every failure
 * swallowed — statistics never break a user query.
 *
 * v1 scope: table-level stats only (rows, file counts before/after
 * pruning, scan tasks, total bytes, delete presence). Column-level
 * stats (value_counts composition, filterability tiers) need new core
 * FFI exports and are the follow-up slice (spec 19 §4b).
 *---------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

void		install_lakehouse_scan_stats(void);

/*
 * owner of lakehouse.table_stats, resolved at record time (inside a
 * transaction) and cached for the commit-callback flush
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

static List *scan_stats_buffer = NIL;
static bool in_scan_stats_flush = false;

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

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	rec = (ScanStatRecord *) palloc(sizeof(ScanStatRecord));
	rec->dbid = MyDatabaseId;
	rec->relid = relid;
	rec->endpoint = pstrdup(endpoint);
	rec->ns_table = pstrdup(ns_table);
	rec->snapshot_id = snapshot_id;
	rec->row_count = row_count;
	rec->files_total = files_total;
	rec->files_pruned = files_pruned;
	rec->scan_tasks = scan_tasks;
	rec->total_bytes = total_bytes;
	rec->has_deletes = has_deletes;
	rec->captured_at = GetCurrentTimestamp();
	scan_stats_buffer = lappend(scan_stats_buffer, rec);
	MemoryContextSwitchTo(oldcxt);
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

	if (scan_stats_buffer == NIL || in_scan_stats_flush ||
		!OidIsValid(stats_owner))
		return;

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	GetUserIdAndSecContext(&save_uid, &save_sec_context);
	SetUserIdAndSecContext(stats_owner, save_sec_context |
						   SECURITY_LOCAL_USERID_CHANGE);

	PG_TRY();
	{
		in_scan_stats_flush = true;

		if (SPI_connect() != SPI_OK_CONNECT)
			elog(ERROR, "scan stats flush: SPI_connect failed");
		connected = true;
		PushActiveSnapshot(GetTransactionSnapshot());
		snap_pushed = true;

		/*
		 * parameterized: endpoint/ns_table are user-controlled FDW options
		 * and must never be string-interpolated
		 */
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
