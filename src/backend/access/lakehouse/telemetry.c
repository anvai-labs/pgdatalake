/*---------------------------------------------------------------------------
 * Query telemetry capture (spec 19 §4, layer L0).
 *
 * One buffered record per completed top-level foreign-scan statement,
 * flushed per transaction into lakehouse.query_stats — an
 * EXTENSION-INTERNAL table (REVOKE ALL FROM PUBLIC; inserts run as the
 * table owner via the RI-trigger user-context switch, so capture works
 * for every invoker without granting anything to anyone). Retention is
 * bounded by lakehouse.telemetry_retention (0 disables pruning, i.e.
 * unbounded retention).
 *
 * The admin-only exports (flush_query_stats,
 * share_query_stats_with_operator, grant/revoke_operator_telemetry)
 * are SQL wrappers in the extension script; consent is never implicit
 * and an operator share without a granted consent row fails closed
 * (55000).
 *
 * Capture must never break a user query, in any error path:
 *
 * - The outermost in-flight execution is tracked by QueryDesc pointer
 *   (NOT a depth counter — a counter leaks after any statement error
 *   between ExecutorStart and ExecutorEnd and silently kills capture
 *   for the rest of the session). ExecutorRun and ExecutorEnd errors
 *   clear the tracking before rethrowing.
 * - Records are allocated in TopMemoryContext (the ambient context at
 *   ExecutorEnd time is the statement's portal context, deleted before
 *   the PRE_COMMIT flush reads the buffer).
 * - The flush runs at XACT_EVENT_PRE_COMMIT; the multi-row INSERT is
 *   a single statement, so it is atomic on its own (no partial rows
 *   can commit). An internal subtransaction is NOT usable here —
 *   BeginInternalSubTransaction is illegal in the callback's
 *   TRANS_COMMIT state and corrupts the allocator. Failures cost the
 *   buffer, never the user's transaction.
 * - The hook chain calls the previous hooks unconditionally.
 *
 * Known v1 caveats (documented, not fixed): statements executed while
 * a portal keeps an executor open (DECLARE CURSOR materialization
 * window, WITH HOLD) are not captured; records from statements rolled
 * back by a savepoint still flush (and an ABORT drops the whole buffer
 * including statements that succeeded) — an asymmetry acceptable for
 * v1; EnableQueryId forces queryId only when compute_query_id is not
 * off.
 *---------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "access/parallel.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "nodes/queryjumble.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

extern bool lakehouse_telemetry_enabled;
extern int	lakehouse_telemetry_retention;

void		install_lakehouse_telemetry(void);
void		lakehouse_telemetry_guc_init(void);

bool		lakehouse_telemetry_enabled = true;
int			lakehouse_telemetry_retention = 100000;

static ExecutorStart_hook_type prev_executor_start_hook = NULL;
static ExecutorRun_hook_type prev_executor_run_hook = NULL;
static ExecutorEnd_hook_type prev_executor_end_hook = NULL;
static bool hooks_installed = false;

/* the outermost in-flight execution: NULL means "not capturing" */
static QueryDesc *active_desc = NULL;

/* true while our own SPI flush runs: captured inserts skip capture */
static bool in_telemetry_flush = false;

/*
 * buffered-record cap: a runaway multi-statement transaction must not
 * grow session memory unboundedly; oldest records drop first
 */
#define TELEMETRY_BUFFER_MAX 1000

/*
 * rows inserted since the last retention prune: prune only when the
 * table can plausibly exceed the cap (a top-N sort on every commit is
 * not acceptable on the commit path)
 */
static int64 inserts_since_prune = 0;

typedef struct TelemetryRecord
{
	Oid			userid;
	Oid			dbid;
	int64		queryid;
	TimestampTz started_at;
	TimestampTz ended_at;
	int64		rows;			/* DML rows; -1 for SELECT */
	int			foreign_scans;
} TelemetryRecord;

/*
 * buffered records; lives in TopMemoryContext so it survives the
 * transaction boundary the commit-callback flush runs behind
 */
static List *telemetry_buffer = NIL;

/*
 * owner of lakehouse.query_stats, resolved during capture (inside a
 * transaction — relcache access asserts transaction state) and cached
 * for the flush, which runs in a commit callback where opening
 * relations is not allowed. v1: not invalidated on DROP/re-CREATE
 * within a session (restart re-resolves).
 */
static Oid	stats_owner = InvalidOid;

static bool rtable_has_foreign(PlannedStmt *pstmt);
static bool count_foreign_scan_walker(PlanState *node, void *context);
static void telemetry_drop_buffer(void);
static void telemetry_flush(void);
static void telemetry_xact_callback(XactEvent event, void *arg);

/*
 * does the planned statement scan any foreign table? lakehouse stats
 * are for lakehouse workloads; heap-only statements are not captured
 */
static bool
rtable_has_foreign(PlannedStmt *pstmt)
{
	ListCell   *lc;

	foreach(lc, pstmt->rtable)
	{
		RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);

		if (rte->rtekind == RTE_RELATION &&
			rte->relkind == RELKIND_FOREIGN_TABLE)
			return true;
	}
	return false;
}

/* count foreign-table scan nodes in the planstate tree */
static bool
count_foreign_scan_walker(PlanState *node, void *context)
{
	int		   *n = (int *) context;

	if (node == NULL)
		return false;
	if (IsA(node, ForeignScanState))
		(*n)++;
	return planstate_tree_walker(node, count_foreign_scan_walker, context);
}

static void
telemetry_drop_buffer(void)
{
	list_free_deep(telemetry_buffer);
	telemetry_buffer = NIL;
}

static void
telemetry_capture_end(QueryDesc *queryDesc, TimestampTz started_at,
					  int n_foreign)
{
	TelemetryRecord *rec;
	MemoryContext oldcxt;

	/*
	 * parallel workers capture nothing: their xact callbacks never flush
	 * (PARALLEL_* PRE events are unhandled), so a record would be wasted work
	 * at best and a duplicate at worst
	 */
	if (IsParallelWorker())
		return;

	if (queryDesc->plannedstmt == NULL ||
		queryDesc->plannedstmt->rtable == NIL)
		return;

	/*
	 * lakehouse stats are for lakehouse workloads: only statements whose plan
	 * scans a foreign table are captured
	 */
	if (!rtable_has_foreign(queryDesc->plannedstmt))
		return;

	/*
	 * nothing to flush into (fresh installs before the extension is updated) —
	 * do not buffer what can never be flushed
	 */

	/*
	 * resolve + cache the stats-table owner while in a real transaction
	 * (relcache asserts transaction state)
	 */
	if (!OidIsValid(stats_owner))
	{
		Oid			relid = get_relname_relid("query_stats",
											  get_namespace_oid("lakehouse",
																true));

		if (OidIsValid(relid))
		{
			Relation	rel = RelationIdGetRelation(relid);

			if (RelationIsValid(rel))
			{
				stats_owner = rel->rd_rel->relowner;
				RelationClose(rel);
			}
		}
	}
	if (!OidIsValid(stats_owner))
		return;

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	rec = (TelemetryRecord *) palloc(sizeof(TelemetryRecord));
	rec->userid = GetUserId();
	rec->dbid = MyDatabaseId;
	rec->queryid = queryDesc->plannedstmt->queryId;
	rec->started_at = started_at;
	rec->ended_at = GetCurrentTimestamp();
	rec->rows = (queryDesc->operation == CMD_SELECT) ? -1 :
		(int64) queryDesc->estate->es_processed;
	rec->foreign_scans = n_foreign;

	if (list_length(telemetry_buffer) >= TELEMETRY_BUFFER_MAX)
	{
		pfree(lfirst(list_head(telemetry_buffer)));
		telemetry_buffer = list_delete_first(telemetry_buffer);
	}
	telemetry_buffer = lappend(telemetry_buffer, rec);
	MemoryContextSwitchTo(oldcxt);
}

static void
lakehouse_telemetry_exec_start(QueryDesc *queryDesc, int eflags)
{
	bool		outer = (active_desc == NULL);

	if (outer)
		active_desc = queryDesc;

	PG_TRY();
	{
		if (prev_executor_start_hook != NULL)
			(*prev_executor_start_hook) (queryDesc, eflags);
		else
			standard_ExecutorStart(queryDesc, eflags);
	}
	PG_CATCH();
	{
		if (outer)
			active_desc = NULL;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

static void
lakehouse_telemetry_exec_run(QueryDesc *queryDesc, ScanDirection direction,
							 uint64 count)
{
	PG_TRY();
	{
		if (prev_executor_run_hook != NULL)
			(*prev_executor_run_hook) (queryDesc, direction, count);
		else
			standard_ExecutorRun(queryDesc, direction, count);
	}
	PG_CATCH();
	{
		/*
		 * the outermost execution died mid-run (statement timeout,
		 * constraint, OOM, ...): without this clearing, every later statement
		 * would be treated as nested and never captured
		 */
		if (queryDesc == active_desc)
			active_desc = NULL;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

static void
lakehouse_telemetry_exec_end(QueryDesc *queryDesc)
{
	TimestampTz started_at = GetCurrentStatementStartTimestamp();
	int			n_foreign = 0;
	bool		outer = (queryDesc == active_desc);

	/*
	 * capture BEFORE the standard end: ExecutorEnd shuts the estate down
	 * (es_processed and the planstate tree are freed by it)
	 */
	if (outer && lakehouse_telemetry_enabled &&
		!in_telemetry_flush &&
		queryDesc->planstate != NULL)
	{
		MemoryContext ccxt = CurrentMemoryContext;

		PG_TRY();
		{
			count_foreign_scan_walker(queryDesc->planstate, &n_foreign);
			telemetry_capture_end(queryDesc, started_at, n_foreign);
		}
		PG_CATCH();
		{
			/*
			 * telemetry must never break the query; capture_end may have
			 * switched to TopMemoryContext before erroring — restore the
			 * caller's context
			 */
			MemoryContextSwitchTo(ccxt);
			FlushErrorState();
			n_foreign = 0;
		}
		PG_END_TRY();
	}

	PG_TRY();
	{
		if (prev_executor_end_hook != NULL)
			(*prev_executor_end_hook) (queryDesc);
		else
			standard_ExecutorEnd(queryDesc);
	}
	PG_CATCH();
	{
		/*
		 * an error inside ExecutorEnd's body must not leave the tracking
		 * pointer set (same poisoning as a Run error)
		 */
		if (outer)
			active_desc = NULL;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (outer)
		active_desc = NULL;
}

/*
 * flush the buffered records into lakehouse.query_stats as the table
 * owner (SECURITY_LOCAL_USERID_CHANGE — the RI-trigger pattern).
 * Runs at XACT_EVENT_PRE_COMMIT; the multi-row INSERT is one
 * statement (atomic — no phantom partial rows), and failures cost the
 * buffer, never the user's transaction.
 */
static void
telemetry_flush(void)
{
	ListCell   *lc;
	Oid			save_uid;
	int			save_sec_context;
	StringInfoData buf;
	bool		connected = false;
	bool		snap_pushed = false;
	MemoryContext oldcxt;

	if (telemetry_buffer == NIL || !lakehouse_telemetry_enabled ||
		in_telemetry_flush || !OidIsValid(stats_owner))
	{
		if (!lakehouse_telemetry_enabled)
			telemetry_drop_buffer();
		return;
	}

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	GetUserIdAndSecContext(&save_uid, &save_sec_context);
	SetUserIdAndSecContext(stats_owner, save_sec_context |
						   SECURITY_LOCAL_USERID_CHANGE);

	PG_TRY();
	{
		bool		first = true;

		in_telemetry_flush = true;

		if (SPI_connect() != SPI_OK_CONNECT)
			elog(ERROR, "telemetry flush: SPI_connect failed");
		connected = true;
		/* commit callbacks run without an active snapshot */
		PushActiveSnapshot(GetTransactionSnapshot());
		snap_pushed = true;

		initStringInfo(&buf);
		appendStringInfoString(&buf,
							   "INSERT INTO lakehouse.query_stats "
							   "(dbid, userid, queryid, rows, total_ms, "
							   "foreign_scans, captured_at) VALUES ");
		foreach(lc, telemetry_buffer)
		{
			TelemetryRecord *rec = lfirst(lc);
			double		total_ms;
			char		rowsbuf[32];

			total_ms = (double) (rec->ended_at - rec->started_at) / 1000.0;
			if (rec->rows < 0)
				snprintf(rowsbuf, sizeof(rowsbuf), "NULL");
			else
				snprintf(rowsbuf, sizeof(rowsbuf), INT64_FORMAT,
						 rec->rows);
			if (!first)
				appendStringInfoChar(&buf, ',');
			first = false;
			appendStringInfo(&buf, "(%u,%u," INT64_FORMAT ",%s,%.3f,%d,'%s')",
							 rec->dbid, rec->userid, (int64) rec->queryid,
							 rowsbuf, total_ms, rec->foreign_scans,
							 timestamptz_to_str(rec->started_at));
		}

		if (SPI_execute(buf.data, false, 0) != SPI_OK_INSERT)
			elog(ERROR, "telemetry flush: insert failed");
		inserts_since_prune += list_length(telemetry_buffer);

		/*
		 * bounded retention: prune only when the table can plausibly exceed
		 * the cap (a top-N sort on every commit is not acceptable on the
		 * commit path); retention = 0 disables pruning entirely (unbounded)
		 */
		if (lakehouse_telemetry_retention > 0 &&
			inserts_since_prune >= lakehouse_telemetry_retention)
		{
			char		prune[512];

			snprintf(prune, sizeof(prune),
					 "DELETE FROM lakehouse.query_stats WHERE ctid IN "
					 "(SELECT ctid FROM lakehouse.query_stats "
					 "ORDER BY captured_at DESC OFFSET %d)",
					 lakehouse_telemetry_retention);
			if (SPI_execute(prune, false, 0) != SPI_OK_DELETE)
				elog(ERROR, "telemetry flush: prune failed");
			inserts_since_prune = 0;
		}

		SPI_finish();
		connected = false;
		PopActiveSnapshot();
		snap_pushed = false;
		in_telemetry_flush = false;
		SetUserIdAndSecContext(save_uid, save_sec_context);
		telemetry_drop_buffer();
	}
	PG_CATCH();
	{
		/* best effort, but surface the cause once for diagnosis */
		MemoryContextSwitchTo(oldcxt);
		{
			ErrorData  *err = CopyErrorData();

			elog(WARNING, "telemetry flush failed: %s", err->message);
			FreeErrorData(err);
		}
		in_telemetry_flush = false;
		SetUserIdAndSecContext(save_uid, save_sec_context);
		telemetry_drop_buffer();
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
telemetry_xact_callback(XactEvent event, void *arg)
{
	if (event == XACT_EVENT_PRE_COMMIT ||
		event == XACT_EVENT_PARALLEL_PRE_COMMIT)
		telemetry_flush();
	else if (event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT ||
			 event == XACT_EVENT_PRE_PREPARE)
	{
		/*
		 * ABORT: drop the buffer. PRE_PREPARE: drop too — flushing
		 * two-phase-prepared work under a LATER transaction would record rows
		 * for work that may never commit
		 */
		telemetry_drop_buffer();
	}
	(void) arg;
}

void
install_lakehouse_telemetry(void)
{
	if (hooks_installed)
		return;
	hooks_installed = true;

	/* no-op when compute_query_id = off (queryid stays 0) */
	EnableQueryId();

	prev_executor_start_hook = ExecutorStart_hook;
	ExecutorStart_hook = lakehouse_telemetry_exec_start;
	prev_executor_run_hook = ExecutorRun_hook;
	ExecutorRun_hook = lakehouse_telemetry_exec_run;
	prev_executor_end_hook = ExecutorEnd_hook;
	ExecutorEnd_hook = lakehouse_telemetry_exec_end;

	RegisterXactCallback(telemetry_xact_callback, NULL);
}

void
lakehouse_telemetry_guc_init(void)
{
	DefineCustomBoolVariable("lakehouse.telemetry",
							 "Capture per-statement lakehouse query telemetry into lakehouse.query_stats.",
							 NULL,
							 &lakehouse_telemetry_enabled,
							 true,
							 PGC_SUSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("lakehouse.telemetry_retention",
							"Rows retained in the lakehouse.query_stats internal table (0 disables pruning).",
							NULL,
							&lakehouse_telemetry_retention,
							100000, 0, INT_MAX,
							PGC_SUSET, 0, NULL, NULL, NULL);
}
