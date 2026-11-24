/*---------------------------------------------------------------------------
 * Query telemetry capture (spec 19 §4, layer L0).
 *
 * One buffered record per completed top-level foreign-scan statement,
 * flushed once per transaction into lakehouse.query_stats — an
 * EXTENSION-INTERNAL table (REVOKE ALL FROM PUBLIC; inserts run as the
 * table owner via the RI-trigger user-context switch, so capture works
 * for every invoker without granting anything to anyone). Retention is
 * bounded by lakehouse.telemetry_retention.
 *
 * The admin-only exports (flush_query_stats,
 * share_query_stats_with_operator, grant/revoke_operator_telemetry)
 * are SQL wrappers in the extension script; consent is never implicit
 * and an operator share without a granted consent row fails closed
 * (55000).
 *
 * Capture must never break a user query: the flush runs at
 * XACT_EVENT_COMMIT — after RecordTransactionCommit, so the user's
 * work is already durable and a telemetry failure costs only the
 * buffer — and every failure inside the capture path is swallowed.
 * The hook chain calls the previous hooks unconditionally.
 *---------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fmgr.h>

#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "nodes/queryjumble.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

extern bool lakehouse_telemetry_enabled;
extern int	lakehouse_telemetry_retention;

bool		lakehouse_telemetry_enabled = true;
int			lakehouse_telemetry_retention = 100000;

void		install_lakehouse_telemetry(void);
void		lakehouse_telemetry_guc_init(void);

static ExecutorStart_hook_type prev_executor_start_hook = NULL;
static ExecutorEnd_hook_type prev_executor_end_hook = NULL;
static bool hooks_installed = false;

/* nesting depth of executor entry: capture only the top level */
static int	exec_depth = 0;

/* true while our own SPI flush runs: captured inserts skip capture */
static bool in_telemetry_flush = false;

/*
 * owner of lakehouse.query_stats, resolved during capture (inside a
 * transaction — relcache access asserts transaction state) and cached
 * for the flush, which runs in a commit callback where opening
 * relations is not allowed
 */
static Oid	stats_owner = InvalidOid;

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
 * transaction boundary the COMMIT-event flush runs behind
 */
static List *telemetry_buffer = NIL;

static bool rtable_has_foreign(PlannedStmt *pstmt);
static bool count_foreign_scan_walker(PlanState *node, void *context);
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
telemetry_capture_end(QueryDesc *queryDesc, TimestampTz started_at,
					  int n_foreign)
{
	TelemetryRecord *rec;
	MemoryContext oldcxt;

	if (queryDesc->plannedstmt == NULL ||
		queryDesc->plannedstmt->rtable == NIL)
		return;
	if (!rtable_has_foreign(queryDesc->plannedstmt))
		return;

	/* resolve + cache the stats-table owner while in a real transaction */
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

	rec = (TelemetryRecord *) palloc(sizeof(TelemetryRecord));
	rec->userid = GetUserId();
	rec->dbid = MyDatabaseId;
	rec->queryid = queryDesc->plannedstmt->queryId;
	rec->started_at = started_at;
	rec->ended_at = GetCurrentTimestamp();
	rec->rows = (queryDesc->operation == CMD_SELECT) ? -1 :
		(int64) queryDesc->estate->es_processed;
	rec->foreign_scans = n_foreign;

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	telemetry_buffer = lappend(telemetry_buffer, rec);
	MemoryContextSwitchTo(oldcxt);
}

static void
lakehouse_telemetry_exec_start(QueryDesc *queryDesc, int eflags)
{
	exec_depth++;

	PG_TRY();
	{
		if (prev_executor_start_hook != NULL)
			(*prev_executor_start_hook) (queryDesc, eflags);
		else
			standard_ExecutorStart(queryDesc, eflags);
	}
	PG_CATCH();
	{
		exec_depth--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

static void
lakehouse_telemetry_exec_end(QueryDesc *queryDesc)
{
	TimestampTz started_at = GetCurrentStatementStartTimestamp();
	int			n_foreign = 0;

	/*
	 * capture BEFORE the standard end: ExecutorEnd shuts the estate down
	 * (es_processed and the planstate tree are freed by it)
	 */
	if (exec_depth == 1 && lakehouse_telemetry_enabled &&
		!in_telemetry_flush &&
		queryDesc->planstate != NULL)
	{
		PG_TRY();
		{
			count_foreign_scan_walker(queryDesc->planstate, &n_foreign);
			telemetry_capture_end(queryDesc, started_at, n_foreign);
		}
		PG_CATCH();
		{
			/* telemetry must never break the query */
			FlushErrorState();
			n_foreign = 0;
		}
		PG_END_TRY();
	}

	if (prev_executor_end_hook != NULL)
		(*prev_executor_end_hook) (queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
	exec_depth--;
}

/*
 * flush the buffered records into lakehouse.query_stats as the table
 * owner (SECURITY_LOCAL_USERID_CHANGE — the RI-trigger pattern).
 * Runs at PRE_COMMIT (transaction active, the same point the staged
 * capture flushes at): any failure is swallowed and costs the buffer,
 * never the user's transaction.
 */
static void
telemetry_flush(void)
{
	ListCell   *lc;
	Oid			owner;
	Oid			save_uid;
	int			save_sec_context;
	Relation	rel;
	StringInfoData buf;
	bool		connected = false;
	MemoryContext oldcxt;

	if (telemetry_buffer == NIL || !lakehouse_telemetry_enabled ||
		in_telemetry_flush || !OidIsValid(stats_owner))
	{
		if (!lakehouse_telemetry_enabled)
			telemetry_buffer = NIL;
		return;
	}

	owner = stats_owner;

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	GetUserIdAndSecContext(&save_uid, &save_sec_context);
	SetUserIdAndSecContext(owner, save_sec_context |
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
				snprintf(rowsbuf, sizeof(rowsbuf), "%ld", rec->rows);
			if (!first)
				appendStringInfoChar(&buf, ',');
			first = false;
			appendStringInfo(&buf, "(%u,%u,%ld,%s,%.3f,%d,'%s')",
							 rec->dbid, rec->userid, rec->queryid,
							 rowsbuf, total_ms, rec->foreign_scans,
							 timestamptz_to_str(rec->started_at));
		}

		if (SPI_execute(buf.data, false, 0) != SPI_OK_INSERT)
			elog(ERROR, "telemetry flush: insert failed");

		/* bounded retention: drop the oldest rows beyond the cap */
		if (lakehouse_telemetry_retention > 0)
		{
			char		prune[512];

			snprintf(prune, sizeof(prune),
					 "DELETE FROM lakehouse.query_stats WHERE ctid IN "
					 "(SELECT ctid FROM lakehouse.query_stats "
					 "ORDER BY captured_at DESC OFFSET %d)",
					 lakehouse_telemetry_retention);
			SPI_execute(prune, false, 0);
		}

		SPI_finish();
		connected = false;
		PopActiveSnapshot();
		in_telemetry_flush = false;
		SetUserIdAndSecContext(save_uid, save_sec_context);
		telemetry_buffer = NIL;
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
		telemetry_buffer = NIL;
		if (connected)
		{
			SPI_finish();
			PopActiveSnapshot();
		}
		FlushErrorState();
	}
	PG_END_TRY();
	MemoryContextSwitchTo(oldcxt);
}

static void
telemetry_xact_callback(XactEvent event, void *arg)
{
	if (event == XACT_EVENT_PRE_COMMIT)
		telemetry_flush();
	else if (event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT)
		telemetry_buffer = NIL;
	(void) arg;
}

void
install_lakehouse_telemetry(void)
{
	if (hooks_installed)
		return;
	hooks_installed = true;

	EnableQueryId();

	prev_executor_start_hook = ExecutorStart_hook;
	ExecutorStart_hook = lakehouse_telemetry_exec_start;
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
							 PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("lakehouse.telemetry_retention",
							"Rows retained in the lakehouse.query_stats internal table.",
							NULL,
							&lakehouse_telemetry_retention,
							100000, 0, INT_MAX,
							PGC_SUSET, 0, NULL, NULL, NULL);
}
