/*
 * publisher.c — the lakehouse publisher background worker (P4c, spec 15
 * §4): coalesces unpublished delta segments into ONE append snapshot per
 * relation, plus the SQL entry points (start_publisher, publish_staged).
 * Rides the write-state machinery exported from tableam.c via
 * lakehouse_tableam.h.
 */
#include "postgres.h"

#include "access/xact.h"
#include "access/table.h"
#include "commands/defrem.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "executor/spi.h"
#include "utils/memutils.h"
#include "libpq/pqsignal.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"

#include "lakehouse_tableam.h"
#include "pg_datalake_core.h"



void
register_publisher_worker(void)
{
	BackgroundWorker bgw;
	MemoryContext old = MemoryContextSwitchTo(TopMemoryContext);

	memset(&bgw, 0, sizeof(bgw));
	snprintf(bgw.bgw_name, BGW_MAXLEN, "lakehouse publisher");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "lakehouse publisher");
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	bgw.bgw_restart_time = 30;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN, "lakehouse");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "lakehouse_publisher_main");
	bgw.bgw_main_arg = 0;
	RegisterBackgroundWorker(&bgw);
	MemoryContextSwitchTo(old);
}

/*
 * pg_lakehouse_start_publisher (P4c, dynamic path): register the
 * publisher worker from a NORMAL backend — no preload required (tests,
 * dev, and upgrades of an existing installation). Returns true when a
 * worker was registered; false when one is already running (checked via
 * pg_stat_activity; the static preload path makes this a no-op).
 */
PG_FUNCTION_INFO_V1(pg_lakehouse_start_publisher);

Datum
pg_lakehouse_start_publisher(PG_FUNCTION_ARGS)
{
	BackgroundWorker bgw;
	BackgroundWorkerHandle *handle;
	int			running = 0;
	int			ret;

	if (!lakehouse_publisher_enabled)
		PG_RETURN_BOOL(false);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	ret = SPI_execute("SELECT count(*) FROM pg_stat_activity"
					  " WHERE backend_type = 'lakehouse publisher'",
					  true, 0);
	if (ret == SPI_OK_SELECT && SPI_processed > 0)
	{
		bool		isnull;

		running = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
											  SPI_tuptable->tupdesc, 1,
											  &isnull));
	}
	PopActiveSnapshot();
	SPI_finish();
	if (running > 0)
		PG_RETURN_BOOL(false);

	memset(&bgw, 0, sizeof(bgw));
	snprintf(bgw.bgw_name, BGW_MAXLEN, "lakehouse publisher");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "lakehouse publisher");
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	bgw.bgw_restart_time = 30;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN, "lakehouse");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "lakehouse_publisher_main");
	bgw.bgw_main_arg = 0;

	if (!RegisterDynamicBackgroundWorker(&bgw, &handle))
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("lakehouse: could not register the publisher worker")));
	PG_RETURN_BOOL(true);
}

/*
 * publisher_main (P4c, spec 15 §4/§G5): the hands-off publisher loop.
 * One cycle per publish_max_lag: find every table with unpublished
 * delta segments and run lakehouse.publish_staged() for it (the
 * emission core + ledger flip of slices P4.1/2). Each table publishes
 * in its OWN transaction so one bad table never wedges the loop; a
 * failing table (e.g. unreachable catalog) logs a WARNING and is
 * retried next cycle — never fatal to the worker.
 */
static volatile sig_atomic_t got_sigterm = false;

static void
publisher_sigterm(SIGNAL_ARGS)
{
	got_sigterm = true;
	SetLatch(MyLatch);
}

PGDLLEXPORT void
			lakehouse_publisher_main(Datum main_arg);	/* bgw entry: must be
														 * exported */

void
lakehouse_publisher_main(Datum main_arg)
{
	const char *dbname = lakehouse_publish_database
		? lakehouse_publish_database : "postgres";

	pqsignal(SIGTERM, publisher_sigterm);
	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnection(dbname, NULL, 0);

	while (!got_sigterm)
	{
		int			rc;
		int			ret;
		int			nrels = 0;
		Oid		   *relids = NULL;

		/* one transaction per CYCLE: discover + publish + flip */
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());
		SPI_connect();
		PG_TRY();
		{
			ret = SPI_execute("SELECT DISTINCT relid FROM"
							  " lakehouse.lakehouse_delta_segments"
							  " WHERE NOT published", true, 0);
			if (ret == SPI_OK_SELECT && SPI_processed > 0)
			{
				/*
				 * TopMemoryContext: the array must SURVIVE the discovery
				 * transaction's commit — allocating in the txn context
				 * freed it before the per-table publish loop read it
				 * (use-after-free: garbage relids, worker SEGV — the same
				 * context-lifetime class as the M2 entry bug)
				 */
				MemoryContext sc = MemoryContextSwitchTo(TopMemoryContext);

				relids = (Oid *) palloc(sizeof(Oid) * SPI_processed);
				for (int i = 0; i < SPI_processed; i++)
				{
					bool		isnull;

					relids[nrels] = DatumGetObjectId(SPI_getbinval(
																   SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1,
																   &isnull));
					if (!isnull)
						nrels++;
				}
				MemoryContextSwitchTo(sc);
			}
		}
		PG_CATCH();
		{
			/* keep the worker alive no matter what */
			FlushErrorState();
			SPI_finish();
			PopActiveSnapshot();
			AbortCurrentTransaction();
			ereport(LOG,
					(errmsg("lakehouse publisher: discovery failed, retrying")));
			goto sleep_cycle;
		}
		PG_END_TRY();
		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();

		for (int i = 0; i < nrels; i++)
		{
			SetCurrentStatementStartTimestamp();
			StartTransactionCommand();
			PushActiveSnapshot(GetTransactionSnapshot());
			SPI_connect();
			PG_TRY();
			{
				StringInfoData sql;

				initStringInfo(&sql);
				appendStringInfo(&sql,
								 "SELECT lakehouse.publish_staged(%u)",
								 (unsigned) relids[i]);
				SPI_execute(sql.data, false, 0);
				SPI_finish();
				PopActiveSnapshot();

				/*
				 * COMMIT, not abort: the ledger flip of publish_staged must
				 * SURVIVE the cycle, or the segment stays unpublished and
				 * re-publishes every cycle (duplicate rows remote-side —
				 * caught live: 4 cycles x same 10 rows).
				 */
				CommitTransactionCommand();
			}
			PG_CATCH();
			{
				/*
				 * failure path: abort (ledger untouched) — the segment
				 * retries next cycle
				 */
				FlushErrorState();
				AbortCurrentTransaction();
				ereport(LOG,
						(errmsg("lakehouse publisher: publish of %u failed,"
								" retrying next cycle",
								(unsigned) relids[i])));
			}
			PG_END_TRY();
		}

sleep_cycle:
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   (long) lakehouse_publish_max_lag * 1000,
					   PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
		if (rc & WL_POSTMASTER_DEATH || got_sigterm)
			break;
	}
	proc_exit(0);
}


PG_FUNCTION_INFO_V1(pg_lakehouse_publish_staged);

Datum
pg_lakehouse_publish_staged(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Relation	rel;
	LakehouseWriteEntry *entry = NULL;
	int			published_count = 0;
	int			ret;
	MemoryContext cb_ctx = CurrentMemoryContext;
	char		errbuf[1024];

	rel = table_open(relid, AccessShareLock);
	entry = get_write_entry(rel);
	open_write_session(entry, RelationGetDescr(rel));
	if (entry->ws == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("lakehouse: publisher could not open a write session")));

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	{
		StringInfoData sql;
		int64	   *claimed;
		int			n_claimed = 0;

		/*
		 * SERIALIZE per table (spec 16, "Publishing must reconcile"): a
		 * manual publish and the worker cycle must not interleave
		 * selection/emission/flip for the same relation. The transaction
		 * advisory lock releases at COMMIT/ABORT, so failures never wedge the
		 * table.
		 */
		SPI_execute(psprintf("SELECT pg_advisory_xact_lock(%lld)",
							 (long long) (unsigned) relid),
					false, 0);

		initStringInfo(&sql);
		appendStringInfo(&sql,
						 "SELECT seg_id, payload FROM"
						 " lakehouse.lakehouse_delta_segments"
						 " WHERE relid = %u AND NOT published"
						 " ORDER BY seg_id", (unsigned) relid);
		ret = SPI_execute(sql.data, false, 0);
		if (ret != SPI_OK_SELECT)
			ereport(ERROR,
					(errcode(ERRCODE_IO_ERROR),
					 errmsg("lakehouse: publisher could not read delta segments")));

		/*
		 * DURABLE CLAIM (spec 16): capture the EXACT segment set now. The old
		 * code selected unbounded and flipped with a blanket "WHERE relid =
		 * %u AND NOT published" — a concurrent INSERT's segment landing
		 * between selection and flip was marked published WITHOUT its rows
		 * ever being emitted (silent loss). The claim is what is emitted AND
		 * what is flipped; anything else stays unpublished for the next
		 * cycle.
		 */
		claimed = (int64 *) palloc(sizeof(int64) *
								   (SPI_processed > 0 ? SPI_processed : 1));
		for (int i = 0; i < SPI_processed; i++)
		{
			bool		isnull;

			claimed[n_claimed++] = DatumGetInt64(SPI_getbinval(
															   SPI_tuptable->vals[i],
															   SPI_tuptable->tupdesc, 1,
															   &isnull));
		}

		for (int i = 0; i < SPI_processed; i++)
		{
			bool		isnull;
			Datum		payload = SPI_getbinval(SPI_tuptable->vals[i],
												SPI_tuptable->tupdesc, 2, &isnull);
			char	   *payload_c;

			if (isnull)
				continue;
			payload_c = TextDatumGetCString(payload);

			/*
			 * emission core (Rust): payload rows -> writer -> flush.
			 * COALESCED emission: every unpublished segment's rows go into
			 * ONE accumulator (rollover bounds file sizes), ONE commit.
			 */
			if (!pg_datalake_publish_segment(entry->ws, payload_c,
											 errbuf, sizeof(errbuf)))
				ereport(ERROR,
						(errcode(ERRCODE_DATA_EXCEPTION),
						 errmsg("lakehouse: publisher emission failed for segment %lld: %s",
								(long long) claimed[i], errbuf)));
			published_count++;
		}

		if (published_count > 0)
		{
			char	   *loc;
			int64		snap_id;
			int			flip_ret;
			StringInfoData claim;

			/* ONE append snapshot for the whole batch */
			loc = pg_datalake_write_commit(entry->ws, 3, errbuf, sizeof(errbuf));
			if (loc == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 errmsg("lakehouse: publisher commit failed: %s", errbuf)));
			snap_id = pg_datalake_write_last_snapshot_id(entry->ws);
			pg_datalake_write_commit_result_free(loc);

			/*
			 * ledger flip: ONLY the claimed seg_ids, by explicit id list —
			 * never a blanket relid flip. Verify exact cardinality: the flip
			 * must mark exactly the emitted set; anything else means someone
			 * touched the ledger under our lock, which is a bug to surface,
			 * not paper over.
			 */
			initStringInfo(&claim);
			for (int i = 0; i < n_claimed; i++)
				appendStringInfo(&claim, "%s%lld", i ? "," : "",
								 (long long) claimed[i]);
			flip_ret = SPI_execute(psprintf("UPDATE"
											" lakehouse.lakehouse_delta_segments"
											" SET published = true,"
											" published_snapshot_id = %lld"
											" WHERE relid = %u AND NOT published"
											" AND seg_id = ANY(ARRAY[%s])",
											(long long) snap_id,
											(unsigned) relid, claim.data),
								   false, 0);
			if (flip_ret != SPI_OK_UPDATE ||
				SPI_processed != published_count)
				ereport(ERROR,
						(errcode(ERRCODE_IO_ERROR),
						 errmsg("lakehouse: publisher ledger flip marked %d"
								" segments but emitted %d",
								flip_ret == SPI_OK_UPDATE ? (int) SPI_processed : -1,
								published_count),
						 errdetail("The flip must cover exactly the claimed"
								   " segment set — refusing to leave the"
								   " ledger inconsistent.")));
		}
	}
	PopActiveSnapshot();
	SPI_finish();
	table_close(rel, AccessShareLock);
	MemoryContextSwitchTo(cb_ctx);

	PG_RETURN_INT32(published_count);
}
