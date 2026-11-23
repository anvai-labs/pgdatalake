/*
 * managed_send.c
 *    Native authorization barrier for a prepared managed publication.
 *
 * This one-shot worker retains the exact request and stops. There is no
 * catalog dispatch, and its SQL receipt confers no permission to send.
 */

#include "postgres.h"

#include "access/detoast.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/pg_type_d.h"
#include "fmgr.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "portability/instr_time.h"
#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/fmgrprotos.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/uuid.h"
#include "utils/wait_event.h"

#define MANAGED_SEND_HOLD_NAME "lakehouse managed send hold"
#define SEND_DIGEST_BYTES 32
#define MAX_SEND_REQUEST 65536
#define SEND_HOLD_TIMEOUT_MS 10000

typedef enum ManagedSendOutcome
{
	MANAGED_SEND_PENDING,
	MANAGED_SEND_HELD,
	MANAGED_SEND_ERROR,
}			ManagedSendOutcome;

/* Exact prepared request and result. This structure carries no credentials. */
typedef struct ManagedSendHold
{
	Oid			database;
	Oid			role;
	pg_uuid_t	operation;
	int64		generation;
	int64		parent;
	int64		snapshot;
	bool		parent_isnull;
	Size		body_len;
	char		input_digest[SEND_DIGEST_BYTES];
	char		proposal_digest[SEND_DIGEST_BYTES];
	char		body[MAX_SEND_REQUEST];
	pg_atomic_uint32 outcome;
	int			sqlstate;
	TransactionId intent_xid;
}			ManagedSendHold;

typedef struct ManagedSendResult
{
	DestReceiver receiver;
	TransactionId xid;
}			ManagedSendResult;

PG_FUNCTION_INFO_V1(pg_lakehouse_hold_managed_publication_send);
PGDLLEXPORT void lakehouse_managed_send_hold_main(Datum main_arg);

static bool
receive_intent_xid(TupleTableSlot *slot, DestReceiver *self)
{
	ManagedSendResult *result = (ManagedSendResult *) self;
	bool		isnull;
	Datum		value = slot_getattr(slot, 1, &isnull);

	if (TransactionIdIsValid(result->xid) || isnull ||
		!TransactionIdIsNormal(DatumGetTransactionId(value)))
		elog(ERROR, "managed publication authorization returned an invalid intent transaction ID");
	result->xid = DatumGetTransactionId(value);
	return true;
}

static void
receive_intent_start(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	if (typeinfo->natts != 1 || TupleDescAttr(typeinfo, 0)->atttypid != XIDOID)
		elog(ERROR, "managed publication authorization returned an invalid result type");
}

static void
receive_intent_nop(DestReceiver *self)
{
}

static Datum
send_bytea(const char *bytes, Size len)
{
	bytea	   *value = palloc(VARHDRSZ + len);

	SET_VARSIZE(value, VARHDRSZ + len);
	memcpy(VARDATA(value), bytes, len);
	return PointerGetDatum(value);
}

/*
 * Execute a real top-level CALL. A regular SPI utility call is atomic, and a
 * nonatomic SPI call alone cannot supply the portal snapshot when nested CALL
 * processing crosses COMMIT. Keep planning and parameters in portal memory,
 * so the procedure can commit and then return its original transaction ID.
 */
static TransactionId
commit_send_intent(ManagedSendHold * hold)
{
	static const char *query =
		"CALL lakehouse.authorize_publication_send($1,$2,$3,$4,$5,$6,$7,NULL)";
	static const Oid types[] = {UUIDOID, INT8OID, BYTEAOID, INT8OID, INT8OID, BYTEAOID, BYTEAOID};
	Portal		portal;
	MemoryContext old_context;
	List	   *raw;
	List	   *queries;
	List	   *plan;
	ParamListInfo params;
	Datum		values[lengthof(types)];
	QueryCompletion completion;
	ManagedSendResult result = {
		.receiver = {
			.receiveSlot = receive_intent_xid,
			.rStartup = receive_intent_start,
			.rShutdown = receive_intent_nop,
			.rDestroy = receive_intent_nop,
			.mydest = DestNone,
		},
		.xid = InvalidTransactionId,
	};

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	portal = CreateNewPortal();
	portal->visible = false;
	old_context = MemoryContextSwitchTo(portal->portalContext);

	raw = pg_parse_query(query);
	PushActiveSnapshot(GetTransactionSnapshot());
	queries = pg_analyze_and_rewrite_fixedparams(linitial_node(RawStmt, raw),
												 query, types, lengthof(types), NULL);
	plan = pg_plan_queries(queries, query, 0, NULL);
	PopActiveSnapshot();

	values[0] = UUIDPGetDatum(&hold->operation);
	values[1] = Int64GetDatum(hold->generation);
	values[2] = send_bytea(hold->input_digest, SEND_DIGEST_BYTES);
	values[3] = Int64GetDatum(hold->parent);
	values[4] = Int64GetDatum(hold->snapshot);
	values[5] = send_bytea(hold->proposal_digest, SEND_DIGEST_BYTES);
	values[6] = send_bytea(hold->body, hold->body_len);
	params = makeParamList(lengthof(types));
	for (int i = 0; i < lengthof(types); i++)
	{
		params->params[i].value = values[i];
		params->params[i].isnull = i == 3 && hold->parent_isnull;
		params->params[i].ptype = types[i];
	}
	MemoryContextSwitchTo(old_context);

	PortalDefineQuery(portal, NULL, query, CMDTAG_CALL, plan, NULL);
	PortalStart(portal, params, 0, InvalidSnapshot);
	InitializeQueryCompletion(&completion);
	if (!PortalRun(portal, FETCH_ALL, true, &result.receiver, &result.receiver, &completion))
		elog(ERROR, "managed publication authorization did not finish");
	PortalDrop(portal, false);

	/* The procedure, not this trailing transaction, must have committed. */
	if (!TransactionIdIsValid(result.xid) || !TransactionIdDidCommit(result.xid))
		elog(ERROR, "managed publication authorization did not commit its intent");
	CommitTransactionCommand();
	return result.xid;
}

void
lakehouse_managed_send_hold_main(Datum main_arg)
{
	dsm_segment *segment = dsm_attach(DatumGetUInt32(main_arg));
	ManagedSendHold *hold;

	if (segment == NULL)
		proc_exit(0);
	hold = dsm_segment_address(segment);

	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();
	PG_TRY();
	{
		BackgroundWorkerInitializeConnectionByOid(hold->database, hold->role, 0);
		if (superuser())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("managed publication send hold requires a non-superuser role")));

		hold->intent_xid = commit_send_intent(hold);
		pg_write_barrier();
		pg_atomic_write_u32(&hold->outcome, MANAGED_SEND_HELD);
	}
	PG_CATCH();
	{
		hold->sqlstate = geterrcode();
		FlushErrorState();
		AbortCurrentTransaction();
		pg_write_barrier();
		pg_atomic_write_u32(&hold->outcome, MANAGED_SEND_ERROR);
	}
	PG_END_TRY();

	dsm_detach(segment);
	proc_exit(0);
}

static BgwHandleStatus
wait_for_send_hold(BackgroundWorkerHandle *handle)
{
	instr_time	started;

	INSTR_TIME_SET_CURRENT(started);
	for (;;)
	{
		BgwHandleStatus status;
		pid_t		pid;
		instr_time	elapsed;
		double		remaining;

		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
		status = GetBackgroundWorkerPid(handle, &pid);
		if (status == BGWH_STOPPED || status == BGWH_POSTMASTER_DIED)
			return status;

		INSTR_TIME_SET_CURRENT(elapsed);
		INSTR_TIME_SUBTRACT(elapsed, started);
		remaining = SEND_HOLD_TIMEOUT_MS - INSTR_TIME_GET_MILLISEC(elapsed);
		if (remaining <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_QUERY_CANCELED),
					 errmsg("managed publication send hold timed out waiting for the worker")));
		if (WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					  (long) remaining + 1, WAIT_EVENT_BGWORKER_SHUTDOWN) & WL_POSTMASTER_DEATH)
			return BGWH_POSTMASTER_DIED;
	}
}

Datum
pg_lakehouse_hold_managed_publication_send(PG_FUNCTION_ARGS)
{
	dsm_segment *segment;
	ManagedSendHold *hold;
	BackgroundWorker worker = {0};
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	Size		body_len;
	uint32		outcome;
	Datum		result;

	if (superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("managed publication send hold requires a non-superuser role")));
	/* The launcher must not own row locks or uncommitted ledger writes. */
	if (RecoveryInProgress() || XactReadOnly || IsTransactionBlock() || IsSubTransaction() ||
		TransactionIdIsValid(GetTopTransactionIdIfAny()))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("managed publication send hold requires a standalone read-write command without an assigned transaction ID on an active primary")));

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2) ||
		PG_ARGISNULL(4) || PG_ARGISNULL(5) || PG_ARGISNULL(6))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("managed publication send hold requires a complete proposal")));
	body_len = toast_raw_datum_size(PG_GETARG_DATUM(6)) - VARHDRSZ;
	if (PG_GETARG_INT64(1) <= 0 ||
		toast_raw_datum_size(PG_GETARG_DATUM(2)) != VARHDRSZ + SEND_DIGEST_BYTES ||
		toast_raw_datum_size(PG_GETARG_DATUM(5)) != VARHDRSZ + SEND_DIGEST_BYTES ||
		body_len < 1 || body_len > MAX_SEND_REQUEST)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("managed publication send hold proposal exceeds its bounds or has an invalid generation")));

	segment = dsm_create(sizeof(ManagedSendHold), 0);
	hold = dsm_segment_address(segment);
	hold->database = MyDatabaseId;
	hold->role = GetUserId();
	hold->operation = *PG_GETARG_UUID_P(0);
	hold->generation = PG_GETARG_INT64(1);
	hold->parent_isnull = PG_ARGISNULL(3);
	hold->parent = hold->parent_isnull ? 0 : PG_GETARG_INT64(3);
	hold->snapshot = PG_GETARG_INT64(4);
	hold->body_len = body_len;
	memcpy(hold->input_digest, VARDATA_ANY(PG_GETARG_BYTEA_PP(2)), SEND_DIGEST_BYTES);
	memcpy(hold->proposal_digest, VARDATA_ANY(PG_GETARG_BYTEA_PP(5)), SEND_DIGEST_BYTES);
	memcpy(hold->body, VARDATA_ANY(PG_GETARG_BYTEA_PP(6)), body_len);
	pg_atomic_init_u32(&hold->outcome, MANAGED_SEND_PENDING);

	strlcpy(worker.bgw_name, MANAGED_SEND_HOLD_NAME, BGW_MAXLEN);
	strlcpy(worker.bgw_type, worker.bgw_name, BGW_MAXLEN);
	strlcpy(worker.bgw_library_name, "lakehouse", BGW_MAXLEN);
	strlcpy(worker.bgw_function_name, "lakehouse_managed_send_hold_main", BGW_MAXLEN);
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_notify_pid = MyProcPid;
	worker.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(segment));

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("could not register the managed publication send hold worker")));
	PG_TRY();
	{
		status = wait_for_send_hold(handle);
	}
	PG_CATCH();
	{
		TerminateBackgroundWorker(handle);
		PG_RE_THROW();
	}
	PG_END_TRY();

	outcome = pg_atomic_read_u32(&hold->outcome);
	pg_read_barrier();
	if (status != BGWH_STOPPED || outcome == MANAGED_SEND_PENDING)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("managed publication send hold worker stopped without a receipt")));
	if (outcome == MANAGED_SEND_ERROR)
		ereport(ERROR,
				(errcode(hold->sqlstate),
				 errmsg("managed publication send hold worker refused the proposal")));

	result = DirectFunctionCall1(jsonb_in, CStringGetDatum(
														   psprintf("{\"status\":\"held\",\"intent_xid\":\"%u\",\"lookup_only\":true}",
																	hold->intent_xid)));
	dsm_detach(segment);
	pfree(handle);
	return result;
}
