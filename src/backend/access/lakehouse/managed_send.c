/*
 * managed_send.c
 *    One-shot scoped managed preparation, authorization and dispatch.
 */

#include "postgres.h"

#include "access/detoast.h"
#include "access/table.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/pg_foreign_server_d.h"
#include "catalog/pg_type_d.h"
#include "commands/defrem.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "foreign/foreign.h"
#include "libpq/pqsignal.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "portability/instr_time.h"
#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/fmgrprotos.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/uuid.h"
#include "utils/wait_event.h"

#include "lakehouse_tableam.h"
#include "pg_datalake_core.h"

#define MANAGED_SEND_HOLD_NAME "lakehouse managed send hold"
#define MANAGED_SEND_PREPARE_NAME "lakehouse managed send prepare"
#define MANAGED_SEND_DISPATCH_NAME "lakehouse managed send dispatch"
#define SEND_DIGEST_BYTES 32
#define MAX_SEND_REQUEST 65536
#define SEND_HOLD_TIMEOUT_MS 10000
#define SEND_PREPARE_TIMEOUT_MS 20000

typedef enum ManagedSendMode
{
	SEND_HOLD,
	SEND_PREPARE,
	SEND_DISPATCH,
} ManagedSendMode;

typedef enum ManagedSendOutcome
{
	MANAGED_SEND_PENDING,
	MANAGED_SEND_HELD,
	MANAGED_SEND_PREPARED_DISCARDED,
	MANAGED_SEND_ACKNOWLEDGED,
	MANAGED_SEND_REFUSED,
	MANAGED_SEND_UNKNOWN,
	MANAGED_SEND_ERROR,
} ManagedSendOutcome;

/* Exact prepared request and result. This structure carries no credentials. */
typedef struct ManagedSendHold
{
	Oid			database;
	Oid			role;
	pg_uuid_t	operation;
	ManagedSendMode mode;
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
} ManagedSendHold;

typedef struct ManagedSendWork
{
	PgDatalakeNativeSendInputs inputs;
	PgDatalakeNativeSendCredentials credentials;
	PgDatalakePreparedSend *owner;
	Oid			mapping;
	Oid			server;
	Oid			source_relid;
	bytea	   *claim_digest;
} ManagedSendWork;

typedef struct ManagedSendResult
{
	DestReceiver receiver;
	TransactionId xid;
} ManagedSendResult;

PG_FUNCTION_INFO_V1(pg_lakehouse_hold_managed_publication_send);
PG_FUNCTION_INFO_V1(pg_lakehouse_prepare_managed_publication_send);
PG_FUNCTION_INFO_V1(pg_lakehouse_dispatch_managed_publication_send);
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
commit_send_intent(ManagedSendHold *hold)
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

/* Copy the tuple out of SPI and convert its strings to the Rust wire format. */
static const uint8 *
copy_prepare_json(const char *value, MemoryContext context, uintptr_t *length)
{
	const char *utf8;
	const uint8 *copy;

	if (value == NULL)
		elog(ERROR, "managed publication preparation returned incomplete JSON");
	utf8 = pg_server_to_any(value, strlen(value), PG_UTF8);
	*length = strlen(utf8);
	copy = (const uint8 *) MemoryContextStrdup(context, utf8);
	return copy;
}

static Datum
prepare_field(HeapTuple row, TupleDesc desc, int column)
{
	bool		isnull;
	Datum		value = SPI_getbinval(row, desc, column, &isnull);

	if (isnull)
		elog(ERROR, "managed publication preparation returned incomplete evidence");
	return value;
}

static void
load_prepare_credentials(PgDatalakeNativeSendCredentials *credentials, Oid role,
						 Oid server, Oid mapping_id, MemoryContext context)
{
	UserMapping *mapping;
	ListCell   *cell;

	if (object_ownercheck(ForeignServerRelationId, server, role) ||
		object_aclcheck(ForeignServerRelationId, server, role, ACL_USAGE) == ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("managed publication preparation requires an operator-owned mapping")));

	mapping = GetUserMapping(role, server);
	if (mapping->umid != mapping_id)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("managed publication credential mapping changed")));
	foreach(cell, mapping->options)
	{
		DefElem    *option = lfirst_node(DefElem, cell);
		const uint8 **field;
		uintptr_t  *length;
		const char *value;

		if (strcmp(option->defname, "token") == 0)
		{
			field = &credentials->catalog_token;
			length = &credentials->catalog_token_len;
		}
		else if (strcmp(option->defname, "s3_access_key") == 0)
		{
			field = &credentials->access_key;
			length = &credentials->access_key_len;
		}
		else if (strcmp(option->defname, "s3_secret_key") == 0)
		{
			field = &credentials->secret_key;
			length = &credentials->secret_key_len;
		}
		else
			continue;

		value = defGetString(option);
		*length = strlen(value);
		if (*length == 0 || *length > 4096)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("managed publication credential length is outside the allowed bounds")));
		*field = (const uint8 *) MemoryContextStrdup(context, value);
	}
	if (!credentials->catalog_token || !credentials->access_key || !credentials->secret_key)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("managed publication preparation requires complete credentials")));
}

static int
prepare_array(HeapTuple row, TupleDesc desc, int column, Oid type,
			  Datum **values, bool **nulls)
{
	int16		length;
	bool		by_value;
	char		alignment;
	int			count;

	get_typlenbyvalalign(type, &length, &by_value, &alignment);
	deconstruct_array(DatumGetArrayTypeP(prepare_field(row, desc, column)),
					  type, length, by_value, alignment, values, nulls, &count);
	return count;
}

static void
load_prepare_members(HeapTuple row, TupleDesc desc, PgDatalakeNativeSendInputs *inputs,
					 MemoryContext context)
{
	Datum	   *ids;
	Datum	   *counts;
	Datum	   *payloads;
	bool	   *ids_null;
	bool	   *counts_null;
	bool	   *payloads_null;
	int			ids_count;
	int			counts_count;
	int			payloads_count;
	PgDatalakeNativeSendMember *members;

	ids_count = prepare_array(row, desc, 7, INT8OID, &ids, &ids_null);
	counts_count = prepare_array(row, desc, 8, INT4OID, &counts, &counts_null);
	payloads_count = prepare_array(row, desc, 9, BYTEAOID, &payloads, &payloads_null);
	if (ids_count < 1 || ids_count > 1024 || counts_count != ids_count || payloads_count != ids_count)
		elog(ERROR, "managed publication preparation returned invalid membership");

	members = MemoryContextAlloc(context, sizeof(*members) * ids_count);
	for (int i = 0; i < ids_count; i++)
	{
		MemoryContext previous;
		bytea	   *payload;

		if (ids_null[i] || counts_null[i] || payloads_null[i])
			elog(ERROR, "managed publication preparation returned an incomplete member");
		previous = MemoryContextSwitchTo(context);
		payload = DatumGetByteaPCopy(payloads[i]);
		MemoryContextSwitchTo(previous);
		members[i] = (PgDatalakeNativeSendMember)
		{
			.segment_id = DatumGetInt64(ids[i]),
				.row_count = DatumGetInt32(counts[i]),
				.payload = (const uint8 *) VARDATA_ANY(payload),
				.payload_len = VARSIZE_ANY_EXHDR(payload),
		};
	}
	inputs->member_count = ids_count;
	inputs->members = members;
}

static void
load_prepare_work(ManagedSendHold *request, ManagedSendWork *work,
				  MemoryContext context, bool include_members)
{
	const char *query = include_members
		? "SELECT * FROM lakehouse.load_native_send_preparation($1, $2)"
		: "SELECT * FROM lakehouse.load_native_send_scope($1, $2)";
	PgDatalakeNativeSendInputs *inputs = &work->inputs;
	PgDatalakeNativeSendCredentials *credentials = &work->credentials;
	Oid			types[] = {UUIDOID, INT8OID};
	Datum		values[] = {UUIDPGetDatum(&request->operation), Int64GetDatum(request->generation)};
	HeapTuple	row;
	TupleDesc	desc;
	Relation	relation;
	int64		expiry;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	set_config_option("search_path", "pg_catalog, pg_temp",
					  PGC_USERSET, PGC_S_SESSION, GUC_ACTION_LOCAL, true, 0, false);
	PushActiveSnapshot(GetTransactionSnapshot());
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "managed publication preparation could not connect to SPI");
	if (SPI_execute_with_args(query,
							  lengthof(types), types, values, NULL, false, 1) != SPI_OK_SELECT ||
		SPI_processed != 1)
		elog(ERROR, "managed publication preparation could not load a bounded operation");

	row = SPI_tuptable->vals[0];
	desc = SPI_tuptable->tupdesc;
	inputs->request = copy_prepare_json(SPI_getvalue(row, desc, 1), context, &inputs->request_len);
	credentials->target = copy_prepare_json(SPI_getvalue(row, desc, 2), context, &credentials->target_len);
	work->source_relid = DatumGetObjectId(prepare_field(row, desc, 3));
	relation = table_open(work->source_relid, AccessShareLock);
	inputs->schema = copy_prepare_json(lakehouse_build_schema_json(RelationGetDescr(relation)),
									   context, &inputs->schema_len);
	table_close(relation, NoLock);
	work->mapping = DatumGetObjectId(prepare_field(row, desc, 4));
	work->server = DatumGetObjectId(prepare_field(row, desc, 5));
	expiry = DatumGetInt64(prepare_field(row, desc, 6));
	if (expiry <= 0)
		elog(ERROR, "managed publication preparation returned an invalid credential expiry");
	credentials->expires_unix_seconds = expiry;
	load_prepare_credentials(credentials, request->role, work->server, work->mapping, context);
	if (include_members)
		load_prepare_members(row, desc, inputs, context);
	else
	{
		MemoryContext previous = MemoryContextSwitchTo(context);

		work->claim_digest = DatumGetByteaPCopy(prepare_field(row, desc, 7));
		MemoryContextSwitchTo(previous);
	}
	inputs->credentials = credentials;

	SPI_finish();
	PopActiveSnapshot();
}

static void
prepare_send_work(ManagedSendHold *hold, ManagedSendWork *work,
				  PgDatalakeNativeSendProposal *proposal, MemoryContext context)
{
	load_prepare_work(hold, work, context, true);
	CommitTransactionCommand();

	CHECK_FOR_INTERRUPTS();
	Assert(!IsTransactionState());
	work->owner = pg_datalake_prepare_managed_send(&work->inputs, proposal);
	if (work->owner == NULL)
	{
		CHECK_FOR_INTERRUPTS();
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("managed publication preparation did not produce a bounded proposal")));
	}
}

static bool
same_bytes(const uint8 *left, uintptr_t left_len,
		   const uint8 *right, uintptr_t right_len)
{
	return left_len == right_len && (left_len == 0 || memcmp(left, right, left_len) == 0);
}

/* Scope, source layout and credentials must still agree under the WAL locks. */
static bool
same_send_work(const ManagedSendWork *original, const ManagedSendWork *current)
{
	return original->mapping == current->mapping &&
		original->server == current->server &&
		original->source_relid == current->source_relid &&
		original->credentials.expires_unix_seconds == current->credentials.expires_unix_seconds &&
		same_bytes(original->inputs.request, original->inputs.request_len,
				   current->inputs.request, current->inputs.request_len) &&
		same_bytes(original->inputs.schema, original->inputs.schema_len,
				   current->inputs.schema, current->inputs.schema_len) &&
		same_bytes(original->credentials.target, original->credentials.target_len,
				   current->credentials.target, current->credentials.target_len) &&
		same_bytes(original->credentials.catalog_token, original->credentials.catalog_token_len,
				   current->credentials.catalog_token, current->credentials.catalog_token_len) &&
		same_bytes(original->credentials.access_key, original->credentials.access_key_len,
				   current->credentials.access_key, current->credentials.access_key_len) &&
		same_bytes(original->credentials.secret_key, original->credentials.secret_key_len,
				   current->credentials.secret_key, current->credentials.secret_key_len) &&
		same_bytes(original->credentials.session_token, original->credentials.session_token_len,
				   current->credentials.session_token, current->credentials.session_token_len);
}

static ManagedSendOutcome
dispatch_prepared(ManagedSendHold *hold, ManagedSendWork *work,
				  const PgDatalakeNativeSendProposal *proposal, MemoryContext context)
{
	ManagedSendWork current = {0};
	PgDatalakeNativeSendAuthorization authorization;
	ManagedSendHold *intent;
	PgDatalakeNativeSendStatus status;

	load_prepare_work(hold, &current, context, false);
	if (!same_send_work(work, &current))
		ereport(ERROR,
				(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
				 errmsg("managed publication send scope or credentials changed during preparation")));

	if (proposal->body_len < 1 || proposal->body_len > MAX_SEND_REQUEST ||
		VARSIZE_ANY_EXHDR(current.claim_digest) != SEND_DIGEST_BYTES)
		elog(ERROR, "managed publication send authorization exceeds its bounds");

	/* Keep the exact request and the live Rust owner in this worker. */
	intent = MemoryContextAllocZero(context, sizeof(*intent));
	intent->operation = hold->operation;
	intent->generation = hold->generation;
	intent->parent_isnull = proposal->parent_is_null;
	intent->parent = proposal->parent;
	intent->snapshot = proposal->snapshot;
	intent->body_len = proposal->body_len;
	memcpy(intent->input_digest, VARDATA_ANY(current.claim_digest), SEND_DIGEST_BYTES);
	memcpy(intent->proposal_digest, proposal->digest, SEND_DIGEST_BYTES);
	memcpy(intent->body, proposal->body, proposal->body_len);

	hold->intent_xid = commit_send_intent(intent);
	CHECK_FOR_INTERRUPTS();
	authorization = (PgDatalakeNativeSendAuthorization)
	{
		.owner_generation = intent->generation,
			.intent_xid = hold->intent_xid,
			.parent_is_null = intent->parent_isnull,
			.parent = intent->parent,
			.snapshot = intent->snapshot,
	};
	memcpy(authorization.operation_id, intent->operation.data, UUID_LEN);
	memcpy(authorization.input_digest, intent->input_digest, SEND_DIGEST_BYTES);
	memcpy(authorization.proposal_digest, intent->proposal_digest, SEND_DIGEST_BYTES);

	/* The portal committed and released its locks. Rust does no PG callbacks. */
	Assert(!IsTransactionState());
	status = pg_datalake_submit_managed_send(&work->owner, &authorization);
	CHECK_FOR_INTERRUPTS();
	if (status == ManagedSendAcknowledged)
		return MANAGED_SEND_ACKNOWLEDGED;
	if (status == ManagedSendRefused)
		return MANAGED_SEND_REFUSED;
	return MANAGED_SEND_UNKNOWN;
}

void
lakehouse_managed_send_hold_main(Datum main_arg)
{
	dsm_segment *segment = dsm_attach(DatumGetUInt32(main_arg));
	ManagedSendHold *hold;
	MemoryContext context;
	ManagedSendWork *work;

	if (segment == NULL)
		proc_exit(0);
	hold = dsm_segment_address(segment);
	context = AllocSetContextCreate(TopMemoryContext, "managed publication send",
									ALLOCSET_DEFAULT_SIZES);
	work = MemoryContextAllocZero(context, sizeof(*work));

	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();
	PG_TRY();
	{
		ManagedSendOutcome outcome;

		BackgroundWorkerInitializeConnectionByOid(hold->database, hold->role, 0);
		if (superuser())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("managed publication send requires a non-superuser role")));

		if (hold->mode == SEND_DISPATCH && !enableFsync)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("managed publication dispatch requires fsync")));

		if (hold->mode == SEND_HOLD)
		{
			SetCurrentStatementStartTimestamp();
			StartTransactionCommand();
			hold->intent_xid = commit_send_intent(hold);
			outcome = MANAGED_SEND_HELD;
		}
		else
		{
			PgDatalakeNativeSendProposal proposal;

			prepare_send_work(hold, work, &proposal, context);
			hold->parent_isnull = proposal.parent_is_null;
			hold->parent = proposal.parent;
			hold->snapshot = proposal.snapshot;
			hold->body_len = proposal.body_len;
			if (hold->mode == SEND_DISPATCH)
			{
				CHECK_FOR_INTERRUPTS();
				outcome = dispatch_prepared(hold, work, &proposal, context);
			}
			else
			{
				pg_datalake_drop_managed_send(&work->owner);
				CHECK_FOR_INTERRUPTS();
				outcome = MANAGED_SEND_PREPARED_DISCARDED;
			}
		}
		pg_write_barrier();
		pg_atomic_write_u32(&hold->outcome, outcome);
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

	pg_datalake_drop_managed_send(&work->owner);
	MemoryContextDelete(context);
	dsm_detach(segment);
	proc_exit(0);
}

static BgwHandleStatus
wait_for_send_worker(BackgroundWorkerHandle *handle, long timeout)
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
		remaining = timeout - INSTR_TIME_GET_MILLISEC(elapsed);
		if (remaining <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_QUERY_CANCELED),
					 errmsg("managed publication send timed out waiting for the worker")));
		if (WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					  (long) remaining + 1, WAIT_EVENT_BGWORKER_SHUTDOWN) & WL_POSTMASTER_DEATH)
			return BGWH_POSTMASTER_DIED;
	}
}

static Datum
launch_managed_send_worker(FunctionCallInfo fcinfo, ManagedSendMode mode)
{
	dsm_segment *segment;
	ManagedSendHold *hold;
	BackgroundWorker worker = {0};
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	Size		body_len = 0;
	uint32		outcome;
	Datum		result;

	if (superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("managed publication send requires a non-superuser role")));
	/* The launcher must not own row locks or uncommitted ledger writes. */
	if (RecoveryInProgress() || XactReadOnly || IsTransactionBlock() || IsSubTransaction() ||
		TransactionIdIsValid(GetTopTransactionIdIfAny()))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("managed publication send requires a standalone read-write command without an assigned transaction ID on an active primary")));

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) ||
		(mode == SEND_HOLD && (PG_ARGISNULL(2) || PG_ARGISNULL(4) || PG_ARGISNULL(5) || PG_ARGISNULL(6))))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("managed publication send requires a complete request")));
	if (PG_GETARG_INT64(1) <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("managed publication send requires a positive owner generation")));
	if (mode == SEND_HOLD)
	{
		body_len = toast_raw_datum_size(PG_GETARG_DATUM(6)) - VARHDRSZ;
		if (toast_raw_datum_size(PG_GETARG_DATUM(2)) != VARHDRSZ + SEND_DIGEST_BYTES ||
			toast_raw_datum_size(PG_GETARG_DATUM(5)) != VARHDRSZ + SEND_DIGEST_BYTES ||
			body_len < 1 || body_len > MAX_SEND_REQUEST)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("managed publication send hold proposal exceeds its bounds")));
	}

	segment = dsm_create(sizeof(ManagedSendHold), 0);
	hold = dsm_segment_address(segment);
	hold->database = MyDatabaseId;
	hold->role = GetUserId();
	hold->operation = *PG_GETARG_UUID_P(0);
	hold->generation = PG_GETARG_INT64(1);
	hold->mode = mode;
	if (mode == SEND_HOLD)
	{
		hold->parent_isnull = PG_ARGISNULL(3);
		hold->parent = hold->parent_isnull ? 0 : PG_GETARG_INT64(3);
		hold->snapshot = PG_GETARG_INT64(4);
		hold->body_len = body_len;
		memcpy(hold->input_digest, VARDATA_ANY(PG_GETARG_BYTEA_PP(2)), SEND_DIGEST_BYTES);
		memcpy(hold->proposal_digest, VARDATA_ANY(PG_GETARG_BYTEA_PP(5)), SEND_DIGEST_BYTES);
		memcpy(hold->body, VARDATA_ANY(PG_GETARG_BYTEA_PP(6)), body_len);
	}
	pg_atomic_init_u32(&hold->outcome, MANAGED_SEND_PENDING);

	strlcpy(worker.bgw_name, mode == SEND_HOLD ? MANAGED_SEND_HOLD_NAME :
			(mode == SEND_PREPARE ? MANAGED_SEND_PREPARE_NAME : MANAGED_SEND_DISPATCH_NAME),
			BGW_MAXLEN);
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
				 errmsg("could not register the managed publication send worker")));
	PG_TRY();
	{
		status = wait_for_send_worker(handle, mode == SEND_HOLD ? SEND_HOLD_TIMEOUT_MS : SEND_PREPARE_TIMEOUT_MS);
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
				 errmsg("managed publication send worker stopped without a receipt")));
	if (outcome == MANAGED_SEND_ERROR)
		ereport(ERROR,
				(errcode(hold->sqlstate),
				 errmsg("managed publication send worker refused the request")));

	if (mode == SEND_PREPARE)
		result = DirectFunctionCall1(jsonb_in, CStringGetDatum(
															   psprintf("{\"status\":\"prepared_discarded\",\"expected_parent\":%s,"
																		"\"proposed_snapshot\":\"" INT64_FORMAT "\",\"proposal_bytes\":%zu}",
																		hold->parent_isnull ? "null" : psprintf("\"" INT64_FORMAT "\"", hold->parent),
																		hold->snapshot, hold->body_len)));
	else if (mode == SEND_DISPATCH)
	{
		const char *status_name;

		if (outcome == MANAGED_SEND_ACKNOWLEDGED)
			status_name = "acknowledged";
		else if (outcome == MANAGED_SEND_REFUSED)
			status_name = "dispatch_refused";
		else
			status_name = "dispatch_unknown";
		result = DirectFunctionCall1(jsonb_in, CStringGetDatum(
															   psprintf("{\"status\":\"%s\",\"intent_xid\":\"%u\","
																		"\"snapshot_id\":\"" INT64_FORMAT "\",\"lookup_only\":true}",
																		status_name, hold->intent_xid, hold->snapshot)));
	}
	else
		result = DirectFunctionCall1(jsonb_in, CStringGetDatum(
															   psprintf("{\"status\":\"held\",\"intent_xid\":\"%u\",\"lookup_only\":true}",
																		hold->intent_xid)));
	dsm_detach(segment);
	pfree(handle);
	return result;
}

Datum
pg_lakehouse_hold_managed_publication_send(PG_FUNCTION_ARGS)
{
	return launch_managed_send_worker(fcinfo, SEND_HOLD);
}

Datum
pg_lakehouse_prepare_managed_publication_send(PG_FUNCTION_ARGS)
{
	return launch_managed_send_worker(fcinfo, SEND_PREPARE);
}

Datum
pg_lakehouse_dispatch_managed_publication_send(PG_FUNCTION_ARGS)
{
	return launch_managed_send_worker(fcinfo, SEND_DISPATCH);
}
