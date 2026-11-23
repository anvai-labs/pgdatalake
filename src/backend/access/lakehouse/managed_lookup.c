/*
 * managed_lookup.c
 *    One-shot native lookup for a retained managed publication proposal.
 *
 * A dedicated worker can preflight local evidence, or reconcile using an
 * explicit read-only credential grant. Neither operation can submit a send.
 */

#include "postgres.h"

#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/pg_foreign_server_d.h"
#include "catalog/pg_type_d.h"
#include "commands/defrem.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "foreign/foreign.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pg_datalake_core.h"
#include "port/atomics.h"
#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "tcop/tcopprot.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/uuid.h"

#define MANAGED_LOOKUP_NAME "lakehouse managed lookup preflight"
#define MANAGED_RECONCILE_NAME "lakehouse managed lookup reconcile"
#define MANAGED_LOOKUP_RECEIPT_SIZE 2048

typedef struct ManagedLookupRequest
{
	Oid			database;
	Oid			role;
	pg_uuid_t	operation;
	int64		generation;
	bool		reconcile;
}			ManagedLookupRequest;

StaticAssertDecl(sizeof(ManagedLookupRequest) <= BGW_EXTRALEN,
				 "managed lookup request must fit in background worker registration");

typedef enum ManagedLookupOutcome
{
	MANAGED_LOOKUP_PENDING,
	MANAGED_LOOKUP_READY,
	MANAGED_LOOKUP_ERROR,
}			ManagedLookupOutcome;

typedef struct ManagedLookupReceipt
{
	pg_atomic_uint32 outcome;
	int			sqlstate;
	char		json[MANAGED_LOOKUP_RECEIPT_SIZE];
}			ManagedLookupReceipt;

typedef struct ManagedLookupWork
{
	char	   *scope;
	bytea	   *body;
	bytea	   *input_digest;
	bytea	   *proposal_digest;
	pg_uuid_t	authority;
	pg_uuid_t	table;
	int64		snapshot;
	int64		generation;
	PgDatalakeLookupCredentials credentials;
}			ManagedLookupWork;

PG_FUNCTION_INFO_V1(pg_lakehouse_preflight_managed_lookup);
PG_FUNCTION_INFO_V1(pg_lakehouse_reconcile_managed_lookup);
PGDLLEXPORT void lakehouse_managed_lookup_main(Datum main_arg);

/*
 * The 1.10 loader checks binding/attempt identity in one snapshot. Check the
 * generation and the retained body on that same snapshot. Leave all bytes
 * owned by SPI and carry only a small receipt out of this transaction.
 */
static void
load_retained_lookup(const ManagedLookupRequest * request, ManagedLookupReceipt * receipt)
{
	static const char *query =
		"SELECT owner_generation, verified_snapshot_id IS NOT NULL,"
		"       attempt_owner_generation IS NOT NULL,"
		"       CASE WHEN owner_generation = $2 AND verified_snapshot_id IS NULL"
		"          AND attempt_owner_generation BETWEEN 1 AND owner_generation"
		"          AND proposed_snapshot_id > 0 AND octet_length(input_digest) = 32"
		"          AND octet_length(proposal_digest) = 32"
		"          AND CASE WHEN octet_length(proposal_body) BETWEEN 1 AND 65536"
		"                   THEN sha256(proposal_body) = proposal_digest"
		"                   ELSE false END"
		"         THEN jsonb_build_object("
		"           'operation_id', operation_id, 'authority_id', authority_id,"
		"           'table_uuid', table_uuid, 'binding_id', binding_id,"
		"           'incarnation_id', incarnation_id, 'binding_generation', binding_generation::text,"
		"           'owner_generation', owner_generation::text,"
		"           'attempt_owner_generation', attempt_owner_generation::text,"
		"           'expected_parent', expected_parent::text, 'proposed_snapshot_id', proposed_snapshot_id::text,"
		"           'input_digest', encode(input_digest, 'hex'),"
		"           'proposal_digest', encode(proposal_digest, 'hex'),"
		"           'proposal_bytes', octet_length(proposal_body), 'worker_pid', pg_backend_pid())"
		"       END"
		" FROM lakehouse.load_managed_publication_job($1)";
	Oid			types[] = {UUIDOID, INT8OID};
	Datum		values[] = {UUIDPGetDatum(&request->operation),
	Int64GetDatum(request->generation)};
	HeapTuple	row;
	TupleDesc	desc;
	bool		isnull;
	int64		generation;
	bool		complete;
	bool		has_attempt;
	char	   *json;

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "managed lookup could not connect to SPI");
	if (SPI_execute_with_args(query, lengthof(types), types, values, NULL,
							  true, 2) != SPI_OK_SELECT)
		elog(ERROR, "managed lookup could not read its retained proposal");
	if (SPI_processed != 1)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("managed publication lookup has no bound operation")));

	row = SPI_tuptable->vals[0];
	desc = SPI_tuptable->tupdesc;
	generation = DatumGetInt64(SPI_getbinval(row, desc, 1, &isnull));
	if (isnull || generation != request->generation)
		ereport(ERROR,
				(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
				 errmsg("stale managed publication lookup generation")));
	complete = DatumGetBool(SPI_getbinval(row, desc, 2, &isnull));
	Assert(!isnull);
	has_attempt = DatumGetBool(SPI_getbinval(row, desc, 3, &isnull));
	Assert(!isnull);
	if (complete || !has_attempt)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("managed publication lookup has no pending retained attempt")));

	json = SPI_getvalue(row, desc, 4);
	if (json == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("managed publication lookup has invalid retained proposal evidence")));
	if (strlcpy(receipt->json, json, sizeof(receipt->json)) >= sizeof(receipt->json))
		elog(ERROR, "managed publication lookup receipt is too large");
	SPI_finish();
}

/*
 * Copy the validated SPI tuple before commit. Read the mapping's secret
 * options directly in the worker and keep them out of DSM and SQL text.
 */
static void
load_lookup_work(const ManagedLookupRequest * request, MemoryContext context,
				 ManagedLookupWork * work)
{
	static const char *query =
		"SELECT jsonb_build_object("
		"  'target', jsonb_build_object("
		"    'binding', jsonb_build_object('authority_id', d.authority_id,"
		"          'binding_id', d.binding_id, 'incarnation_id', d.incarnation_id,"
		"          'binding_generation', d.binding_generation),"
		"    'table_uuid', d.table_uuid, 'table_root', d.storage_root,"
		"    'schema_id', d.schema_id, 'spec_id', d.spec_id, 'ref_name', d.ref_name,"
		"    'catalog_endpoint', d.catalog_endpoint, 'catalog_prefix', d.catalog_prefix,"
		"    'namespace', d.namespace_components, 'table_name', d.table_name,"
		"    'object_endpoint', d.object_endpoint, 's3_region', d.s3_region),"
		"  'operation_id', j.operation_id, 'owner_generation', j.owner_generation,"
		"  'input_digest', encode(j.input_digest, 'hex'),"
		"  'expected_parent', j.expected_parent, 'proposed_snapshot_id', j.proposed_snapshot_id,"
		"  'proposal_digest', encode(j.proposal_digest, 'hex')),"
		"  j.proposal_body, j.input_digest, j.proposal_digest, j.authority_id, j.table_uuid,"
		"  j.proposed_snapshot_id, g.mapping_id, m.srvid,"
		"  floor(extract(epoch FROM g.expires_at))::bigint"
		" FROM lakehouse.load_managed_publication_job($1) j"
		" JOIN lakehouse.get_publication_lookup_binding($1) d ON d.binding_id = j.binding_id"
		" JOIN lakehouse.publication_lookup_grants g ON g.binding_id = j.binding_id"
		" JOIN pg_catalog.pg_user_mappings m ON m.umid = g.mapping_id AND m.umuser = g.worker_role"
		" WHERE g.worker_role = $2 AND g.expires_at > clock_timestamp()";
	Oid			types[] = {UUIDOID, OIDOID};
	Datum		args[] = {UUIDPGetDatum(&request->operation), ObjectIdGetDatum(request->role)};
	HeapTuple	tuple;
	TupleDesc	desc;
	Datum		values[10];
	UserMapping *mapping;
	Oid			server;
	int64		expires;
	MemoryContext old_context;
	char	   *scope;
	ListCell   *cell;

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "managed lookup could not connect to SPI");
	if (SPI_execute_with_args(query, lengthof(types), types, args, NULL,
							  true, 2) != SPI_OK_SELECT)
		elog(ERROR, "managed lookup could not resolve its credential grant");
	if (SPI_processed != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("managed lookup requires an approved role-specific credential mapping")));

	tuple = SPI_tuptable->vals[0];
	desc = SPI_tuptable->tupdesc;
	scope = SPI_getvalue(tuple, desc, 1);
	for (int i = 1; i < lengthof(values); i++)
	{
		bool		isnull;

		values[i] = SPI_getbinval(tuple, desc, i + 1, &isnull);
		if (isnull)
			elog(ERROR, "managed lookup received incomplete retained evidence");
	}

	server = DatumGetObjectId(values[8]);
	expires = DatumGetInt64(values[9]);
	if (scope == NULL || strlen(scope) > 16384 || expires <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("managed lookup scope or credential expiry is outside the supported bounds")));

	/*
	 * With USAGE (or ownership) the worker can inspect or modify its mapping.
	 * A dedicated operator-owned lookup server makes substitution fail
	 * closed.
	 */
	if (object_ownercheck(ForeignServerRelationId, server, request->role) ||
		object_aclcheck(ForeignServerRelationId, server, request->role, ACL_USAGE) == ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("managed lookup requires an operator-owned credential mapping")));

	mapping = GetUserMapping(request->role, server);
	if (mapping->umid != DatumGetObjectId(values[7]))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("managed lookup credential mapping no longer matches its approval")));

	old_context = MemoryContextSwitchTo(context);
	work->scope = pstrdup(scope);
	work->body = DatumGetByteaPCopy(values[1]);
	work->input_digest = DatumGetByteaPCopy(values[2]);
	work->proposal_digest = DatumGetByteaPCopy(values[3]);
	work->authority = *DatumGetUUIDP(values[4]);
	work->table = *DatumGetUUIDP(values[5]);
	work->snapshot = DatumGetInt64(values[6]);
	work->credentials.expires_unix_seconds = expires;
	foreach(cell, mapping->options)
	{
		DefElem    *option = lfirst_node(DefElem, cell);
		const char *value = defGetString(option);

		if (strlen(value) > 4096)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("managed lookup credential exceeds the supported length")));
		if (strcmp(option->defname, "token") == 0)
			work->credentials.catalog_token = pstrdup(value);
		else if (strcmp(option->defname, "s3_access_key") == 0)
			work->credentials.access_key = pstrdup(value);
		else if (strcmp(option->defname, "s3_secret_key") == 0)
			work->credentials.secret_key = pstrdup(value);
	}
	MemoryContextSwitchTo(old_context);
	if (!work->credentials.catalog_token || !*work->credentials.catalog_token ||
		!work->credentials.access_key || !*work->credentials.access_key ||
		!work->credentials.secret_key || !*work->credentials.secret_key)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("managed lookup requires complete catalog and object credentials")));
	SPI_finish();
}

static int64
advance_lookup_generation(const ManagedLookupRequest * request)
{
	Oid			types[] = {UUIDOID, INT8OID};
	Datum		args[] = {UUIDPGetDatum(&request->operation), Int64GetDatum(request->generation)};
	bool		isnull;
	int64		generation;

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "managed lookup could not connect to SPI");
	if (SPI_execute_with_args("SELECT lakehouse.advance_publication_generation($1, $2)",
							  lengthof(types), types, args, NULL, false, 1) != SPI_OK_SELECT ||
		SPI_processed != 1)
		elog(ERROR, "managed lookup could not advance its owner generation");
	generation = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
											 SPI_tuptable->tupdesc, 1, &isnull));
	Assert(!isnull);
	SPI_finish();
	return generation;
}

static void
finish_verified_lookup(const ManagedLookupRequest * request, ManagedLookupWork * work,
					   ManagedLookupReceipt * receipt)
{
	Oid			types[] = {UUIDOID, INT8OID, UUIDOID, UUIDOID, BYTEAOID, INT8OID, BYTEAOID};
	Datum		args[] = {UUIDPGetDatum(&request->operation), Int64GetDatum(work->generation),
		UUIDPGetDatum(&work->authority), UUIDPGetDatum(&work->table),
		PointerGetDatum(work->input_digest), Int64GetDatum(work->snapshot),
	PointerGetDatum(work->proposal_digest)};
	int			members;
	bool		isnull;

	StartTransactionCommand();
	set_config_option("search_path", "pg_catalog, pg_temp",
					  PGC_USERSET, PGC_S_SESSION, GUC_ACTION_LOCAL, true, 0, false);
	set_config_option("synchronous_commit", "local",
					  PGC_USERSET, PGC_S_SESSION, GUC_ACTION_LOCAL, true, 0, false);
	PushActiveSnapshot(GetTransactionSnapshot());
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "managed lookup could not connect to SPI");
	if (SPI_execute_with_args(
							  "SELECT lakehouse.finalize_publication($1, $2, $3, $4, $5, $6, $7)",
							  lengthof(types), types, args, NULL, false, 1) != SPI_OK_SELECT ||
		SPI_processed != 1)
		elog(ERROR, "managed lookup could not finalize its verified proposal");
	members = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
	Assert(!isnull);
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	snprintf(receipt->json, sizeof(receipt->json),
			 "{\"status\":\"verified\",\"owner_generation\":\"" INT64_FORMAT "\","
			 "\"snapshot_id\":\"" INT64_FORMAT "\",\"members\":%d}",
			 work->generation, work->snapshot, members);
}

void
lakehouse_managed_lookup_main(Datum main_arg)
{
	dsm_segment *segment;
	ManagedLookupRequest request;
	ManagedLookupReceipt *receipt;
	ManagedLookupWork work = {0};
	MemoryContext context;

	memcpy(&request, MyBgworkerEntry->bgw_extra, sizeof(request));
	segment = dsm_attach(DatumGetUInt32(main_arg));
	if (segment == NULL)
		proc_exit(0);			/* launcher exited before we attached */
	receipt = dsm_segment_address(segment);
	context = AllocSetContextCreate(TopMemoryContext, "managed publication lookup",
									ALLOCSET_DEFAULT_SIZES);

	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();
	PG_TRY();
	{
		BackgroundWorkerInitializeConnectionByOid(request.database, request.role, 0);
		if (superuser())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("managed publication lookup requires a non-superuser role")));

		if (request.reconcile && !enableFsync)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("managed publication reconciliation requires fsync")));

		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		XactReadOnly = !request.reconcile;
		set_config_option("search_path", "pg_catalog, pg_temp",
						  PGC_USERSET, PGC_S_SESSION, GUC_ACTION_LOCAL, true, 0, false);
		PushActiveSnapshot(GetTransactionSnapshot());
		load_retained_lookup(&request, receipt);
		if (request.reconcile)
		{
			load_lookup_work(&request, context, &work);
			work.generation = advance_lookup_generation(&request);
		}
		PopActiveSnapshot();
		CommitTransactionCommand();

		if (request.reconcile)
		{
			PgDatalakeManagedLookupStatus status;

			/*
			 * SQL state has been copied and committed. No PostgreSQL
			 * callbacks or interrupt longjmp can run across Rust.
			 */
			CHECK_FOR_INTERRUPTS();
			status = pg_datalake_lookup_managed_publication(
															(const uint8 *) work.scope, strlen(work.scope),
															(const uint8 *) VARDATA_ANY(work.body), VARSIZE_ANY_EXHDR(work.body),
															&work.credentials);
			CHECK_FOR_INTERRUPTS();
			if (status == ManagedLookupAccepted)
				finish_verified_lookup(&request, &work, receipt);
			else if (status == ManagedLookupInvalid)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_EXCEPTION),
						 errmsg("managed lookup rejected its scope or retained proposal")));
			else
				snprintf(receipt->json, sizeof(receipt->json),
						 "{\"status\":\"%s\",\"owner_generation\":\"" INT64_FORMAT "\"}",
						 status == ManagedLookupUnresolved ? "unresolved" : "unavailable",
						 work.generation);
		}

		pg_write_barrier();
		pg_atomic_write_u32(&receipt->outcome, MANAGED_LOOKUP_READY);
	}
	PG_CATCH();
	{
		/* Keep backend diagnostics out of the cross-process receipt. */
		receipt->sqlstate = geterrcode();
		FlushErrorState();
		AbortCurrentTransaction();
		pg_write_barrier();
		pg_atomic_write_u32(&receipt->outcome, MANAGED_LOOKUP_ERROR);
	}
	PG_END_TRY();

	MemoryContextDelete(context);
	dsm_detach(segment);
	proc_exit(0);
}

static Datum
run_managed_lookup(FunctionCallInfo fcinfo, bool reconcile)
{
	ManagedLookupRequest request = {0};
	BackgroundWorker worker = {0};
	BackgroundWorkerHandle *handle;
	dsm_segment *segment;
	ManagedLookupReceipt *receipt;
	BgwHandleStatus status;
	uint32		outcome;
	Datum		result;

	if (superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("managed publication lookup requires a non-superuser role")));
	if (RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("managed publication lookup requires an active primary")));

	request.database = MyDatabaseId;
	request.role = GetUserId();
	request.reconcile = reconcile;
	request.operation = *PG_GETARG_UUID_P(0);
	request.generation = PG_GETARG_INT64(1);
	if (request.generation <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("managed publication lookup generation must be positive")));

	segment = dsm_create(sizeof(ManagedLookupReceipt), 0);
	receipt = dsm_segment_address(segment);
	pg_atomic_init_u32(&receipt->outcome, MANAGED_LOOKUP_PENDING);
	strlcpy(worker.bgw_name, reconcile ? MANAGED_RECONCILE_NAME : MANAGED_LOOKUP_NAME, BGW_MAXLEN);
	strlcpy(worker.bgw_type, worker.bgw_name, BGW_MAXLEN);
	strlcpy(worker.bgw_library_name, "lakehouse", BGW_MAXLEN);
	strlcpy(worker.bgw_function_name, "lakehouse_managed_lookup_main", BGW_MAXLEN);
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_notify_pid = MyProcPid;
	worker.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(segment));
	memcpy(worker.bgw_extra, &request, sizeof(request));

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("could not register the managed publication lookup worker")));
	PG_TRY();
	{
		status = WaitForBackgroundWorkerShutdown(handle);
	}
	PG_CATCH();
	{
		/* A canceled SQL caller must not leave a worker waiting on its locks. */
		TerminateBackgroundWorker(handle);
		PG_RE_THROW();
	}
	PG_END_TRY();

	outcome = pg_atomic_read_u32(&receipt->outcome);
	pg_read_barrier();
	if (status != BGWH_STOPPED || outcome == MANAGED_LOOKUP_PENDING)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("managed publication lookup worker stopped without a receipt")));
	if (outcome == MANAGED_LOOKUP_ERROR)
		ereport(ERROR,
				(errcode(receipt->sqlstate),
				 errmsg("managed publication lookup worker refused the retained proposal")));

	result = DirectFunctionCall1(jsonb_in, CStringGetDatum(receipt->json));
	dsm_detach(segment);
	pfree(handle);
	return result;
}

Datum
pg_lakehouse_preflight_managed_lookup(PG_FUNCTION_ARGS)
{
	return run_managed_lookup(fcinfo, false);
}

Datum
pg_lakehouse_reconcile_managed_lookup(PG_FUNCTION_ARGS)
{
	return run_managed_lookup(fcinfo, true);
}
