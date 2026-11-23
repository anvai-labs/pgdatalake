/*
 * managed_lookup.c
 *    One-shot native preflight for a retained managed publication proposal.
 *
 * This path reads the ledger in a dedicated PostgreSQL worker. It confers no
 * authority to acquire the operation, contact a provider, or finalize a write.
 */

#include "postgres.h"

#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/pg_type_d.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "tcop/tcopprot.h"
#include "utils/fmgrprotos.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"
#include "utils/uuid.h"

#define MANAGED_LOOKUP_NAME "lakehouse managed lookup preflight"
#define MANAGED_LOOKUP_RECEIPT_SIZE 2048

typedef struct ManagedLookupRequest
{
	Oid			database;
	Oid			role;
	pg_uuid_t	operation;
	int64		generation;
} ManagedLookupRequest;

StaticAssertDecl(sizeof(ManagedLookupRequest) <= BGW_EXTRALEN,
				 "managed lookup request must fit in background worker registration");

typedef enum ManagedLookupOutcome
{
	MANAGED_LOOKUP_PENDING,
	MANAGED_LOOKUP_READY,
	MANAGED_LOOKUP_ERROR,
} ManagedLookupOutcome;

typedef struct ManagedLookupReceipt
{
	pg_atomic_uint32 outcome;
	int			sqlstate;
	char		json[MANAGED_LOOKUP_RECEIPT_SIZE];
} ManagedLookupReceipt;

PG_FUNCTION_INFO_V1(pg_lakehouse_preflight_managed_lookup);
PGDLLEXPORT void lakehouse_managed_lookup_main(Datum main_arg);

/*
 * The 1.10 loader checks binding/attempt identity in one snapshot. Check the
 * generation and the retained body on that same snapshot. Leave all bytes
 * owned by SPI and carry only a small receipt out of this transaction.
 */
static void
load_retained_lookup(const ManagedLookupRequest *request, ManagedLookupReceipt *receipt)
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

void
lakehouse_managed_lookup_main(Datum main_arg)
{
	dsm_segment *segment;
	ManagedLookupRequest request;
	ManagedLookupReceipt *receipt;

	memcpy(&request, MyBgworkerEntry->bgw_extra, sizeof(request));
	segment = dsm_attach(DatumGetUInt32(main_arg));
	if (segment == NULL)
		proc_exit(0);			/* launcher exited before we attached */
	receipt = dsm_segment_address(segment);

	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();
	PG_TRY();
	{
		BackgroundWorkerInitializeConnectionByOid(request.database, request.role, 0);
		if (superuser())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("managed publication lookup requires a non-superuser role")));

		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		XactReadOnly = true;
		set_config_option("search_path", "pg_catalog, pg_temp",
						  PGC_USERSET, PGC_S_SESSION, GUC_ACTION_LOCAL, true, 0, false);
		PushActiveSnapshot(GetTransactionSnapshot());
		load_retained_lookup(&request, receipt);
		PopActiveSnapshot();
		CommitTransactionCommand();

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

	dsm_detach(segment);
	proc_exit(0);
}

Datum
pg_lakehouse_preflight_managed_lookup(PG_FUNCTION_ARGS)
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
	request.operation = *PG_GETARG_UUID_P(0);
	request.generation = PG_GETARG_INT64(1);
	if (request.generation <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("managed publication lookup generation must be positive")));

	segment = dsm_create(sizeof(ManagedLookupReceipt), 0);
	receipt = dsm_segment_address(segment);
	pg_atomic_init_u32(&receipt->outcome, MANAGED_LOOKUP_PENDING);
	strlcpy(worker.bgw_name, MANAGED_LOOKUP_NAME, BGW_MAXLEN);
	strlcpy(worker.bgw_type, MANAGED_LOOKUP_NAME, BGW_MAXLEN);
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
