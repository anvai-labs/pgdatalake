/*-------------------------------------------------------------------------
 *
 * lakehouse_core.c
 *		C glue for pgdatalake-core, the Rust core of the format-generic
 *		lakehouse storage engine (specs/lakehouse/01-architecture.md,
 *		decision D2). Iceberg is the first FORMAT layer inside the core.
 *
 * M0 scope: SQL exposure of the core version. The full TableAM
 * implementation grows alongside this file from M2 on (iceberg.c,
 * scan.c, insert.c, ...; see doc 01 §2 for the planned layout).
 *
 * Portions Copyright (c) 2026, Anvai Labs
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "pg_datalake_core.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

/*
 * lakehouse.version()
 *
 * Returns the pgdatalake-core version string (see core/pgdatalake-core/src/ffi.rs
 * for the documented format `pgdatalake-core <semver> (<milestone>)`). The Rust
 * core owns the string; it is a static, so no lifetime management is needed.
 */
PG_FUNCTION_INFO_V1(pg_lakehouse_version);

Datum
pg_lakehouse_version(PG_FUNCTION_ARGS)
{
	const char *core_version = pg_datalake_core_version();

	PG_RETURN_TEXT_P(cstring_to_text(core_version));
}

/*
 * lakehouse.scan_files(endpoint text, warehouse text, token text,
 *                      namespace text, table_name text,
 *                      s3_endpoint text, s3_access_key text,
 *                      s3_secret_key text)
 *
 * Plans a scan over one REST-catalog table through the FFI and returns the
 * task list (file_path text, record_count bigint). M1 slice 9: this is the
 * SQL-visible seam over pg_datalake_scan_new/task_*; the FDW executor
 * (later slice) reuses the same pipeline per-scan.
 *
 * STRICT: any NULL argument yields NULL. A planning failure raises
 * ERRCODE_CONNECTION_EXCEPTION with the client's error message (which names
 * the endpoint — doc 03 §5: fail fast at plan time).
 */
PG_FUNCTION_INFO_V1(pg_lakehouse_scan_files);

Datum
pg_lakehouse_scan_files(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		char		errbuf[1024];
		ScanSession *session;
		const char *endpoint;
		const char *warehouse;
		const char *token;
		const char *namespace;
		const char *table;
		const char *s3_endpoint;
		const char *s3_access_key;
		const char *s3_secret_key;
		TupleDesc	tupdesc;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		endpoint = text_to_cstring(PG_GETARG_TEXT_PP(0));
		warehouse = text_to_cstring(PG_GETARG_TEXT_PP(1));
		token = text_to_cstring(PG_GETARG_TEXT_PP(2));
		namespace = text_to_cstring(PG_GETARG_TEXT_PP(3));
		table = text_to_cstring(PG_GETARG_TEXT_PP(4));

		/*
		 * storage credentials come from the caller, never from the catalog
		 * (doc 09-security.md: secrets are client-side settings)
		 */
		s3_endpoint = text_to_cstring(PG_GETARG_TEXT_PP(5));
		s3_access_key = text_to_cstring(PG_GETARG_TEXT_PP(6));
		s3_secret_key = text_to_cstring(PG_GETARG_TEXT_PP(7));

		errbuf[0] = '\0';
		session = pg_datalake_scan_new(endpoint,
									   warehouse,
									   token,
									   namespace,
									   table,
									   s3_endpoint,
									   s3_access_key,
									   s3_secret_key,
									   NULL,
									   NULL,
									   errbuf,
									   sizeof(errbuf));
		if (session == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("lakehouse: could not plan scan: %s", errbuf)));

		funcctx->user_fctx = session;
		funcctx->max_calls = (uint64) pg_datalake_scan_task_count(session);

		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("return type of lakehouse.scan_files is not composite")));
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		ScanSession *session = (ScanSession *) funcctx->user_fctx;
		const char *file_path;
		Datum		values[2];
		bool		nulls[2] = {false, false};
		HeapTuple	tuple;

		file_path = pg_datalake_scan_task_file(session, funcctx->call_cntr);
		values[0] = CStringGetTextDatum(file_path);
		values[1] = Int64GetDatum(pg_datalake_scan_task_rows(session,
															 funcctx->call_cntr));
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}

	/* done: free the session before leaving the SRF */
	pg_datalake_scan_free((ScanSession *) funcctx->user_fctx);
	funcctx->user_fctx = NULL;

	SRF_RETURN_DONE(funcctx);
}
