/*-------------------------------------------------------------------------
 *
 * fdw.c
 *		Lakehouse foreign-data wrapper handler (specs/lakehouse/
 *		01-architecture.md D1: external tables attach through an FDW and
 *		show up as ordinary pg_class rows — one catalog, one visibility).
 *
 * M1 slice 11: plan/iterate over REST-catalog tables through the FFI.
 * Planning opens a session to size the estimate, Begin re-opens it
 * (fail-fast per doc 03 §5), Iterate drives pg_datalake_rows_* over each
 * task's parquet file, converting cells with the attribute input
 * functions (doc 05 type mapping completes in the type-matrix slice).
 * No pushdown yet: quals are evaluated locally by the executor.
 *
 * Portions Copyright (c) 2026, Anvai Labs
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "fmgr.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/explain_format.h"
#include "access/table.h"
#include "foreign/fdwapi.h"
#include "nodes/primnodes.h"
#include "foreign/foreign.h"
#include "optimizer/planmain.h"
#include "lakehouse_fdw.h"
#include "optimizer/tlist.h"
#include "nodes/makefuncs.h"
#include "utils/fmgroids.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/restrictinfo.h"
#include "utils/lsyscache.h"
#include "utils/fmgrprotos.h"
#include "tcop/tcopprot.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "access/parallel.h"
#include "port/atomics.h"
#include "utils/injection_point.h"
#include "utils/rel.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_operator.h"
#include "utils/syscache.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "access/reloptions.h"
#include "utils/json.h"
#include "lib/stringinfo.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "optimizer/optimizer.h"
#include "utils/fmgroids.h"
#include "utils/builtins.h"

#include "pg_datalake_core.h"

#include "lakehouse.h"
#include "batch_fill.h"

/*
 * LakehouseOptions moved to lakehouse.h (M2 slice 26): the table access
 * method shares the option block and the session pipeline.
 */

/* shared task-claim counter for parallel scans (slice 17) */


/*
 * Option homes (13-review #6, doc 09 authority model): each option has
 * exactly one legal home. Transport identity lives on the SERVER,
 * secrets on the USER MAPPING (per-user, ACL-controlled, one place to
 * rotate), relation identity/pin on the FOREIGN TABLE. The validator
 * enforces this at DDL; ExtractOptions enforces it again on use, so a
 * legacy pre-validator definition errors instead of silently using
 * credentials that were never authorized through a mapping.
 */
enum
{
	LOPT_SERVER = 1 << 0,
	LOPT_MAPPING = 1 << 1,
	LOPT_TABLE = 1 << 2
};

typedef struct LakehouseOptSpec
{
	const char *name;
	int			homes;
} LakehouseOptSpec;

static const LakehouseOptSpec lakehouse_opt_specs[] = {
	{"endpoint", LOPT_SERVER},
	{"warehouse", LOPT_SERVER},
	{"s3_endpoint", LOPT_SERVER},
	{"expected_table_uuid", LOPT_SERVER},
	{"storage_root", LOPT_SERVER},
	{"catalog_prefix", LOPT_SERVER},
	{"s3_region", LOPT_SERVER},
	{"token", LOPT_MAPPING},
	{"s3_access_key", LOPT_MAPPING},
	{"s3_secret_key", LOPT_MAPPING},
	{"table", LOPT_TABLE},
	{"snapshot_id", LOPT_TABLE},
};

bool
lakehouse_IsBoundServerOption(const char *name)
{
	return strcmp(name, "expected_table_uuid") == 0 ||
		strcmp(name, "storage_root") == 0 ||
		strcmp(name, "catalog_prefix") == 0 ||
		strcmp(name, "s3_region") == 0;
}

static const LakehouseOptSpec *
find_lakehouse_option(const char *name)
{
	int			i;

	for (i = 0; i < lengthof(lakehouse_opt_specs); i++)
		if (strcmp(lakehouse_opt_specs[i].name, name) == 0)
			return &lakehouse_opt_specs[i];
	return NULL;
}

/* human-readable home name for validator/use-time diagnostics */
static const char *
option_home_name(int home_flag)
{
	switch (home_flag)
	{
		case LOPT_SERVER:
			return "servers";
		case LOPT_MAPPING:
			return "user mappings";
		case LOPT_TABLE:
			return "foreign tables";
		default:
			/* multi-bit or unset homes: the wrapper-level catch-all */
			return "the lakehouse wrapper's supported objects";
	}
}

static void
reject_misplaced_option(const char *defname, const char *object_kind,
						int home_flag)
{
	const LakehouseOptSpec *spec = find_lakehouse_option(defname);

	ereport(ERROR,
			(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
			 spec != NULL ?
			 errmsg("option \"%s\" is not allowed on %s", defname,
					object_kind) :
			 errmsg("invalid option \"%s\"", defname),
			 spec != NULL ?
			 errdetail("Option \"%s\" belongs on %s.", defname,
					   option_home_name(spec->homes)) :
			 0));
}


PG_FUNCTION_INFO_V1(lakehouse_fdw_handler);

PG_FUNCTION_INFO_V1(lakehouse_fdw_validator);

/*
 * Canonical option validator (13-review #6): reject unknown and
 * misplaced options at DDL, so a wrapper created with
 * VALIDATOR lakehouse.lakehouse_fdw_validator cannot carry bearer
 * secrets on servers/tables or repurpose destinations on mappings.
 */
Datum
lakehouse_fdw_validator(PG_FUNCTION_ARGS)
{
	Oid			catalog = PG_GETARG_OID(1);
	List	   *deflist;
	ListCell   *lc;
	int			home_flag = 0;
	const char *object_kind = "this object type";

	switch (catalog)
	{
		case ForeignDataWrapperRelationId:
			object_kind = "foreign data wrappers";
			break;
		case ForeignServerRelationId:
			home_flag = LOPT_SERVER;
			object_kind = "servers";
			break;
		case UserMappingRelationId:
			home_flag = LOPT_MAPPING;
			object_kind = "user mappings";
			break;
		case ForeignTableRelationId:
			home_flag = LOPT_TABLE;
			object_kind = "foreign tables";
			break;
		default:
			break;
	}

	deflist = untransformRelOptions(PG_GETARG_DATUM(0));
	foreach(lc, deflist)
	{
		DefElem    *def = (DefElem *) lfirst(lc);
		const LakehouseOptSpec *spec = find_lakehouse_option(def->defname);

		if (spec == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", def->defname)));
		if (home_flag == 0 || (spec->homes & home_flag) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("option \"%s\" is not allowed on %s",
							def->defname, object_kind),
					 errdetail("Option \"%s\" belongs on %s.",
							   def->defname,
							   option_home_name(spec->homes))));
	}

	PG_RETURN_VOID();
}

Datum
lakehouse_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *routine = makeNode(FdwRoutine);

	routine->GetForeignRelSize = lakehouse_GetForeignRelSize;
	routine->GetForeignPaths = lakehouse_GetForeignPaths;
	routine->GetForeignPlan = lakehouse_GetForeignPlan;

	/*
	 * Execution-time COUNT successor (spec 16): descriptor-only one-row upper
	 * path; the count is resolved in Begin against the CURRENT snapshot with
	 * delete eligibility re-proved per execution, and the ordinary aggregate
	 * is the fallback. No count is embedded in plans.
	 */
	routine->GetForeignUpperPaths = lakehouse_GetForeignUpperPaths;
	routine->BeginForeignScan = lakehouse_BeginForeignScan;
	routine->IterateForeignScan = lakehouse_IterateForeignScan;
	routine->ReScanForeignScan = lakehouse_ReScanForeignScan;
	routine->EndForeignScan = lakehouse_EndForeignScan;
	routine->ExplainForeignScan = lakehouse_ExplainForeignScan;
	routine->EstimateDSMForeignScan = lakehouse_EstimateDSMForeignScan;
	routine->InitializeDSMForeignScan = lakehouse_InitializeDSMForeignScan;
	routine->ReInitializeDSMForeignScan = lakehouse_InitializeDSMForeignScan;
	routine->InitializeWorkerForeignScan = lakehouse_InitializeWorkerForeignScan;
	routine->ShutdownForeignScan = lakehouse_ShutdownForeignScan;

	/*
	 * ANALYZE support (spec 19 §6.5): metadata relsize + reservoir row
	 * sampling so pg_statistic carries real NDV (family E residual).
	 */
	routine->AnalyzeForeignTable = lakehouse_AnalyzeForeignTable;

	/*
	 * Our reader is process-local (no shared connections), so every
	 * foreign-table scan is parallel safe (slice 17, doc 06 §3). Without
	 * this hook the planner disables parallel paths for foreign tables
	 * entirely (allpaths.c set_rel_size).
	 */
	routine->IsForeignScanParallelSafe = lakehouse_IsForeignScanParallelSafe;

	PG_RETURN_POINTER(routine);
}

/*
 * Resolve the FDW session options (13-review #6 authority model):
 * transport identity from the SERVER, relation identity/pin from the
 * FOREIGN TABLE, credentials from the USER MAPPING of the effective
 * user (invoker semantics: a view reader selects through their own
 * mapping; postgres_fdw precedent). Misplaced options — including a
 * table-level storage endpoint or legacy table-level credentials —
 * are rejected on use, so pre-validator definitions cannot silently
 * attach credentials that were never mapped for this user. The
 * Iceberg namespace is the part before the last '.' of the table
 * option; the table name is the rest (doc 03 §3: last namespace
 * level == Postgres schema).
 */
void
ExtractOptions(Oid foreigntableid, Oid checkAsUser, LakehouseOptions *opts)
{
	ForeignServer *server;
	ForeignTable *table;
	UserMapping *um;
	ListCell   *lc;
	char	   *table_opt = NULL;

	memset(opts, 0, sizeof(*opts));
	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);

	/* server: transport identity only, never secrets */
	foreach(lc, server->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "endpoint") == 0)
			opts->endpoint = defGetString(def);
		else if (strcmp(def->defname, "warehouse") == 0)
			opts->warehouse = defGetString(def);
		else if (strcmp(def->defname, "s3_endpoint") == 0)
			opts->s3_endpoint = defGetString(def);
		else if (strcmp(def->defname, "expected_table_uuid") == 0)
			opts->read_binding.expected_table_uuid = defGetString(def);
		else if (strcmp(def->defname, "storage_root") == 0)
			opts->read_binding.storage_root = defGetString(def);
		else if (strcmp(def->defname, "catalog_prefix") == 0)
			opts->read_binding.catalog_prefix = defGetString(def);
		else if (strcmp(def->defname, "s3_region") == 0)
			opts->read_binding.s3_region = defGetString(def);
		else
			reject_misplaced_option(def->defname, "servers", LOPT_SERVER);
	}

	/* table: identity + snapshot pin, never credentials/destinations */
	foreach(lc, table->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "table") == 0)
			table_opt = defGetString(def);
		else if (strcmp(def->defname, "snapshot_id") == 0)
			opts->snapshot_id = defGetString(def);
		else
			reject_misplaced_option(def->defname, "foreign tables",
									LOPT_TABLE);
	}

	/* Core supplies the view owner where its privileges apply. */
	um = GetUserMapping(OidIsValid(checkAsUser) ? checkAsUser : GetUserId(),
						server->serverid);
	foreach(lc, um->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "token") == 0)
			opts->token = defGetString(def);
		else if (strcmp(def->defname, "s3_access_key") == 0)
			opts->s3_access_key = defGetString(def);
		else if (strcmp(def->defname, "s3_secret_key") == 0)
			opts->s3_secret_key = defGetString(def);
		else
			reject_misplaced_option(def->defname, "user mappings",
									LOPT_MAPPING);
	}

	if (table_opt == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				 errmsg("lakehouse foreign table requires a \"table\" option")));
	{
		/* namespace = everything before the last '.'; table = after it */
		const char *last_dot = strrchr(table_opt, '.');

		if (last_dot != NULL)
		{
			opts->namespace = pnstrdup(table_opt, last_dot - table_opt);
			opts->table = pstrdup(last_dot + 1);
		}
		else
		{
			opts->namespace = pstrdup("");
			opts->table = pstrdup(table_opt);
		}
	}
}

ScanSession *
lakehouse_OpenSession(LakehouseOptions *opts, const char *rte_selector)
{
	char		errbuf[1024];
	ScanSession *session;
	char	   *sel_json;
	PgDatalakeReadStatus status;
	const PgDatalakeReadBinding *binding = NULL;

	if (opts->read_binding.expected_table_uuid != NULL ||
		opts->read_binding.storage_root != NULL ||
		opts->read_binding.catalog_prefix != NULL ||
		opts->read_binding.s3_region != NULL)
		binding = &opts->read_binding;

	/*
	 * Selector priority (slice 19): the query-level FOR ... AS OF pin wins
	 * over the table's snapshot_id option (a default), which falls back to
	 * the current snapshot.
	 */
	if (rte_selector != NULL && rte_selector[0] != '\0')
		sel_json = lakehouse_BuildSelectorJson(rte_selector);
	else if (opts->snapshot_id != NULL)
		sel_json = psprintf("{\"kind\":\"id\",\"value\":" INT64_FORMAT "}",
							pg_strtoint64(opts->snapshot_id));
	else
		sel_json = "";

	errbuf[0] = '\0';
	session = pg_datalake_scan_new_bound(opts->endpoint,
										 opts->warehouse,
										 opts->token ? opts->token : "",
										 opts->namespace,
										 opts->table,
										 opts->s3_endpoint,
										 opts->s3_access_key,
										 opts->s3_secret_key,
										 opts->predicate_json ? opts->predicate_json : "",
										 sel_json,
										 binding,
										 &status,
										 errbuf,
										 sizeof(errbuf));
	if (session == NULL)
	{
		if (binding != NULL)
		{
			int			sqlstate = ERRCODE_CONNECTION_EXCEPTION;

			switch (status)
			{
				case ReadDenied:
					sqlstate = ERRCODE_INSUFFICIENT_PRIVILEGE;
					break;
				case ReadChangedBinding:
					sqlstate = ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE;
					break;
				case ReadPinUnavailable:
					sqlstate = ERRCODE_INVALID_PARAMETER_VALUE;
					break;
				case ReadUnsupported:
					sqlstate = ERRCODE_FEATURE_NOT_SUPPORTED;
					break;
				default:
					break;
			}
			ereport(ERROR,
					(errcode(sqlstate),
					 errmsg("lakehouse: could not resolve bound scan: %s", errbuf)));
		}
		if (sel_json[0] != '\0')
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("lakehouse: could not plan scan with selector %s: %s",
							sel_json, errbuf)));
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg("lakehouse: could not plan scan: %s", errbuf)));
	}
	return session;
}

/*
 * Canonical RTE selector string (RTE.lakehouse_snapshot, slice 19:
 * "ts:<epoch-ms>" / "id:<snapshot-id>" / "ref:<name>") -> selector JSON for
 * the FFI. The C-side canonical form is validated at parse-analysis time
 * (parse_clause.c), so the value parts are known-safe here.
 */
char *
lakehouse_BuildSelectorJson(const char *sel)
{
	StringInfoData buf;
	const char *p;

	Assert(sel != NULL && sel[0] != '\0');
	initStringInfo(&buf);
	if (strncmp(sel, "ts:", 3) == 0)
		appendStringInfo(&buf, "{\"kind\":\"ts\",\"value-ms\":%s}", sel + 3);
	else if (strncmp(sel, "id:", 3) == 0)
		appendStringInfo(&buf, "{\"kind\":\"id\",\"value\":%s}", sel + 3);
	else if (strncmp(sel, "ref:", 4) == 0)
	{
		appendStringInfoString(&buf, "{\"kind\":\"ref\",\"name\":\"");
		for (p = sel + 4; *p != '\0'; p++)
		{
			if (*p == '"' || *p == '\\')
				appendStringInfoChar(&buf, '\\');
			appendStringInfoChar(&buf, *p);
		}
		appendStringInfoString(&buf, "\"}");
	}
	else
		elog(ERROR, "unrecognized lakehouse snapshot selector \"%s\"", sel);
	return buf.data;
}
