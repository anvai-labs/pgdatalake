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
#include "lib/stringinfo.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "optimizer/optimizer.h"
#include "utils/fmgroids.h"
#include "utils/builtins.h"
#include "utils/rel.h"

#include "pg_datalake_core.h"

#include "lakehouse.h"

/* LakehouseOptions moved to lakehouse.h (M2 slice 26): the table access
 * method shares the option block and the session pipeline. */

/* shared task-claim counter for parallel scans (slice 17) */
typedef struct LakehouseShared
{
	pg_atomic_uint64 next_task;
	/*
	 * The LEADER's pinned snapshot id, written before workers launch
	 * (13-review #7 stable-task-mapping contract): workers plan THIS
	 * snapshot, so a concurrent commit can never make task index N mean
	 * different files in different processes. 0 = not pinned (serial).
	 */
	uint64		snapshot_id;
}			LakehouseShared;

/*
 * Option homes (13-review #6, doc 09 authority model): each option has
 * exactly one legal home. Transport identity lives on the SERVER,
 * secrets on the USER MAPPING (per-user, ACL-controlled, one place to
 * rotate), relation identity/pin on the FOREIGN TABLE. The validator
 * enforces this at DDL; ExtractOptions enforces it again on use, so a
 * legacy pre-validator definition errors instead of silently using
 * credentials that were never authorized through a mapping.
 */
#define LOPT_SERVER		(1 << 0)
#define LOPT_MAPPING	(1 << 1)
#define LOPT_TABLE		(1 << 2)

typedef struct LakehouseOptSpec
{
	const char *name;
	int			homes;
}			LakehouseOptSpec;

static const LakehouseOptSpec lakehouse_opt_specs[] = {
	{"endpoint", LOPT_SERVER},
	{"warehouse", LOPT_SERVER},
	{"s3_endpoint", LOPT_SERVER},
	{"token", LOPT_MAPPING},
	{"s3_access_key", LOPT_MAPPING},
	{"s3_secret_key", LOPT_MAPPING},
	{"table", LOPT_TABLE},
	{"snapshot_id", LOPT_TABLE},
};

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
	}
	return "the lakehouse wrapper's supported objects";
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

/* per-executor-node state */
typedef struct LakehouseExecState
{
	LakehouseOptions opts;
	char	   *predicate_json; /* translated quals (slice 15) */
	char	   *rte_selector;	/* time travel pin from fdw_private (slice 19) */
	bool		parallel;		/* running as a parallel worker (slice 17) */
	ScanSession *session;
	RowsHandle *rows;
	uint64		current_task;
	uint64		task_count;
	char	   *projection_csv; /* visible attribute names, comma-joined */
	/*
	 * THIS node's parallel coordination (13-review #7): per-executor-node,
	 * never a process global — two scans in one query (self-join) must
	 * not claim from or clear each other's queues.
	 */
	LakehouseShared *shared;
	int			natts;
	FmgrInfo   *in_funcs;		/* per visible attribute */
	Oid		   *typioparams;
	int		   *slot_index;		/* attribute slot per projection column */
}			LakehouseExecState;

static void lakehouse_GetForeignRelSize(PlannerInfo *root,
										RelOptInfo *baserel,
										Oid foreigntableid);
static void lakehouse_GetForeignPaths(PlannerInfo *root,
									  RelOptInfo *baserel,
									  Oid foreigntableid);
static ForeignScan *lakehouse_GetForeignPlan(PlannerInfo *root,
											 RelOptInfo *baserel,
											 Oid foreigntableid,
											 ForeignPath *best_path,
											 List *tlist,
											 List *scan_clauses,
											 Plan *outer_plan);
static void lakehouse_BeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *lakehouse_IterateForeignScan(ForeignScanState *node);
static void lakehouse_ReScanForeignScan(ForeignScanState *node);
static void lakehouse_EndForeignScan(ForeignScanState *node);
static Size lakehouse_EstimateDSMForeignScan(ForeignScanState *node,
											 ParallelContext *pcxt);
static void lakehouse_InitializeDSMForeignScan(ForeignScanState *node,
											   ParallelContext *pcxt,
											   void *coordinate);
static void lakehouse_InitializeWorkerForeignScan(ForeignScanState *node,
												  shm_toc *toc,
												  void *coordinate);
static void lakehouse_ShutdownForeignScan(ForeignScanState *node);
static bool lakehouse_IsForeignScanParallelSafe(PlannerInfo *root,
												RelOptInfo *baserel, RangeTblEntry *rte);
static void lakehouse_ExplainForeignScan(ForeignScanState *node,
										 ExplainState *es);

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
static void
ExtractOptions(Oid foreigntableid, LakehouseOptions * opts)
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

	/* credentials: the user mapping for the effective (invoking) user */
	um = GetUserMapping(GetUserId(), server->serverid);
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
lakehouse_OpenSession(LakehouseOptions * opts, const char *rte_selector)
{
	char		errbuf[1024];
	ScanSession *session;
	char	   *sel_json;

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
	session = pg_datalake_scan_new(opts->endpoint,
								   opts->warehouse,
								   opts->token ? opts->token : "",
								   opts->namespace,
								   opts->table,
								   opts->s3_endpoint,
								   opts->s3_access_key,
								   opts->s3_secret_key,
								   opts->predicate_json ? opts->predicate_json : "",
								   sel_json,
								   errbuf,
								   sizeof(errbuf));
	if (session == NULL)
	{
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

static void
lakehouse_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
							Oid foreigntableid)
{
	LakehouseOptions opts;
	ScanSession *session;
	RangeTblEntry *rte;
	uint64		task_count;
	double		total_rows = 0;

	ExtractOptions(foreigntableid, &opts);
	/* query-level time travel pin (slice 19) */
	rte = root->simple_rte_array[baserel->relid];
	session = lakehouse_OpenSession(&opts, rte->lakehouse_snapshot);
	task_count = pg_datalake_scan_task_count(session);
	for (uint64 i = 0; i < task_count; i++)
		total_rows += (double) pg_datalake_scan_task_rows(session, i);
	pg_datalake_scan_free(session);

	/* estimates; actual rows are known only after execution */
	baserel->rows = clamp_row_est(total_rows);
	baserel->pages = clamp_row_est(total_rows / 200.0 + 10);
}

static void
lakehouse_GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
						  Oid foreigntableid)
{
	ForeignPath *path;

	/*
	 * One simple scan path; no pushdown yet (quals evaluated locally by the
	 * executor — doc 06 §4 pushdown lands as its own slice).
	 */
	path = create_foreignscan_path(root, baserel, NULL,
								   baserel->rows, 0,
								   0.0,
								   10.0 * baserel->pages +
								   10.0 * baserel->rows * cpu_operator_cost,
								   NIL, NULL, NULL, NIL, NIL);
	add_path(baserel, &path->path);

	/*
	 * Partial path for parallel scans (slice 17, doc 06 §3): tasks are
	 * claimed atomically from THIS node's shared queue, and every worker
	 * plans the leader's PINNED snapshot (13-review #7), so the task
	 * mapping is stable. Gated on consider_parallel per the path
	 * invariant (PostgreSQL asserts partial paths only on parallel-safe
	 * rels — 13-review #7).
	 */
	if (!baserel->consider_parallel)
		return;
	{
	ForeignPath *partial;

	partial = create_foreignscan_path(root, baserel, NULL,
									  baserel->rows, 0,
									  0.0,
									  (10.0 * baserel->pages +
									   10.0 * baserel->rows * cpu_operator_cost) / 2.0,
									  NIL, NULL, NULL, NIL, NIL);
	partial->path.parallel_workers = 2;
	/* parallel_aware: workers get the DSM hook and claim tasks atomically */
	partial->path.parallel_aware = true;
	add_partial_path(baserel, &partial->path);
	}
}

/*
 * 13-review #4: pushdown eligibility is decided by OPERATOR IDENTITY,
 * not spelling. get_opname() alone accepts a user operator that merely
 * shares the name "=" / "<" but has different semantics — the pushed
 * byte-order pruning then eliminates files whose rows the operator
 * WOULD have matched, and local rechecks cannot recover them. Require
 * the operator to live in pg_catalog.
 */
static bool
operator_is_builtin(Oid opno)
{
	HeapTuple	tp;
	Form_pg_operator opform;
	bool		ok;

	tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
	if (!HeapTupleIsValid(tp))
		return false;
	opform = (Form_pg_operator) GETSTRUCT(tp);
	ok = opform->oprnamespace == PG_CATALOG_NAMESPACE;
	ReleaseSysCache(tp);
	return ok;
}

/*
 * Non-deterministic collations (case/accent-insensitive) make PG's text
 * equality broader than byte equality — pushing `name = 'APPLE'` as a
 * byte-eq prunes files that DO contain matches under the collation
 * (live-verified: ground truth 1 row, returned 0). Deterministic
 * collations define equality as byte equality, so eq pushdown is safe.
 */
static bool
collation_is_deterministic(Oid collid)
{
	HeapTuple	tp;
	Form_pg_collation collform;
	bool		det;

	if (collid == InvalidOid || collid == DEFAULT_COLLATION_OID)
		return true;			/* implicit / deterministic database default */
	tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collid));
	if (!HeapTupleIsValid(tp))
		return false;
	collform = (Form_pg_collation) GETSTRUCT(tp);
	det = collform->collisdeterministic;
	ReleaseSysCache(tp);
	return det;
}

/*
 * Ordering (lt/le/gt/ge) pushdown compares byte order on the Rust side
 * (spec-wise deterministic for Iceberg strings). Only the collations
 * DEFINED as byte order (C) may push — a locale-ordered column
 * (en-US-x-icu) has rows where 'apple' < 'B' holds locally while every
 * byte comparison says otherwise (live-verified: ground truth 2 rows,
 * returned 0).
 */
static bool
collation_is_byte_ordered(Oid collid)
{
	return collid == C_COLLATION_OID || collid == InvalidOid;
}

/*
 * JSON string encoding with a growable buffer (13-review #4): escape
 * quotes, backslashes AND the control characters a JSON parser rejects
 * (raw 0x00-0x1F in a value string previously failed the Rust
 * serde decode outright). Used for field names and text values alike.
 */
static void
append_json_string(StringInfo buf, const char *s)
{
	const unsigned char *p;

	appendStringInfoCharMacro(buf, '"');
	for (p = (const unsigned char *) s; *p; p++)
	{
		switch (*p)
		{
			case '"':
				appendStringInfoString(buf, "\\\"");
				break;
			case '\\':
				appendStringInfoString(buf, "\\\\");
				break;
			case '\b':
				appendStringInfoString(buf, "\\b");
				break;
			case '\f':
				appendStringInfoString(buf, "\\f");
				break;
			case '\n':
				appendStringInfoString(buf, "\\n");
				break;
			case '\r':
				appendStringInfoString(buf, "\\r");
				break;
			case '\t':
				appendStringInfoString(buf, "\\t");
				break;
			default:
				if (*p < 0x20)
					appendStringInfo(buf, "\\u%04x", *p);
				else
					appendStringInfoCharMacro(buf, *p);
		}
	}
	appendStringInfoCharMacro(buf, '"');
}

/*
 * Translate one Var-op-Const qual clause into the ScanPredicate JSON the
 * Rust side understands (slice 15). Returns NULL for anything we cannot
 * translate — those quals stay local (executor-evaluated, correct but
 * not pruning). Numeric semantics: int/date/timestamp constants pass as
 * their binary width (micros for timestamps — matches the parquet stats
 * bounds, doc 05 §4); float passes through. 13-review #4: text types
 * push only under the collation rules above; ALL other types (whose
 * output functions are arbitrary user code and whose semantics the
 * byte-order pruner cannot model) stay local — pushdown eligibility
 * must not call user-defined output functions.
 */
static char *
TranslateQual(Expr *expr, TupleDesc tupdesc)
{
	OpExpr	   *op;
	Var		   *var = NULL;
	Const	   *con = NULL;
	Oid			opno;
	const char *opname;
	const char *json_op = NULL;
	char	   *attname;
	char	   *value_buf = NULL;
	Node	   *a1;
	Node	   *a2;

	if (!IsA(expr, OpExpr))
		return NULL;
	op = (OpExpr *) expr;
	if (list_length(op->args) != 2)
		return NULL;

	opno = op->opno;
	opname = get_opname(opno);
	if (opname == NULL)
		return NULL;
	/* spelling alone is not semantics (13-review #4): same-named user
	 * operators must stay local */
	if (!operator_is_builtin(opno))
		return NULL;
	if (strcmp(opname, "=") == 0)
		json_op = "eq";
	else if (strcmp(opname, "<") == 0)
		json_op = "lt";
	else if (strcmp(opname, "<=") == 0)
		json_op = "le";
	else if (strcmp(opname, ">") == 0)
		json_op = "gt";
	else if (strcmp(opname, ">=") == 0)
		json_op = "ge";
	else
		return NULL;

	a1 = (Node *) linitial(op->args);
	a2 = (Node *) lsecond(op->args);
	if (IsA(a1, Var) && IsA(a2, Const))
	{
		var = (Var *) a1;
		con = (Const *) a2;
	}
	else if (IsA(a2, Var) && IsA(a1, Const))
	{
		/* flipped comparison: invert the operator */
		var = (Var *) a2;
		con = (Const *) a1;
		if (strcmp(opname, "<") == 0)
			json_op = "gt";
		else if (strcmp(opname, "<=") == 0)
			json_op = "ge";
		else if (strcmp(opname, ">") == 0)
			json_op = "lt";
		else if (strcmp(opname, ">=") == 0)
			json_op = "le";
	}
	else
		return NULL;

	if (con->constisnull)
		return NULL;			/* NULL comparison: keep local */
	if (var->varattno <= 0 || var->varattno > tupdesc->natts)
		return NULL;
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, var->varattno - 1);

		if (attr->attisdropped)
			return NULL;
		attname = pstrdup(NameStr(attr->attname));
	}

	{
		Oid			typid = exprType((Node *) con);

		switch (typid)
		{
			case INT2OID:
				value_buf = psprintf("%lld",
									 (long long) DatumGetInt16(con->constvalue));
				break;
			case INT4OID:
				value_buf = psprintf("%lld",
									 (long long) DatumGetInt32(con->constvalue));
				break;
			case DATEOID:
				/* PG date = days since 2000-01-01; Iceberg = days since
				 * 1970-01-01. Pushing the raw datum compared every date
				 * predicate against bounds 10957 days off. */
				value_buf = psprintf("%lld",
									 (long long) (DatumGetInt32(con->constvalue) +
												  (POSTGRES_EPOCH_JDATE -
												   UNIX_EPOCH_JDATE)));
				break;
			case INT8OID:
				value_buf = psprintf("%lld",
									 (long long) DatumGetInt64(con->constvalue));
				break;
			case TIMESTAMPOID:
			case TIMESTAMPTZOID:
				/* PG micros since 2000 -> Iceberg micros since 1970 */
				value_buf = psprintf("%lld",
									 (long long) (DatumGetInt64(con->constvalue) +
												  946684800000000LL));
				break;
			case FLOAT4OID:
			case FLOAT8OID:
				{
					char	   *out = DatumGetCString(typid == FLOAT4OID
						? DirectFunctionCall1(float4out, con->constvalue)
						: DirectFunctionCall1(float8out, con->constvalue));

					/* out funcs give shortest round-trip decimal (%g
					 * loses digits and mis-pruned near-bound files);
					 * NaN/Infinity are not JSON numbers -> no pushdown
					 * (the local filter still applies) */
					if (strstr(out, "NaN") || strstr(out, "Inf"))
						return NULL;
					value_buf = out;
					break;
				}
			case TEXTOID:
			case VARCHAROID:
			case NAMEOID:
				/* bpchar is deliberately absent: PG's bpchar equality
				 * ignores trailing blanks, byte equality does not. */
				{
					Oid			outfn;
					bool		isvarlen;
					const char *raw;
					bool		ordering;
					bool		coll_ok;
					StringInfoData buf;

					ordering = strcmp(json_op, "eq") != 0;
					if (ordering)
						coll_ok = collation_is_byte_ordered(var->varcollid);
					else
						coll_ok = collation_is_deterministic(var->varcollid);
					if (!coll_ok)
						return NULL;	/* stay local: collation semantics
										 * are not byte semantics */

					getTypeOutputInfo(typid, &outfn, &isvarlen);
					raw = OidOutputFunctionCall(outfn, con->constvalue);
					initStringInfo(&buf);
					append_json_string(&buf, raw);
					value_buf = buf.data;
					break;
				}
			default:
				/* 13-review #4: unknown / user-defined types stay local —
				 * their output functions are arbitrary code, and the
				 * byte-order pruner cannot model their comparison
				 * semantics */
				return NULL;
		}
	}

	{
		StringInfoData buf;

		initStringInfo(&buf);
		append_json_string(&buf, attname);
		return psprintf("{\"op\":\"%s\",\"field\":%s,\"value\":%s}",
						json_op, buf.data, value_buf);
	}
}

static ForeignScan *
lakehouse_GetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,
						 Oid foreigntableid, ForeignPath *best_path,
						 List *tlist, List *scan_clauses, Plan *outer_plan)
{
	TupleDesc	tupdesc;
	ListCell   *lc;
	StringInfoData pred;
	List	   *local_quals = NIL;
	char	   *pred_json = NULL;
	bool		first_pred = true;
	Relation	tmpl_rel;

	/*
	 * Translate Var-op-Const quals into the ScanPredicate JSON (slice 15):
	 * translated clauses are dropped from the local list — the Rust side
	 * prunes whole FILES conservatively (bounds/partitions), while any
	 * remaining clause is evaluated per-row by the executor. Row-level
	 * correctness holds because pruning is strictly conservative.
	 */
	tmpl_rel = table_open(foreigntableid, AccessShareLock);
	tupdesc = CreateTupleDescCopy(RelationGetDescr(tmpl_rel));
	table_close(tmpl_rel, AccessShareLock);

	/* unwrap RestrictInfos first: TranslateQual wants plain Exprs */
	scan_clauses = extract_actual_clauses(scan_clauses, false);
	initStringInfo(&pred);
	foreach(lc, scan_clauses)
	{
		Expr	   *expr = (Expr *) lfirst(lc);
		char	   *clause = TranslateQual(expr, tupdesc);

		if (clause != NULL)
		{
			if (!first_pred)
				appendStringInfoChar(&pred, ',');
			appendStringInfoString(&pred, clause);
			first_pred = false;
		}
		else
			local_quals = lappend(local_quals, expr);
	}
	pfree(tupdesc);

	if (!first_pred)
		pred_json = psprintf("{\"and\":[%s]}", pred.data);

	/*
	 * ALL actual clauses stay in qpqual for exact per-row evaluation:
	 * bounds/partition pruning is conservative (file-level), so pushed quals
	 * must ALSO be enforced locally for exact results (doc 06 s4).
	 */
	{
		char	   *pred_private = pred_json ? pstrdup(pred_json) : pstrdup("");
		RangeTblEntry *rte = root->simple_rte_array[baserel->relid];
		char	   *sel_private;

		/*
		 * slice 19: the query-level time travel pin rides in fdw_private
		 * (plan -> executor) as the second element; empty string = none.
		 */
		sel_private = rte->lakehouse_snapshot
			? pstrdup(rte->lakehouse_snapshot)
			: pstrdup("");

		/*
		 * qpqual = ALL clauses: pruning is file-level conservative, the
		 * executor enforces the exact per-row filter on top (doc 06 s4)
		 */
		return make_foreignscan(tlist, scan_clauses, baserel->relid,
								NIL, list_make2(makeString(pred_private),
												makeString(sel_private)),
								NIL, NIL, outer_plan);
	}
}

static void
lakehouse_BeginForeignScan(ForeignScanState *node, int eflags)
{
	Relation	rel = node->ss.ss_currentRelation;
	LakehouseOptions opts;
	LakehouseExecState *state;
	TupleDesc	tupdesc = RelationGetDescr(rel);
	StringInfoData proj;
	bool		first_col = true;

	ExtractOptions(RelationGetRelid(rel), &opts);

	state = (LakehouseExecState *) palloc0(sizeof(LakehouseExecState));
	state->natts = tupdesc->natts;
	state->in_funcs = (FmgrInfo *) palloc0(tupdesc->natts * sizeof(FmgrInfo));
	state->typioparams = (Oid *) palloc0(tupdesc->natts * sizeof(Oid));
	state->slot_index = (int *) palloc0(tupdesc->natts * sizeof(int));

	/* projection: visible attribute names, in attribute order */
	initStringInfo(&proj);
	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		if (attr->attisdropped)
		{
			state->slot_index[i] = -1;
			continue;
		}
		if (!first_col)
			appendStringInfoChar(&proj, ',');
		appendStringInfoString(&proj, NameStr(attr->attname));
		first_col = false;
		getTypeInputInfo(attr->atttypid,
						 &state->in_funcs[i].fn_oid,
						 &state->typioparams[i]);
		fmgr_info(state->in_funcs[i].fn_oid, &state->in_funcs[i]);
	}
	/* renumber projection columns (dropped slots don't occupy positions) */
	{
		int			proj_col = 0;

		for (int i = 0; i < tupdesc->natts; i++)
			if (state->slot_index[i] >= 0)
				state->slot_index[i] = proj_col++;
	}
	state->projection_csv = proj.data;

	/*
	 * slice 15/19: translated-quals JSON + time travel selector ride in
	 * fdw_private (plan -> executor) as makeString elements.
	 */
	state->parallel = IsParallelWorker();
	if (node->ss.ps.plan != NIL)
	{
		ForeignScan *fscan = (ForeignScan *) node->ss.ps.plan;

		if (fscan->fdw_private != NIL)
		{
			char	   *pred_private = strVal(linitial(fscan->fdw_private));

			state->predicate_json = pred_private[0] != '\0'
				? pstrdup(pred_private)
				: NULL;
			if (list_length(fscan->fdw_private) >= 2)
			{
				char	   *sel_private = strVal(lsecond(fscan->fdw_private));

				state->rte_selector = sel_private[0] != '\0'
					? pstrdup(sel_private)
					: NULL;
			}
		}
	}
	state->opts = opts;
	state->opts.endpoint = opts.endpoint ? pstrdup(opts.endpoint) : NULL;
	state->opts.warehouse = opts.warehouse ? pstrdup(opts.warehouse) : NULL;
	state->opts.token = opts.token ? pstrdup(opts.token) : NULL;
	state->opts.namespace = pstrdup(opts.namespace);
	state->opts.table = pstrdup(opts.table);
	state->opts.s3_endpoint = opts.s3_endpoint ? pstrdup(opts.s3_endpoint) : NULL;
	state->opts.s3_access_key = opts.s3_access_key ? pstrdup(opts.s3_access_key) : NULL;
	state->opts.s3_secret_key = opts.s3_secret_key ? pstrdup(opts.s3_secret_key) : NULL;

	/* predicate_json must be set on opts BEFORE OpenSession (slice 15) */
	if (state->predicate_json != NULL)
		state->opts.predicate_json = state->predicate_json;
	state->session = lakehouse_OpenSession(&state->opts, state->rte_selector);
	state->task_count = pg_datalake_scan_task_count(state->session);
	state->rows = NULL;
	state->current_task = 0;

	node->fdw_state = state;
}

static TupleTableSlot *
lakehouse_IterateForeignScan(ForeignScanState *node)
{
	LakehouseExecState *state = (LakehouseExecState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	TupleDesc	tupdesc = node->ss.ss_currentRelation->rd_att;

	for (;;)
	{
		if (state->rows == NULL)
		{
			char		errbuf[1024];
			const char *file;

			if (state->shared != NULL)
			{
				/* parallel: atomically claim the next task (slice 17) */
				uint64		claimed =
					pg_atomic_fetch_add_u64(&state->shared->next_task,
											1);

				if (claimed >= state->task_count)
					return ExecClearTuple(slot);
				state->current_task = claimed;
			}
			else if (state->current_task >= state->task_count)
				return ExecClearTuple(slot);

			errbuf[0] = '\0';
			file = pg_datalake_scan_task_file(state->session,
											  state->current_task);
			state->rows = pg_datalake_rows_open(state->session,
												state->current_task,
												state->projection_csv,
												errbuf,
												sizeof(errbuf));
			if (state->rows == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_IO_ERROR),
						 errmsg("lakehouse: could not open task %llu (%s): %s",
								(unsigned long long) state->current_task,
								file, errbuf)));
		}

		if (pg_datalake_rows_next(state->rows))
		{
			Datum	   *values = (Datum *) palloc(tupdesc->natts * sizeof(Datum));
			bool	   *nulls = (bool *) palloc(tupdesc->natts * sizeof(bool));
			HeapTuple	tuple;

			for (int i = 0; i < tupdesc->natts; i++)
			{
				int			pcol = state->slot_index[i];

				if (pcol < 0)
				{
					values[i] = PointerGetDatum(NULL);
					nulls[i] = true;
					continue;
				}
				if (pg_datalake_rows_col_null(state->rows, pcol))
				{
					values[i] = PointerGetDatum(NULL);
					nulls[i] = true;
					continue;
				}
				values[i] = InputFunctionCall(&state->in_funcs[i],
											  pg_datalake_rows_col_text(state->rows,
																		pcol),
											  state->typioparams[i], -1);
				nulls[i] = false;
			}
			tuple = heap_form_tuple(tupdesc, values, nulls);
			pfree(values);
			pfree(nulls);
			return ExecStoreHeapTuple(tuple, slot, false);
		}

		/* task exhausted: advance to the next one */
		pg_datalake_rows_close(state->rows);
		state->rows = NULL;
		state->current_task++;
	}
}

/*
 * EXPLAIN visibility (00-goals #3, doc 01 §4): show what the scan will
 * actually do — catalog endpoint, table identity, task count, rows.
 */
static void
lakehouse_ExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	LakehouseExecState *state = (LakehouseExecState *) node->fdw_state;
	uint64		rows = 0;

	if (state == NULL)
		return;
	for (uint64 i = 0; i < state->task_count; i++)
		rows += (uint64) pg_datalake_scan_task_rows(state->session, i);

	ExplainPropertyText("Lakehouse table", state->opts.namespace[0]
						? psprintf("%s.%s", state->opts.namespace,
								   state->opts.table)
						: state->opts.table, es);
	ExplainPropertyText("Lakehouse endpoint", state->opts.endpoint, es);
	{
		uint64		before =
			pg_datalake_scan_files_before_pruning(state->session);
		uint64		after = pg_datalake_scan_files_after_pruning(state->session);

		ExplainPropertyInteger("Iceberg files examined", NULL,
							   (int64) before, es);
		if (after < before)
			ExplainPropertyInteger("Iceberg files pruned", NULL,
								   (int64) (before - after), es);
	}
	ExplainPropertyInteger("Iceberg scan tasks", NULL,
						   (int64) state->task_count, es);
	ExplainPropertyInteger("Iceberg rows (parquet metadata)", NULL,
						   (int64) rows, es);
	ExplainPropertyText("Projection", state->projection_csv[0]
						? state->projection_csv : "(all columns)", es);
	ExplainPropertyText("Pushdown", "projection only; quals evaluated locally (M1)",
						es);
}

static void
lakehouse_ReScanForeignScan(ForeignScanState *node)
{
	LakehouseExecState *state = (LakehouseExecState *) node->fdw_state;

	/*
	 * Rewind only: the planned session is REUSED. Merge/NestLoop join inner
	 * rescans call this many times per statement — a full re-plan here (the
	 * original implementation) costs a catalog round-trip + manifest fetch
	 * PER RESCAN and made join-shaped TPC-DS queries take minutes (caught by
	 * the M1 gate, slice 23). Within one statement the remote snapshot cannot
	 * move underneath us in any visible way: the scan is pinned to the plan's
	 * snapshot at open time.
	 */
	if (state->rows != NULL)
	{
		pg_datalake_rows_close(state->rows);
		state->rows = NULL;
	}
	if (state->session == NULL)
	{
		state->session = lakehouse_OpenSession(&state->opts, state->rte_selector);
		state->task_count = pg_datalake_scan_task_count(state->session);
	}
	state->current_task = 0;
}

static bool
lakehouse_IsForeignScanParallelSafe(PlannerInfo *root, RelOptInfo *baserel,
									RangeTblEntry *rte)
{
	/* process-local reads over object storage: always safe (doc 06 §3) */
	return true;
}

static Size
lakehouse_EstimateDSMForeignScan(ForeignScanState *node, ParallelContext *pcxt)
{
	return sizeof(LakehouseShared);
}

/*
 * Re-open THIS node's session pinned to the leader's concrete snapshot
 * (13-review #7): every process must plan the SAME immutable snapshot,
 * or task index N can mean different files under a concurrent commit.
 * Cheap when ids already match (no commit raced us).
 */
static void
lakehouse_repin_session(LakehouseExecState *state, uint64 snapshot_id)
{
	char		sel[64];

	Assert(state != NULL);
	if (state->session != NULL &&
		(uint64) pg_datalake_scan_snapshot_id(state->session) == snapshot_id)
		return;
	snprintf(sel, sizeof(sel), "{\"kind\":\"id\",\"value\":%llu}",
			 (unsigned long long) snapshot_id);
	if (state->session != NULL)
	{
		pg_datalake_scan_free(state->session);
		state->session = NULL;
	}
	state->rte_selector = pstrdup(sel);
	state->session = lakehouse_OpenSession(&state->opts, state->rte_selector);
	state->task_count = pg_datalake_scan_task_count(state->session);
	state->rows = NULL;
	state->current_task = 0;
}

static void
lakehouse_InitializeDSMForeignScan(ForeignScanState *node,
								   ParallelContext *pcxt, void *coordinate)
{
	LakehouseShared *shared = (LakehouseShared *) coordinate;

	pg_atomic_init_u64(&shared->next_task, 0);
	/*
	 * Begin (per-execution pin — the plancache-fresh boundary) ran before
	 * this hook everywhere: children initialize before the Gather parent.
	 * The leader's session therefore holds the just-resolved snapshot;
	 * record it for the workers, who re-pin in their own Initialize hook
	 * (13-review #7).
	 */
	{
		LakehouseExecState *state = (LakehouseExecState *) node->fdw_state;

		shared->snapshot_id = (state != NULL && state->session != NULL)
			? (uint64) pg_datalake_scan_snapshot_id(state->session)
			: 0;
		if (state != NULL)
		{
			state->shared = shared;
			/* ids already match by construction on the leader */
		}
	}
}

static void
lakehouse_InitializeWorkerForeignScan(ForeignScanState *node, shm_toc *toc,
									  void *coordinate)
{
	LakehouseShared *shared = (LakehouseShared *) coordinate;
	LakehouseExecState *state = (LakehouseExecState *) node->fdw_state;

	/*
	 * Begin already ran here (children init before the Gather parent in
	 * workers too), so fdw_state carries this node's real state: attach
	 * the node's queue and re-pin the session to the leader's snapshot.
	 * (An early state is only a defensive fallback.)
	 */
	if (state == NULL)
	{
		state = (LakehouseExecState *) palloc0(sizeof(LakehouseExecState));
		node->fdw_state = state;
	}
	state->shared = shared;
	if (shared->snapshot_id != 0)
		lakehouse_repin_session(state, shared->snapshot_id);
}

static void
lakehouse_ShutdownForeignScan(ForeignScanState *node)
{
	/* clear THIS node's coordination only (13-review #7) */
	LakehouseExecState *state = (LakehouseExecState *) node->fdw_state;

	if (state != NULL)
		state->shared = NULL;
}

static void
lakehouse_EndForeignScan(ForeignScanState *node)
{
	LakehouseExecState *state = (LakehouseExecState *) node->fdw_state;

	if (state == NULL)
		return;
	if (state->rows != NULL)
	{
		pg_datalake_rows_close(state->rows);
		state->rows = NULL;
	}
	if (state->session != NULL)
	{
		pg_datalake_scan_free(state->session);
		state->session = NULL;
	}
}
