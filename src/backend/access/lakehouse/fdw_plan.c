/*
 * lakehouse FDW — planning hooks (specs/lakehouse/06-execution.md). Split out of fdw.c — see
 * lakehouse_fdw.h for the module map and HANDOFF for the decomposition
 * plan. Include surface deliberately mirrors fdw.c (superset is
 * harmless in C).
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_collation_d.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/explain_format.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_namespace_d.h"
#include "utils/syscache.h"
#include "utils/regproc.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/primnodes.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/paths.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "parser/parse_collate.h"
#include "parser/parsetree.h"
#include "nodes/nodeFuncs.h"
#include "access/sysattr.h"
#include "access/table.h"
#include "utils/typcache.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "optimizer/optimizer.h"
#include "optimizer/cost.h"
#include "utils/json.h"

#include "lakehouse_fdw.h"
#include "batch_fill.h"
#include "pg_datalake_core.h"

/*
 * COUNT(*) elision (spec 15 §6.6) is GATED OFF (spec 16, "COUNT must be
 * computed at execution"): the plan-time record_count Const served stale
 * totals under prepared plans and Begin revalidated the sum without
 * rechecking delete eligibility (a same-total snapshot change with deletes
 * overcounted). The successor is an execution-time one-row upper path that
 * resolves the snapshot and re-proves delete eligibility per execution.
 */


/* The stock package uses the foreign table's snapshot_id option. */
static const char *
query_snapshot_selector(PlannerInfo *root, RelOptInfo *baserel)
{
#ifdef LAKEHOUSE_STOCK_FDW
	return NULL;
#else
	return root->simple_rte_array[baserel->relid]->lakehouse_snapshot;
#endif
}

/*
 * Trivial count(*) over exactly one foreign base relation of ours: the
 * builtin identity (F_COUNT_), single star aggregate, no quals/joins/
 * grouping/having/window/distinct/limit-offset. The shape checks mirror
 * the managed successor (count_exec.c) — keep both in sync.
 */
bool
lakehouse_trivial_count_shape(Query *parse)
{
	TargetEntry *te;
	Aggref	   *agg;

	if (parse->commandType != CMD_SELECT || parse->hasWindowFuncs ||
		parse->hasTargetSRFs || parse->setOperations ||
		parse->groupClause || parse->groupingSets || parse->havingQual ||
		parse->distinctClause || parse->limitOffset != NULL)
		return false;
	if (list_length(parse->targetList) != 1)
		return false;
	te = linitial_node(TargetEntry, parse->targetList);
	if (te->resjunk || !IsA(te->expr, Aggref))
		return false;
	agg = (Aggref *) te->expr;
	if (agg->aggfnoid != F_COUNT_ ||
		agg->aggdistinct != NIL || agg->aggfilter != NULL ||
		agg->aggorder != NIL || agg->args != NIL)
		return false;
	return true;
}

static bool
fdw_count_wanted(PlannerInfo *root, RelOptInfo *input_rel,
				 RangeTblEntry **rte_out)
{
	Query	   *parse = root->parse;
	RangeTblEntry *rte;

	if (!lakehouse_trivial_count_shape(parse))
		return false;
	if (input_rel->rtekind != RTE_RELATION || input_rel->relid == 0)
		return false;
	rte = root->simple_rte_array[input_rel->relid];
	if (rte->rtekind != RTE_RELATION ||
		rte->relkind != RELKIND_FOREIGN_TABLE)
		return false;
	if (input_rel->baserestrictinfo != NIL || input_rel->joininfo != NIL)
		return false;
	if (parse->jointree && parse->jointree->quals != NULL)
		return false;
	if (rte_out != NULL)
		*rte_out = rte;
	return true;
}

/*
 * Offer a descriptor-only path for the single COUNT result. Resolve the
 * authorized snapshot and compute the count on first tuple demand.
 */
void
lakehouse_GetForeignUpperPaths(PlannerInfo *root, UpperRelationKind stage,
							   RelOptInfo *input_rel, RelOptInfo *output_rel,
							   void *extra)
{
	ForeignPath *path;
	ForeignPath *best;
	RangeTblEntry *rte;
	const char *selector;
	PathTarget *target;

	if (stage != UPPERREL_GROUP_AGG)
		return;
	if (!fdw_count_wanted(root, input_rel, &rte))
		return;
	best = (ForeignPath *) input_rel->cheapest_total_path;
	if (best == NULL || !IsA(best, ForeignPath))
		return;

	target = output_rel->reltarget;
	/* the stock selector is NULL: the descriptor carries an empty one */
	selector = query_snapshot_selector(root, input_rel);
	path = create_foreign_upper_path(root, output_rel,
									 target,
									 1 /* rows: exactly one answer */ ,
									 0, 0.0 /* startup */ ,
									 10.0 /* total: manifest sum, cached */ ,
									 NIL, NULL,
									 NIL /* fdw_restrictinfo */ ,
									 list_make3(makeString("count_exec"),
												makeString(selector
														   ? pstrdup(selector)
														   : pstrdup("")),
												makeString(psprintf("%u",
																	(unsigned) rte->relid))));
	add_path(output_rel, &path->path);
}

/*
 * Local catalog statistics are the serving default. Operators can opt into a
 * remote estimate for isolated profiling of an unbound foreign table.
 */
#define PLAN_ROWS_DEFAULT 0
#define PLAN_ROWS_METADATA 1

static int	lakehouse_plan_rows_mode = PLAN_ROWS_DEFAULT;
static bool lakehouse_cache_enabled = true;
bool		lakehouse_rowgroup_pruning = true;
static double lakehouse_plan_rows_scale = 1.0;
static double lakehouse_plan_rows_max = 1e12;

static void
assign_cache_enabled(bool enabled, void *extra)
{
	pg_datalake_core_cache_set_enabled(enabled);
}

void		lakehouse_fdw_plan_guc_init(void);

void
lakehouse_fdw_plan_guc_init(void)
{
	static const struct config_enum_entry plan_rows_modes[] = {
		{"default", PLAN_ROWS_DEFAULT, false},
		{"metadata", PLAN_ROWS_METADATA, false},
		{NULL, 0, false}
	};

	DefineCustomEnumVariable("lakehouse.plan_rows_mode",
							 "Foreign-table row estimates: \"default\" uses local statistics, \"metadata\" enables operator remote estimates.",
							 NULL,
							 &lakehouse_plan_rows_mode,
							 PLAN_ROWS_DEFAULT,
							 plan_rows_modes,
							 PGC_SUSET, 0,
							 NULL, NULL, NULL);
	DefineCustomRealVariable("lakehouse.plan_rows_scale",
							 "Multiplier applied to metadata-derived row estimates (per-profile L1 calibration, spec 19 §6.5).",
							 NULL,
							 &lakehouse_plan_rows_scale,
							 1.0, 0.0001, 1e6,
							 PGC_SUSET, 0,
							 NULL, NULL, NULL);
	DefineCustomBoolVariable("lakehouse.cache_enabled",
							 "Serve immutable-path reads through the per-backend block cache (off = pass-through: cold reads, counters untouched).",
							 NULL,
							 &lakehouse_cache_enabled,
							 true,
							 PGC_USERSET, 0,
							 NULL, assign_cache_enabled, NULL);
	DefineCustomBoolVariable("lakehouse.rowgroup_pruning",
							 "Predicate-based pruning (file bounds/partitions AND row-group stats) when a translated predicate exists; rows are always re-checked locally. Off = no plan-time pruning at all.",
							 NULL,
							 &lakehouse_rowgroup_pruning,
							 true,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);
	DefineCustomRealVariable("lakehouse.plan_rows_max",
							 "Sanity clamp on metadata-derived row estimates.",
							 NULL,
							 &lakehouse_plan_rows_max,
							 1e12, 1, 1e15,
							 PGC_SUSET, 0,
							 NULL, NULL, NULL);
}

static bool
count_aggregate_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Aggref))
	{
		Aggref	   *agg = (Aggref *) node;

		return agg->aggstar;
	}
	return expression_tree_walker(node, count_aggregate_walker, context);
}

/*
 * Is this parse a trivial SELECT count(*) over exactly one relation with
 * no quals? The FDW count-elision upper path sizes that shape as one row
 * itself, and its fixture pins the catalog LOAD count during count
 * fallback (spec 16): a plan-time metadata session would consume the
 * first stale LOAD and shift the observed reload sequence. Skip sizing
 * for that shape — the metadata is dead weight there.
 */
static bool
parse_is_trivial_count(Query *parse)
{
	if (parse->commandType != CMD_SELECT || parse->hasAggs == false)
		return false;
	if (list_length(parse->rtable) != 1 || parse->jointree == NULL ||
		list_length(parse->jointree->fromlist) != 1 ||
		parse->jointree->quals != NULL ||
		parse->groupClause != NIL || parse->havingQual != NULL)
		return false;
	if (!IsA(linitial(parse->jointree->fromlist), RangeTblRef))
		return false;
	if (list_length(parse->targetList) != 1)
		return false;
	return expression_returns_set((Node *) parse->targetList) == false &&
		count_aggregate_walker((Node *) parse->targetList, NULL);
}

/*
 * Collect an operator's estimate without retaining credential options or a
 * relation-only cache across plans. Provider unavailability and a missing
 * mapping can fall back to the local estimate.
 */
static bool
planner_metadata_rows(Oid foreigntableid, const char *selector, int64 *rows_out)
{
	LakehouseOptions opts;
	ScanSession *volatile session = NULL;
	MemoryContext context;
	MemoryContext oldcxt;
	volatile bool available = false;
	int64		rows = 0;

	context = AllocSetContextCreate(CurrentMemoryContext,
									"lakehouse planning options",
									ALLOCSET_SMALL_SIZES);
	oldcxt = MemoryContextSwitchTo(context);
	PG_TRY();
	{
		ExtractOptions(foreigntableid, GetUserId(), &opts);
		if (opts.read_binding.expected_table_uuid == NULL &&
			opts.read_binding.storage_root == NULL &&
			opts.read_binding.catalog_prefix == NULL &&
			opts.read_binding.s3_region == NULL)
		{
			uint64		tasks;

			CHECK_FOR_INTERRUPTS();
			session = lakehouse_OpenSession(&opts, selector);
			CHECK_FOR_INTERRUPTS();
			tasks = pg_datalake_scan_task_count(session);
			available = true;
			for (uint64 i = 0; i < tasks; i++)
			{
				int64		task_rows = pg_datalake_scan_task_rows(session, i);

				CHECK_FOR_INTERRUPTS();
				if (task_rows < 0 || task_rows > PG_INT64_MAX - rows)
				{
					available = false;
					break;
				}
				rows += task_rows;
			}
			pg_datalake_scan_free(session);
			session = NULL;
		}
	}
	PG_CATCH();
	{
		int			sqlstate = geterrcode();

		MemoryContextSwitchTo(oldcxt);
		if (session != NULL)
			pg_datalake_scan_free(session);
		if (sqlstate != ERRCODE_CONNECTION_EXCEPTION &&
			sqlstate != ERRCODE_UNDEFINED_OBJECT)
			PG_RE_THROW();
		FlushErrorState();
		available = false;
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(context);
	if (available)
		*rows_out = rows;
	return available;
}

void
lakehouse_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
							Oid foreigntableid)
{
	/* Serving plans use the PostgreSQL catalog without origin I/O. */
	if (baserel->tuples >= 0)
	{
		set_baserel_size_estimates(root, baserel);
		return;
	}

	if (lakehouse_plan_rows_mode == PLAN_ROWS_METADATA && superuser() &&
		baserel->reloptkind == RELOPT_BASEREL &&
		(!OidIsValid(baserel->userid) || baserel->userid == GetUserId()) &&
		!parse_is_trivial_count(root->parse))
	{
		int64		rows;

		if (planner_metadata_rows(foreigntableid,
								  query_snapshot_selector(root, baserel), &rows) &&
			(double) rows <= lakehouse_plan_rows_max)
		{
			double		scaled = (double) rows * lakehouse_plan_rows_scale;

			/*
			 * tuples drives set_baserel_size_estimates (quals selectivity
			 * applies on top); pages feeds the foreign-scan cost in
			 * GetForeignPaths (~100 rows per page-equivalent)
			 */
			baserel->tuples = scaled;
			baserel->pages = Max(1, (int) Min(scaled / 100.0,
											  (double) INT_MAX));
			set_baserel_size_estimates(root, baserel);
			return;
		}
		/* fall through: fail-safe keeps core's rows=1000 default */
	}

	/*
	 * mode = default, or metadata declined (fetch failed / clamped): leave
	 * core's estimate untouched — rel->rows is already core's rows=1000
	 * default, and baserel->tuples < 0 here (the tuples >= 0 branch returned
	 * above), so there is nothing further to compute.
	 */
}

void
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
	 * plans the leader's PINNED snapshot (13-review #7), so the task mapping
	 * is stable. Gated on consider_parallel per the path invariant
	 * (PostgreSQL asserts partial paths only on parallel-safe rels —
	 * 13-review #7).
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

	/*
	 * spelling alone is not semantics (13-review #4): same-named user
	 * operators must stay local
	 */
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

				/*
				 * PG date = days since 2000-01-01; Iceberg = days since
				 * 1970-01-01. Pushing the raw datum compared every date
				 * predicate against bounds 10957 days off.
				 */
				value_buf = psprintf("%lld",
									 (long long) DatumGetInt32(con->constvalue) +
									 (POSTGRES_EPOCH_JDATE -
									  UNIX_EPOCH_JDATE));
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

					/*
					 * out funcs give shortest round-trip decimal (%g loses
					 * digits and mis-pruned near-bound files); NaN/Infinity
					 * are not JSON numbers -> no pushdown (the local filter
					 * still applies)
					 */
					if (strstr(out, "NaN") || strstr(out, "Inf"))
						return NULL;
					value_buf = out;
					break;
				}
			case TEXTOID:
			case VARCHAROID:
			case NAMEOID:

				/*
				 * bpchar is deliberately absent: PG's bpchar equality ignores
				 * trailing blanks, byte equality does not.
				 */
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
						return NULL;	/* stay local: collation semantics are
										 * not byte semantics */

					getTypeOutputInfo(typid, &outfn, &isvarlen);
					raw = OidOutputFunctionCall(outfn, con->constvalue);
					initStringInfo(&buf);
					append_json_string(&buf, raw);
					value_buf = buf.data;
					break;
				}
			default:

				/*
				 * 13-review #4: unknown / user-defined types stay local —
				 * their output functions are arbitrary code, and the
				 * byte-order pruner cannot model their comparison semantics
				 */
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

ForeignScan *
lakehouse_GetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,
						 Oid foreigntableid, ForeignPath *best_path,
						 List *tlist, List *scan_clauses, Plan *outer_plan)
{
	TupleDesc	tupdesc;
	ListCell   *lc;
	StringInfoData pred;
	Bitmapset  *used_attrs = NULL;
	List	   *projection_attrs = NIL;
	int			member;
	char	   *pred_json = NULL;
	bool		first_pred = true;
	Relation	tmpl_rel;

	/*
	 * Describe the aggregate emitted in the scan slot. setrefs matches the
	 * plan's Aggref to this expression and substitutes an INDEX_VAR.
	 */
	if (baserel->reloptkind == RELOPT_UPPER_REL)
	{
		List	   *scan_tlist = make_tlist_from_pathtarget(best_path->path.pathtarget);

		Assert(list_length(best_path->fdw_private) == 3);
		return make_foreignscan(tlist, NIL, 0, NIL, best_path->fdw_private,
								scan_tlist, NIL, NULL);
	}

	/*
	 * Translate Var-op-Const quals for conservative file pruning. Keep all
	 * clauses in the scan for exact per-row evaluation.
	 */
	tmpl_rel = table_open(foreigntableid, AccessShareLock);
	tupdesc = CreateTupleDescCopy(RelationGetDescr(tmpl_rel));
	table_close(tmpl_rel, AccessShareLock);

	/* unwrap RestrictInfos first: TranslateQual wants plain Exprs */
	scan_clauses = extract_actual_clauses(scan_clauses, false);
	pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid, &used_attrs);
	pull_varattnos((Node *) tlist, baserel->relid, &used_attrs);
	pull_varattnos((Node *) scan_clauses, baserel->relid, &used_attrs);

	/*
	 * A whole-row or system-column reference retains the full tuple. An empty
	 * list also uses the existing row-producing path for scans with no data
	 * columns (for example a COUNT without upper-path elision).
	 */
	member = bms_next_member(used_attrs, -1);
	if (member > -FirstLowInvalidHeapAttributeNumber)
	{
		for (; member >= 0; member = bms_next_member(used_attrs, member))
			projection_attrs = lappend_int(projection_attrs,
										   member + FirstLowInvalidHeapAttributeNumber);
	}
	bms_free(used_attrs);

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
		const char *selector = query_snapshot_selector(root, baserel);

		/* Empty selector lets the foreign table's snapshot_id take effect. */
		return make_foreignscan(tlist, scan_clauses, baserel->relid,
								NIL, list_make3(makeString(pred_private),
												makeString(pstrdup(selector ? selector : "")),
												projection_attrs),
								NIL, NIL, outer_plan);
	}
}
