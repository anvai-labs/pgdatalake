/*-------------------------------------------------------------------------
 * managed_count.c — execution-time COUNT for MANAGED lakehouse tables
 * (spec 16, "COUNT must be computed at execution"; successor of the
 * gated-off plan-time elision count_elision.c).
 *
 * The planner hook rewrites a trivial `SELECT count(*) FROM <managed
 * table>` into `SELECT lakehouse.managed_count('tbl'::regclass)` — a
 * VOLATILE invoker function, so:
 *   - prepared plans are re-evaluated per execution (no stale count;
 *     blocker 1's prepared-COUNT regression cannot recur)
 *   - no count is embedded in the plan: the only plan input is the
 *     relation OID descriptor
 * managed_count() then resolves everything AT EXECUTION:
 *   1. SELECT ACL on the table as the invoking user (denied -> the same
 *      permission error the ordinary scan would raise)
 *   2. decline (to the SPI ordinary aggregate, which runs with full
 *      executor semantics) when anything could make the ledger sum wrong:
 *      RLS in effect, inheritance children, quals (never reach us — the
 *      hook declines), published segments, NULL row_counts
 *   3. otherwise return SUM(row_count) over UNPUBLISHED segments on the
 *     current statement snapshot (MVCC-exact)
 * Declining means executing the ordinary aggregate — never fabricate.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/table.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_proc_d.h"
#include "catalog/pg_type.h"
#include "parser/parse_func.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "nodes/makefuncs.h"
#include "optimizer/planner.h"
#include "tcop/tcopprot.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rls.h"
#include "utils/syscache.h"

#include "lakehouse.h"
#include "lakehouse_tableam.h"

extern Oid	lakehouse_am_oid(void);

static planner_hook_type prev_planner_hook = NULL;

/*
 * RECURSION GUARD: the fallback path runs SELECT count(*) over the very
 * relation the hook rewrites; without this flag the inner query would be
 * rewritten into managed_count() again — for a table whose proof gate
 * PERMANENTLY fails (published segments) that is an infinite loop.
 */
static bool count_exec_in_fallback = false;

static Oid	lh_am_oid_cache = InvalidOid;

static bool
managed_lakehouse_rel(Oid relid)
{
	Relation	rel;
	bool		is_lh;

	if (lh_am_oid_cache == InvalidOid)
		lh_am_oid_cache = lakehouse_am_oid();

	rel = table_open(relid, AccessShareLock);
	is_lh = RelationGetForm(rel)->relam == lh_am_oid_cache;
	table_close(rel, AccessShareLock);
	return is_lh;
}

/*
 * Trivial count(*) over exactly one managed lakehouse base relation, no
 * quals/joins/group/having/window/distinct/limit-offset, the BUILTIN
 * count identity (a same-named user aggregate must NOT be rewritten —
 * it may compute something else entirely), and an RTE with no RLS
 * security quals. On match, returns the base rel's OID.
 */
static bool
managed_count_wanted(Query *parse, Oid *relid_out)
{
	TargetEntry *te;
	Aggref	   *agg;
	RangeTblEntry *rte;

	if (parse->commandType != CMD_SELECT || parse->hasWindowFuncs ||
		parse->hasTargetSRFs || parse->setOperations ||
		parse->groupClause || parse->groupingSets || parse->havingQual ||
		parse->distinctClause || parse->limitOffset != NULL)
		return false;

	/* exactly one target: a plain star-aggregate */
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

	/*
	 * exactly one base relation, no quals: the fromlist holds exactly one
	 * RangeTblRef pointing at it
	 */
	if (list_length(parse->rtable) != 1 || parse->jointree == NULL ||
		list_length(parse->jointree->fromlist) != 1 ||
		parse->jointree->quals != NULL)
		return false;
	if (!IsA(linitial(parse->jointree->fromlist), RangeTblRef))
		return false;

	rte = linitial_node(RangeTblEntry, parse->rtable);
	if (rte->rtekind != RTE_RELATION || rte->relkind != RELKIND_RELATION ||
		rte->relid == InvalidOid || rte->securityQuals != NIL)
		return false;
	if (!managed_lakehouse_rel(rte->relid))
		return false;

	if (relid_out != NULL)
		*relid_out = rte->relid;
	return true;
}

/*
 * OID of lakehouse.managed_count(oid), resolved once per backend from the
 * extension's schema (the extension installs into the `lakehouse` schema,
 * RULES §4).
 */
static Oid
managed_count_func_oid(void)
{
	static Oid	fn_oid = InvalidOid;

	if (fn_oid == InvalidOid)
	{
		/*
		 * Qualified regprocedure parse: schema-independent of search_path
		 * (the extension installs into the fixed `lakehouse` schema, RULES
		 * §4). Invalid when the extension was not upgraded to 1.3 —
		 * planning then stays ordinary (the gate-off default).
		 */
		fn_oid = DatumGetObjectId(DirectFunctionCall1(regprocedurein,
													  CStringGetDatum("lakehouse.managed_count(oid)")));
	}
	return fn_oid;
}


/*
 * Rewrite the parse tree in place into `SELECT lakehouse.managed_count(
 * relid::oid)` — the planner produces a Result whose target is the
 * function call, evaluated per execution. The rtable is cleared here
 * (the executor would otherwise try to open a scan over it); permission
 * and RLS semantics are re-established inside managed_count() itself.
 */
static void
rewrite_to_managed_count(Query *parse, Oid relid)
{
	Oid			fn_oid = managed_count_func_oid();
	FuncExpr   *fn;
	TargetEntry *te = linitial_node(TargetEntry, parse->targetList);

	if (!OidIsValid(fn_oid))
		return;					/* extension not upgraded: normal planning */

	fn = makeFuncExpr(fn_oid, INT8OID,
					  list_make1(makeConst(OIDOID, -1, InvalidOid,
										   sizeof(Oid),
										   ObjectIdGetDatum(relid),
										   false, true)),
					  InvalidOid, InvalidOid, COERCE_EXPLICIT_CALL);

	te->expr = (Expr *) fn;
	parse->hasAggs = false;
	parse->rtable = NIL;
	if (parse->jointree != NULL)
	{
		parse->jointree->fromlist = NIL;
		parse->jointree->quals = NULL;
	}
	parse->distinctClause = NIL;
	parse->groupClause = NIL;
	parse->havingQual = NULL;
	parse->windowClause = NIL;
	parse->sortClause = NIL;
	parse->limitOffset = NULL;
	parse->limitCount = NULL;
}

static PlannedStmt *
lakehouse_count_exec_planner(Query *parse, const char *query_string,
							 int cursorOptions, ParamListInfo boundParams,
							 ExplainState *es)
{
	Oid			relid = InvalidOid;

	elog(DEBUG1, "lakehouse count_exec: hook entered");
	if (!count_exec_in_fallback && managed_count_wanted(parse, &relid))
	{
		elog(DEBUG1, "lakehouse count_exec: rewriting count(*) relid %u",
			 (unsigned) relid);
		rewrite_to_managed_count(parse, relid);
	}

	if (prev_planner_hook != NULL)
		return prev_planner_hook(parse, query_string, cursorOptions,
								 boundParams, es);
	return standard_planner(parse, query_string, cursorOptions, boundParams, es);
}

void
install_lakehouse_count_exec(void)
{
	prev_planner_hook = planner_hook;
	planner_hook = lakehouse_count_exec_planner;
}

/*
 * lakehouse.managed_count(oid) returns bigint — the execution-time COUNT
 * described in the file header. VOLATILE (declared in the extension
 * script): re-evaluated per execution, so prepared plans stay fresh.
 * SECURITY INVOKER: the caller's permissions and RLS apply.
 */
static void count_exec_run_ordinary(Oid relid, int64 *result);

PG_FUNCTION_INFO_V1(lakehouse_managed_count);

Datum
lakehouse_managed_count(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	AclResult	aclresult;
	int64		ord_result;
	int			ret_dummy = 0;
	int			ret;
	int64		result = 0;
	bool		from_ledger = false;

	/*
	 * PERMISSION (the plan carries no rtable, so the executor's
	 * ExecCheckRTPerms never runs): enforce SELECT as the invoking user with
	 * the same error the ordinary scan would raise.
	 */
	aclresult = pg_class_aclcheck(relid, GetUserId(), ACL_SELECT);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_TABLE, get_rel_name(relid));

	/*
	 * The ledger receives staged rows at pre-commit. A backend with write
	 * state for this relation must use the same overlay as a normal scan.
	 * RLS and inherited rows also require the ordinary aggregate.
	 */
	if (lakehouse_find_write_entry(relid) != NULL ||
		check_enable_rls(relid, GetUserId(), false) != RLS_NONE ||
		find_inheritance_children(relid, NoLock) != NIL)
		goto ordinary_fallback;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	ret = SPI_execute(psprintf("SELECT"
							   " count(*) FILTER (WHERE published) AS p,"
							   " count(*) FILTER (WHERE NOT published AND"
							   " row_count IS NULL) AS n,"
							   " COALESCE(sum(row_count) FILTER (WHERE NOT"
							   " published), 0) AS s"
							   " FROM lakehouse.lakehouse_delta_segments"
							   " WHERE relid = %u",
							   (unsigned) relid),
					  true, 0);
	if (ret == SPI_OK_SELECT && SPI_processed == 1)
	{
		bool		dummy;
		int64		published = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
															SPI_tuptable->tupdesc,
															1, &dummy));
		int64		null_counts = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
															  SPI_tuptable->tupdesc,
															  2, &dummy));

		/*
		 * decode counts as INT8 (the gated-off predecessor read the NULL
		 * counter with DatumGetBool — wrong for any count whose low byte is
		 * 0). Any published segment or NULL row_count: the ledger does not
		 * cover the whole table -> ordinary aggregate.
		 */
		if (published == 0 && null_counts == 0)
		{
			result = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
												 SPI_tuptable->tupdesc,
												 3, &dummy));
			from_ledger = true;
		}
	}
	PopActiveSnapshot();
	SPI_finish();

	ereport(DEBUG1,
			(errmsg("lakehouse managed_count relid %u: %s = %lld",
					(unsigned) relid,
					from_ledger ? "ledger" : "fallback",
					(long long) result)));

	if (from_ledger)
		PG_RETURN_INT64(result);

ordinary_fallback:

	/*
	 * Run the ordinary aggregate with the rewrite SUPPRESSED: the SPI count
	 * replans, and without the guard the hook would rewrite it straight back
	 * into managed_count(). Error-safe: an ereport inside the SPI call
	 * longjmps past the reset, so PG_TRY restores the flag.
	 */
	count_exec_in_fallback = true;
	PG_TRY();
	{
		count_exec_run_ordinary(relid, &result);
		count_exec_in_fallback = false;
	}
	PG_CATCH();
	{
		count_exec_in_fallback = false;
		PG_RE_THROW();
	}
	PG_END_TRY();
	PG_RETURN_INT64(result);
}

/*
 * The ordinary aggregate as fallback: SELECT count(*) over the relation,
 * executed via SPI with the invoking user's full executor semantics
 * (permissions re-checked inside, RLS applied, inheritance included).
 */
static void
count_exec_run_ordinary(Oid relid, int64 *result)
{
	int			ret;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	{
		char	   *nsp = get_namespace_name(get_rel_namespace(relid));
		char	   *nm = get_rel_name(relid);

		/* relation dropped between planning and execution: zero */
		if (nsp == NULL || nm == NULL)
		{
			PopActiveSnapshot();
			SPI_finish();
			*result = 0;
			return;
		}
		ret = SPI_execute(psprintf("SELECT count(*) FROM %s",
								   quote_qualified_identifier(nsp, nm)),
						  true, 0);
	}
	if (ret == SPI_OK_SELECT && SPI_processed == 1)
	{
		bool		dummy;

		*result = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
											  SPI_tuptable->tupdesc, 1, &dummy));
	}
	PopActiveSnapshot();
	SPI_finish();
}
