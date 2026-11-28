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
#include "storage/lmgr.h"
#include "utils/memutils.h"
#include "fmgr.h"
#include "utils/guc.h"
#include "utils/typcache.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "parser/parsetree.h"
#include "nodes/nodeFuncs.h"
#include "tcop/tcopprot.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "parser/parse_oper.h"
#include "parser/parse_coerce.h"
#include "utils/rel.h"
#include "utils/rls.h"
#include "utils/syscache.h"

#include "lakehouse.h"
#include "lakehouse_tableam.h"

/* cte_stats_hook.c (spec 19 §6.5 family C/E) */
extern void lakehouse_cte_stats_hook_begin(List *entries,
										   MemoryContext plan_ctx);
extern void lakehouse_cte_stats_hook_end(void);
extern List *collect_cte_ndv_entries(Query *parse);
#include "lakehouse_fdw.h"

extern Oid	lakehouse_am_oid(void);

static planner_hook_type prev_planner_hook = NULL;

/*
 * RECURSION GUARD: the fallback path runs SELECT count(*) over the very
 * relation the hook rewrites; without this flag the inner query would be
 * rewritten into managed_count() again — for a table whose proof gate
 * PERMANENTLY fails (published segments) that is an infinite loop.
 */
static bool count_exec_in_fallback = false;

/*
 * operator gate for the correlated-aggregate pullup rewrite (spec 19 §5
 * remedy registry): ON by default; OFF restores the per-row SubPlan for
 * the q1/q6 shapes so A/B receipts (pullup x relsize, §6.5) can attribute
 * wins and an operator has an escape hatch if a shape misbehaves. This is
 * an OPERATOR gate only — it never participates in admission decisions
 * (the L2 doctrine: estimates and models never enable or disable rewrites).
 */
bool		lakehouse_agg_pullup_enabled = true;

/*
 * q4-pivot gate (spec 19 §5 registry entry 3): fold per-reference
 * discriminator quals of a multiply-referenced CTE into conditional
 * aggregation ( collapsing the 2^k self-join to a single scan + pivot).
 */
bool		lakehouse_q4_pivot_enabled = true;

void
lakehouse_agg_pullup_guc_init(void)
{
	DefineCustomBoolVariable("lakehouse.agg_pullup",
							 "Enable the correlated-aggregate pullup rewrite (q1/q6 shapes).",
							 NULL,
							 &lakehouse_agg_pullup_enabled,
							 true,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);
}

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
managed_count_wanted(Query *parse, Oid *relid_out, bool *bound_out)
{
	RangeTblEntry *rte;

	if (!lakehouse_trivial_count_shape(parse))
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

	/*
	 * Capture-bound relations take the metadata-only cut fast path
	 * (lakehouse.managed_count_cut): the installed cut's two parts are
	 * disjoint by construction - published base rows from Parquet metadata
	 * plus overlay ledger row_count - so the sum is exact without dedup (spec
	 * 03). Planning inside a parallel worker cannot take the latest snapshot,
	 * so there the rewrite is skipped and the executor's bound gate refuses
	 * parallel scans with the ordinary 0A000.
	 */
	if (IsInParallelMode())
		return false;
	*bound_out = lakehouse_rel_is_capture_bound(rte->relid);

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
managed_count_cut_func_oid(void)
{
	static Oid	fn_oid = InvalidOid;

	if (fn_oid == InvalidOid)
	{
		/*
		 * Extensions below 1.20 have no cut COUNT function: regprocedurein
		 * would raise 42883 at planning time for every bound count(*).
		 * Swallow that and keep ordinary planning — the bound gate still
		 * produces the pre-1.20 refusal at execution.
		 */
		MemoryContext ccxt = CurrentMemoryContext;

		PG_TRY();
		{
			fn_oid = DatumGetObjectId(DirectFunctionCall1(regprocedurein,
														  CStringGetDatum("lakehouse.managed_count_cut(regclass)")));
		}
		PG_CATCH();
		{
			MemoryContextSwitchTo(ccxt);
			FlushErrorState();
			fn_oid = InvalidOid;
		}
		PG_END_TRY();
	}
	return fn_oid;
}


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
rewrite_to_managed_count(Query *parse, Oid relid, bool bound)
{
	Oid			fn_oid = bound ? managed_count_cut_func_oid()
		: managed_count_func_oid();
	FuncExpr   *fn;
	TargetEntry *te = linitial_node(TargetEntry, parse->targetList);

	if (!OidIsValid(fn_oid))
		return;					/* extension not upgraded: normal planning */

	fn = makeFuncExpr(fn_oid, INT8OID,
					  list_make1(makeConst(bound ? REGCLASSOID : OIDOID,
										   -1, InvalidOid,
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

/*
 * ---------------------------------------------------------------------------
 * Correlated-aggregate pullup (TPC-DS q1 family)
 * ---------------------------------------------------------------------------
 * Shape: c1.t > (SELECT avg(c2.x) * k FROM cte c2 WHERE c2.key = c1.key)
 * where cte is a CTE of THIS query whose body scans a foreign table,
 * referenced multiply (c1 is a CTE Scan of the same CTE), correlated by
 * btree-equality conjuncts only. The subplan re-scans the materialized
 * CTE tuplestore per outer row — O(outer x ctr). Rewrite: pull the
 * aggregate into a grouped subquery over the same CTE (one grouped
 * pass) joined INNER on the correlation keys; the outer comparison
 * references the joined aggregate column, and its strictness makes
 * INNER equivalent to the SubPlan's NULL-on-no-match semantics exactly.
 *
 * Decline (fail-safe, SubPlan kept): volatile functions, non-equality
 * or multi-level correlation, subselects with group/having/limit/
 * window/DML/TargetSRF, correlated targetlist Vars or sublinks,
 * non-strict outer comparison, OR/NOT-wrapped sublinks, correlated
 * alias off the top-level fromlist, parallel mode. Gate: only CTEs
 * whose body scans a foreign table (the per-row cost is catastrophic
 * there; local heap CTEs keep stock planning).
 */
typedef struct AggPullupContext
{
	char	   *cte_name;		/* referenced CTE name */
	int			outer_rtindex;	/* rtable index of the outer CTE alias (c1) */
	int			inner_rtindex;	/* rtable index inside the subselect (c2) */
	int			new_rtindex;	/* rtable index of the grouped subquery */
	int			agg_resno;		/* resno of the pulled-up aggregate */
	Oid			agg_type;		/* type of the pulled-up aggregate expr */
	int32		agg_typmod;
	Oid			agg_collid;
	List	   *corr_resnos;	/* grouped-subquery resnos of corr cols */
	List	   *corr_inner_attnos;	/* inner attnos per correlated equality */
	List	   *corr_outer_attnos;	/* outer attnos, same order */
	List	   *corr_types;		/* Var type per correlated col */
	List	   *corr_typmods;
	List	   *corr_collids;
	List	   *corr_eqexprs;	/* copies of the original equality OpExprs */
} AggPullupContext;

static bool foreign_rte_walker(Node *node, void *context);
static bool is_corr_equality(OpExpr *op, int outer_rtindex, int inner_rtindex,
							 AttrNumber *outer_attno, Var **inner_var);
static bool match_agg_sublink_shape(SubLink *sl, Query *parse,
									AggPullupContext *ctx);
static void pullup_cte_agg_sublinks(Query *parse);
static bool q4_pivot_apply(Query *parse);
static Node *fix_corr_eq_vars(Node *node, void *context);
static bool set_agg_varno_walker(Node *node, void *context);
static Node *replace_sublink_mutator(Node *node, void *context);

static bool
foreign_rte_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Query))
	{
		Query	   *q = (Query *) node;
		ListCell   *lc;

		foreach(lc, q->rtable)
		{
			RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);

			if (rte->rtekind == RTE_RELATION &&
				rte->relkind == RELKIND_FOREIGN_TABLE)
				return true;
		}
		return query_tree_walker(q, foreign_rte_walker, context, 0);
	}
	return expression_tree_walker(node, foreign_rte_walker, context);
}

typedef struct SublReplaceCtx
{
	SubLink    *target;			/* the matched SubLink node to replace */
	Index		new_rtindex;	/* rtable index of the grouped subquery */
	AttrNumber	agg_resno;		/* grouped subquery's pulled-up agg resno */
	Oid			agg_type;		/* type of the pulled-up aggregate expr */
	int32		agg_typmod;
	Oid			agg_collid;
} SublReplaceCtx;

/*
 * Swap the matched SubLink for a Var over the grouped subquery's
 * pulled-up aggregate column. Does not descend into other sublinks
 * (they stay as deliberate declines) or into subqueries.
 */
static Node *
replace_sublink_mutator(Node *node, void *context)
{
	SublReplaceCtx *rc = (SublReplaceCtx *) context;

	if (node == NULL)
		return NULL;
	if (node == (Node *) rc->target)
		return (Node *) makeVar(rc->new_rtindex, rc->agg_resno,
								rc->agg_type, rc->agg_typmod,
								rc->agg_collid, 0);
	if (IsA(node, SubLink))
		return node;			/* do not descend into other sublinks */
	if (IsA(node, Query))
		return node;			/* do not descend into subqueries */
	return expression_tree_mutator(node, replace_sublink_mutator, context);
}

/*
 * The correlated-EQUALITY pattern: one side an OUTER Var (level 1 on
 * the outer CTE alias), the other an INNER Var (level 0 on the
 * subselect's own CTE alias), joined by the type's btree equality
 * operator. Inequality, disequality or any other boolean operator
 * changes the grouped-subquery semantics (union-of-partitions vs
 * single-partition averages, row fan-out) and declines. Fills the
 * outer attribute number and the inner Var (for its type data) when
 * the OpExpr matches.
 */
static bool
is_corr_equality(OpExpr *op, int outer_rtindex, int inner_rtindex,
				 AttrNumber *outer_attno, Var **inner_var)
{
	Node	   *a;
	Node	   *b;
	Oid			a_type;
	TypeCacheEntry *tce;

	if (!IsA(op, OpExpr) || list_length(op->args) != 2 ||
		op->opresulttype != BOOLOID)
		return false;

	a = linitial(op->args);
	b = lsecond(op->args);

	/*
	 * the operator must be the equality operator of the args' comparison
	 * type: exprType of the pre-unwrap arg (RelabelType reports its own
	 * result type, e.g. text for varchar = varchar resolved to texteq)
	 */
	a_type = exprType(a);
	tce = lookup_type_cache(a_type, TYPECACHE_EQ_OPR);
	if (tce->eq_opr == InvalidOid || op->opno != tce->eq_opr)
		return false;

	/*
	 * look through binary-coercibility relabels (varchar = varchar resolves
	 * to texteq over RelabelType-wrapped Vars); anything deeper (functional,
	 * I/O coercions) is not the plain-equality shape
	 */
	while (a != NULL && IsA(a, RelabelType))
		a = (Node *) ((RelabelType *) a)->arg;
	while (b != NULL && IsA(b, RelabelType))
		b = (Node *) ((RelabelType *) b)->arg;

	if (IsA(a, Var) && ((Var *) a)->varlevelsup == 1 &&
		((Var *) a)->varno == outer_rtindex &&
		IsA(b, Var) && ((Var *) b)->varlevelsup == 0 &&
		((Var *) b)->varno == inner_rtindex)
	{
		*outer_attno = ((Var *) a)->varattno;
		*inner_var = (Var *) b;
		return true;
	}
	if (IsA(b, Var) && ((Var *) b)->varlevelsup == 1 &&
		((Var *) b)->varno == outer_rtindex &&
		IsA(a, Var) && ((Var *) a)->varlevelsup == 0 &&
		((Var *) a)->varno == inner_rtindex)
	{
		*outer_attno = ((Var *) b)->varattno;
		*inner_var = (Var *) a;
		return true;
	}
	return false;
}

/*
 * Subselect shape gate: CMD_SELECT, single RTE_CTE from item resolving
 * to a CTE of this (top-level) query whose body scans a foreign table,
 * no group/having/limit/window/distinct/setops/TargetSRF, no volatile
 * functions, single targetlist entry holding an aggregate with no
 * outer-level Vars and no sublinks inside it, WHERE = conjuncts of
 * correlated equalities only (>= 1).
 */
static bool
decl(int line)
{
	elog(DEBUG1, "agg-pullup: declined at line %d", line);
	return false;
}

/*
 * Decline detector for the subselect targetlist: any Var above the
 * subselect's own level (correlated targetlist — pull_var_clause would
 * ERROR before our decline could fire) or any SubLink (nested scalar
 * subqueries change the aggregated value's independence). We return
 * true AT the SubLink node, so its subselect is never entered.
 */
static bool
tle_has_outer_var_or_sublink_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
		return ((Var *) node)->varlevelsup != 0;
	if (IsA(node, SubLink))
		return true;
	if (IsA(node, Query))
		return false;			/* do not descend into subqueries */
	return expression_tree_walker(node, tle_has_outer_var_or_sublink_walker,
								  context);
}

static bool
match_agg_sublink_shape(SubLink *sl, Query *parse, AggPullupContext *ctx)
{
	Query	   *sq;
	RangeTblRef *rtr;
	RangeTblEntry *rte;
	Node	   *where;
	Node	   *te_expr;
	List	   *agg_vars;
	ListCell   *lc;
	ListCell   *lc2;
	int			n_corr = 0;
	bool		found_outer_rte = false;
	CommonTableExpr *cte = NULL;
	AttrNumber	oa;
	Var		   *inner_var;

	/* a declined earlier candidate must not leave stale match state */
	ctx->cte_name = NULL;
	ctx->outer_rtindex = 0;
	ctx->inner_rtindex = 0;
	ctx->agg_type = InvalidOid;
	ctx->agg_typmod = -1;
	ctx->agg_collid = InvalidOid;
	ctx->corr_inner_attnos = NIL;
	ctx->corr_outer_attnos = NIL;
	ctx->corr_types = NIL;
	ctx->corr_typmods = NIL;
	ctx->corr_collids = NIL;
	ctx->corr_eqexprs = NIL;

	if (!IsA(sl->subselect, Query))
		return decl(__LINE__);
	sq = (Query *) sl->subselect;
	if (sq->commandType != CMD_SELECT ||
		sq->groupClause != NIL || sq->havingQual != NULL ||
		sq->windowClause != NIL || sq->limitOffset != NULL ||
		sq->limitCount != NULL || sq->setOperations != NULL ||
		sq->distinctClause != NIL || sq->hasTargetSRFs ||
		sq->hasModifyingCTE ||
		sq->jointree == NULL ||
		list_length(sq->jointree->fromlist) != 1 ||
		!sq->hasAggs)
		return decl(__LINE__);

	if (!IsA(linitial(sq->jointree->fromlist), RangeTblRef))
		return decl(__LINE__);
	rtr = linitial_node(RangeTblRef, sq->jointree->fromlist);
	rte = rt_fetch(rtr->rtindex, sq->rtable);
	if (rte == NULL)
		return decl(__LINE__);

	/*
	 * v2: the subselect source is either a CTE of this query (q1 family) or a
	 * bare FOREIGN TABLE (the q6 family: item is a foreign table, not a CTE).
	 * Local heap sources keep stock planning. An inner WITH in the subselect
	 * declines (name-shadowing ambiguity, M2/13-review).
	 */
	if (rte->rtekind == RTE_CTE)
	{
		/*
		 * the CTE must resolve to THIS query's list (one level up from the
		 * subselect): a name-shadowing inner WITH would bind differently, and
		 * its copied RTE could not be re-homed (M2/13-review)
		 */
		if (rte->ctelevelsup != 1 || sq->cteList != NIL)
			return decl(__LINE__);

		/* the referenced CTE must be one of this query's CTEs */
		foreach(lc, parse->cteList)
		{
			cte = lfirst_node(CommonTableExpr, lc);

			if (strcmp(cte->ctename, rte->ctename) == 0)
			{
				ctx->cte_name = cte->ctename;
				break;
			}
		}
		if (ctx->cte_name == NULL)
			return decl(__LINE__);

		/*
		 * the CTE body must scan a foreign table: the per-row rescans are
		 * catastrophic there; local CTEs keep stock planning
		 */
		if (!foreign_rte_walker((Node *) cte->ctequery, NULL))
			return decl(__LINE__);

		/* locate the outer CTE alias among the outer rtable entries */
		for (int i = 1; i <= list_length(parse->rtable); i++)
		{
			RangeTblEntry *r = rt_fetch(i, parse->rtable);

			if (r != NULL && r->rtekind == RTE_CTE &&
				strcmp(r->ctename, rte->ctename) == 0)
			{
				ctx->outer_rtindex = i;
				found_outer_rte = true;
				break;
			}
		}
		if (!found_outer_rte)
			return decl(__LINE__);
	}
	else if (rte->rtekind == RTE_RELATION &&
			 rte->relkind == RELKIND_FOREIGN_TABLE)
	{
		/*
		 * bare foreign table (q6 family): the outer reference is whichever
		 * rtable entry the correlation equalities point at, derived from the
		 * WHERE conjuncts below. No same-table requirement: the equivalence
		 * holds for any outer table joined on the correlation keys (the
		 * aggregate reads the inner table only).
		 */
	}
	else
		return decl(__LINE__);
	ctx->inner_rtindex = rtr->rtindex;

	/* volatile anywhere declines the rewrite */
	if (contain_volatile_functions((Node *) sq))
		return decl(__LINE__);

	/*
	 * targetlist: exactly one non-resjunk entry holding an aggregate, with no
	 * outer-level Vars anywhere (the correlation lives in the WHERE
	 * equalities)
	 */
	if (list_length(sq->targetList) != 1)
		return decl(__LINE__);
	{
		TargetEntry *te = linitial_node(TargetEntry, sq->targetList);

		if (te->resjunk)
			return decl(__LINE__);
		te_expr = (Node *) te->expr;
		/* decline before pull_var_clause can ERROR on upper-level Vars */
		if (tle_has_outer_var_or_sublink_walker(te_expr, NULL))
			return decl(__LINE__);
		agg_vars = pull_var_clause(te_expr, PVC_INCLUDE_AGGREGATES);
		if (agg_vars == NIL)
			return decl(__LINE__);
		ctx->agg_type = exprType(te_expr);
		ctx->agg_typmod = exprTypmod(te_expr);
		ctx->agg_collid = exprCollation(te_expr);
	}

	/*
	 * WHERE: conjuncts of correlated equalities only (>= 1). For the
	 * bare-foreign-table path the outer alias is not known yet — derive it
	 * from the level-1 Vars in the conjuncts first: all outer references must
	 * name ONE rtable entry, whose RTE must itself be a foreign table (the
	 * correlation keys join the outer rows to the inner aggregate; the
	 * subselect reads the inner table only).
	 */
	if (sq->jointree->quals == NULL)
		return decl(__LINE__);
	where = (Node *) sq->jointree->quals;
	if (ctx->outer_rtindex == 0)
	{
		int			outer_varno = -1;
		ListCell   *varcell;

		elog(DEBUG1, "agg-pullup: foreign-path pre-pass, quals top=%d",
			 IsA(where, OpExpr) ? T_OpExpr :
			 IsA(where, BoolExpr) ? T_BoolExpr : -1);

		if (IsA(where, OpExpr))
		{
			foreach(varcell, ((OpExpr *) where)->args)
			{
				Node	   *arg = lfirst(varcell);

				while (arg != NULL && IsA(arg, RelabelType))
					arg = (Node *) ((RelabelType *) arg)->arg;
				if (arg != NULL && IsA(arg, Var) &&
					((Var *) arg)->varlevelsup == 1)
				{
					int			v = ((Var *) arg)->varno;

					if (outer_varno == -1)
						outer_varno = v;
					else if (outer_varno != v)
						return decl(__LINE__);
				}
			}
		}
		else if (IsA(where, BoolExpr) &&
				 ((BoolExpr *) where)->boolop == AND_EXPR)
		{
			foreach(varcell, ((BoolExpr *) where)->args)
			{
				Node	   *conj = lfirst(varcell);

				if (!IsA(conj, OpExpr))
					continue;
				foreach(varcell, ((OpExpr *) conj)->args)
				{
					Node	   *arg = lfirst(varcell);

					while (arg != NULL && IsA(arg, RelabelType))
						arg = (Node *) ((RelabelType *) arg)->arg;
					if (arg != NULL && IsA(arg, Var) &&
						((Var *) arg)->varlevelsup == 1)
					{
						int			v = ((Var *) arg)->varno;

						if (outer_varno == -1)
							outer_varno = v;
						else if (outer_varno != v)
							return decl(__LINE__);
					}
				}
			}
		}
		elog(DEBUG1, "agg-pullup: foreign-path outer_varno=%d natts-check next",
			 outer_varno);
		if (outer_varno <= 0)
			return decl(__LINE__);
		{
			/*
			 * the outer Vars live in the OUTER query's rtable (level-1 from
			 * the subselect's perspective), not in sq->rtable
			 */
			RangeTblEntry *r = rt_fetch(outer_varno, parse->rtable);

			if (r == NULL || r->rtekind != RTE_RELATION ||
				r->relkind != RELKIND_FOREIGN_TABLE)
			{
				elog(DEBUG1, "agg-pullup: foreign-path outer RTE %d not foreign (kind=%d rel=%c)",
					 outer_varno, r ? (int) r->rtekind : -1,
					 r ? r->relkind : '?');
				return decl(__LINE__);
			}
		}
		ctx->outer_rtindex = outer_varno;
	}
	if (sq->jointree->quals == NULL)
		return decl(__LINE__);
	where = (Node *) sq->jointree->quals;
	if (IsA(where, BoolExpr) &&
		((BoolExpr *) where)->boolop == AND_EXPR)
	{
		foreach(lc2, ((BoolExpr *) where)->args)
		{
			Node	   *c = lfirst(lc2);

			if (!IsA(c, OpExpr) ||
				!is_corr_equality((OpExpr *) c, ctx->outer_rtindex,
								  ctx->inner_rtindex, &oa, &inner_var))
				return decl(__LINE__);
			ctx->corr_eqexprs = lappend(ctx->corr_eqexprs,
										copyObject(c));
			ctx->corr_inner_attnos = lappend_int(ctx->corr_inner_attnos,
												 inner_var->varattno);
			ctx->corr_outer_attnos = lappend_int(ctx->corr_outer_attnos, oa);
			ctx->corr_types = lappend_oid(ctx->corr_types, inner_var->vartype);
			ctx->corr_typmods = lappend_int(ctx->corr_typmods,
											inner_var->vartypmod);
			ctx->corr_collids = lappend_oid(ctx->corr_collids,
											inner_var->varcollid);
			n_corr++;
		}
	}
	else if (IsA(where, OpExpr))
	{
		if (!is_corr_equality((OpExpr *) where, ctx->outer_rtindex,
							  ctx->inner_rtindex, &oa, &inner_var))
			return decl(__LINE__);
		ctx->corr_eqexprs = lappend(ctx->corr_eqexprs,
									copyObject(where));
		ctx->corr_inner_attnos = lappend_int(ctx->corr_inner_attnos,
											 inner_var->varattno);
		ctx->corr_outer_attnos = lappend_int(ctx->corr_outer_attnos, oa);
		ctx->corr_types = lappend_oid(ctx->corr_types, inner_var->vartype);
		ctx->corr_typmods = lappend_int(ctx->corr_typmods,
										inner_var->vartypmod);
		ctx->corr_collids = lappend_oid(ctx->corr_collids,
										inner_var->varcollid);
		n_corr = 1;
	}
	else
		return decl(__LINE__);

	return n_corr >= 1;
}

/*
 * set the Var numbering inside the grouped subquery: its single rtable
 * entry (the copied CTE) is index 1
 */
typedef struct VarFixCtx
{
	Index		new_rtindex;	/* rtable index of the grouped subquery */
	AttrNumber	new_resno;		/* its correlated-column resno for this eq */
} VarFixCtx;

/*
 * Retarget a copied correlation equality: level-1 Vars (the outer c1
 * reference, level 1 only inside the subselect) drop to level 0 for the
 * outer query scope; level-0 Vars (the inner c2 reference) re-target the
 * grouped subquery's correlated column.
 */
static Node *
fix_corr_eq_vars(Node *node, void *context)
{
	VarFixCtx  *fc = (VarFixCtx *) context;

	if (node == NULL)
		return NULL;
	if (IsA(node, Var))
	{
		Var		   *v = (Var *) node;

		if (v->varlevelsup == 1)
		{
			v->varlevelsup = 0;
			return node;
		}
		v->varno = fc->new_rtindex;
		v->varattno = fc->new_resno;
		return node;
	}
	return expression_tree_mutator(node, fix_corr_eq_vars, context);
}

static bool
set_agg_varno_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		Var		   *v = (Var *) node;

		if (v->varlevelsup == 0)
			v->varno = 1;
		return false;
	}
	if (IsA(node, Query))
		return false;			/* skip nested subqueries, keep walking */
	return expression_tree_walker(node, set_agg_varno_walker, context);
}

static void
pullup_cte_agg_sublinks(Query *parse)
{
	AggPullupContext ctx;
	OpExpr	   *candidate = NULL;
	SubLink    *sl;
	TargetEntry *te;
	Query	   *gq;
	RangeTblEntry *rte;
	RangeTblEntry *nrte;
	TargetEntry *agg_te;
	TargetEntry *gte;
	JoinExpr   *j;
	List	   *groupcls = NIL;
	int			i;

	memset(&ctx, 0, sizeof(ctx));
	if (parse->jointree == NULL ||
		parse->jointree->quals == NULL || parse->jointree->fromlist == NIL ||
		IsInParallelMode())
		return;

	/*
	 * find the first OpExpr carrying a matching correlated-agg SubLink. The
	 * SubLink may sit one level deep inside a scalar expression over it (the
	 * real q6 shape: i.price > 1.2 * (sublink) — OpExpr(>) with an
	 * OpExpr(*) arg), so descend into non-SubLink args of the top OpExpr as
	 * well. The candidate stays the TOP OpExpr (the strict outer comparison
	 * is checked on it).
	 */
	if (IsA(parse->jointree->quals, OpExpr))
	{
		OpExpr	   *op = (OpExpr *) parse->jointree->quals;
		ListCell   *a;
		bool		found = false;

		foreach(a, op->args)
		{
			if (IsA(lfirst(a), SubLink))
			{
				found = true;
				if (match_agg_sublink_shape((SubLink *) lfirst(a), parse,
											&ctx))
				{
					candidate = op;
					sl = lfirst_node(SubLink, a);
					break;
				}
			}
			else if (IsA(lfirst(a), OpExpr))
			{
				OpExpr	   *inner = (OpExpr *) lfirst(a);
				ListCell   *a2;

				foreach(a2, inner->args)
				{
					if (IsA(lfirst(a2), SubLink))
					{
						found = true;
						if (match_agg_sublink_shape((SubLink *) lfirst(a2),
													parse, &ctx))
						{
							candidate = op;
							sl = lfirst_node(SubLink, a2);
							break;
						}
					}
				}
				if (sl != NULL)
					break;
			}
		}
		if (found && sl == NULL)
		{
			/*
			 * the quals contain correlated-agg candidate SubLinks but no
			 * shape matched — say so (B1/verification: silent declines hid
			 * exactly this class in the v2 review)
			 */
			elog(DEBUG1, "agg-pullup: quals contain candidate SubLinks but "
				 "no shape matched");
			return;
		}
	}

	/*
	 * B2/13-review: only an AND-BoolExpr is safe to walk. An OR containing
	 * the sublink keeps rows the inner join would drop (TRUE OR NULL), and
	 * NOT has the same hazard through three-valued logic; decline both.
	 */
	else if (IsA(parse->jointree->quals, BoolExpr) &&
			 ((BoolExpr *) parse->jointree->quals)->boolop == AND_EXPR)
	{
		ListCell   *a;
		bool		found = false;

		foreach(a, ((BoolExpr *) parse->jointree->quals)->args)
		{
			Node	   *arg = lfirst(a);
			ListCell   *a2;
			bool		arg_found = false;

			if (!IsA(arg, OpExpr))
				continue;
			{
				ListCell   *a3;

				foreach(a3, ((OpExpr *) arg)->args)
				{
					if (IsA(lfirst(a3), SubLink))
					{
						found = true;
						if (match_agg_sublink_shape((SubLink *) lfirst(a3),
													parse, &ctx))
						{
							candidate = (OpExpr *) arg;
							sl = lfirst_node(SubLink, a3);
							arg_found = true;
							break;
						}
					}
					else if (IsA(lfirst(a3), OpExpr))
					{
						OpExpr	   *inner = (OpExpr *) lfirst(a3);
						ListCell   *a4;

						foreach(a4, inner->args)
						{
							if (IsA(lfirst(a4), SubLink))
							{
								found = true;
								if (match_agg_sublink_shape(
															(SubLink *) lfirst(a4), parse, &ctx))
								{
									candidate = (OpExpr *) arg;
									sl = lfirst_node(SubLink, a4);
									arg_found = true;
									break;
								}
							}
						}
						if (sl != NULL)
							break;
					}
				}
			}
			if (arg_found)
				break;
		}
		if (found && sl == NULL)
		{
			elog(DEBUG1, "agg-pullup: quals contain candidate SubLinks but "
				 "no shape matched");
			return;
		}
	}
	if (candidate == NULL)
		return;

	/*
	 * B1/13-review: the outer comparison must be a STRICT operator. The
	 * inner-join equivalence rests on `compare(t, NULL)` dropping the row
	 * exactly as a non-match does; a non-strict operator could keep such rows
	 * and the rewrite would lose them.
	 */
	set_opfuncid(candidate);
	if (!func_strict(candidate->opfuncid))
	{
		elog(DEBUG1, "agg-pullup: declined: outer comparison operator is not strict");
		return;
	}

	/*
	 * B3/13-review: the join must be attached to the member that actually
	 * carries the correlated alias. Attaching to llast(fromlist) breaks
	 * whenever the correlated RTE is not the last member (ON quals would
	 * reference a rel outside the join subtree -> lateral-reference
	 * assert/error).
	 */
	{
		ListCell   *fc;
		bool		found_member = false;

		foreach(fc, parse->jointree->fromlist)
		{
			Node	   *m = lfirst(fc);

			if (IsA(m, RangeTblRef) && ((RangeTblRef *) m)->rtindex == ctx.outer_rtindex)
			{
				found_member = true;
				break;
			}
		}
		if (!found_member)
		{
			elog(DEBUG1, "agg-pullup: declined: correlated CTE alias is not a "
				 "top-level fromlist member");
			return;
		}
	}

	/*
	 * build the grouped subquery: the pulled aggregate (resno 1) plus the
	 * correlated columns (grouping keys)
	 */
	gq = makeNode(Query);
	gq->commandType = CMD_SELECT;
	gq->hasAggs = true;
	rte = rt_fetch(ctx.inner_rtindex, ((Query *) sl->subselect)->rtable);
	nrte = copyObject(rte);

	/*
	 * PG16+: an RTE with relid MUST carry a perminfoindex into the new
	 * subquery's perminfo list — copy the subselect's entry and point at it
	 * (gq's rtable has exactly one entry, so index 1).
	 */
	if (rte->perminfoindex != 0)
	{
		RTEPermissionInfo *pinfo = list_nth(((Query *) sl->subselect)->rteperminfos,
											rte->perminfoindex - 1);

		gq->rteperminfos = lappend(gq->rteperminfos, copyObject(pinfo));
		nrte->perminfoindex = list_length(gq->rteperminfos);
	}

	/*
	 * keep the copied CTE RTE's alias/eref (colnames match the CTE output
	 * arity; the planner derives max_attr from eref->colnames)
	 */
	gq->rtable = list_make1(nrte);

	te = linitial_node(TargetEntry, ((Query *) sl->subselect)->targetList);
	agg_te = copyObject(te);
	agg_te->resno = 1;
	agg_te->resname = "agg";
	set_agg_varno_walker((Node *) agg_te->expr, NULL);
	gq->targetList = lappend(gq->targetList, agg_te);
	ctx.agg_resno = 1;

	for (i = 1; i <= list_length(ctx.corr_types); i++)
	{
		SortGroupClause *sgc;

		gte = makeTargetEntry((Expr *) makeVar(1,
											   list_nth_int(ctx.corr_inner_attnos, i - 1),
											   list_nth_oid(ctx.corr_types, i - 1),
											   list_nth_int(ctx.corr_typmods, i - 1),
											   list_nth_oid(ctx.corr_collids, i - 1), 0),
							  list_length(gq->targetList) + 1, NULL, false);
		gte->ressortgroupref = list_length(gq->targetList);
		gq->targetList = lappend(gq->targetList, gte);
		ctx.corr_resnos = lappend_int(ctx.corr_resnos, gte->resno);

		sgc = makeNode(SortGroupClause);
		sgc->tleSortGroupRef = gte->ressortgroupref;

		/*
		 * grouping needs only the equality operator; ordering ops are not
		 * required (hash-only types like point would ERROR on needLT/needGT)
		 */
		sgc->sortop = InvalidOid;
		get_sort_group_operators(list_nth_oid(ctx.corr_types, i - 1),
								 false, true, false,
								 NULL, &sgc->eqop, NULL,
								 &sgc->hashable);
		sgc->nulls_first = false;
		groupcls = lappend(groupcls, sgc);
	}
	gq->groupClause = groupcls;
	gq->jointree = makeNode(FromExpr);
	gq->jointree->fromlist = list_make1(makeNode(RangeTblRef));
	((RangeTblRef *) linitial(gq->jointree->fromlist))->rtindex = 1;

	/* new RTE for the grouped subquery */
	nrte = makeNode(RangeTblEntry);
	nrte->rtekind = RTE_SUBQUERY;
	nrte->subquery = gq;

	/*
	 * eref colnames must match the subquery output arity: the planner derives
	 * the rel's max_attr (attr_needed array size) from them.
	 */
	{
		List	   *colnames = list_make1(makeString("agg"));

		for (i = 1; i <= list_length(ctx.corr_types); i++)
			colnames = lappend(colnames,
							   makeString(psprintf("corr%d", i)));
		nrte->alias = makeAlias("agg_pullup", colnames);
		nrte->eref = copyObject(nrte->alias);
	}
	nrte->inFromCl = true;
	parse->rtable = lappend(parse->rtable, nrte);
	ctx.new_rtindex = list_length(parse->rtable);

	/*
	 * JOIN_INNER the grouped subquery on the correlated equalities: the
	 * SubPlan yields NULL on no match and `compare(t, NULL)` drops the row,
	 * which is exactly what a non-matching inner join does (the outer
	 * operator is checked strict above). INNER also keeps the planner's
	 * outer-join removal machinery out of the rewritten tree. Output columns:
	 * resno 1 = the pulled aggregate, resnos 2.. = the correlated columns in
	 * ctx order.
	 */
	j = makeNode(JoinExpr);
	j->jointype = JOIN_INNER;
	j->rarg = (Node *) makeNode(RangeTblRef);
	((RangeTblRef *) j->rarg)->rtindex = ctx.new_rtindex;
	j->alias = NULL;

	/*
	 * The grouped subquery's output columns are: resno 1 = the pulled
	 * aggregate, resnos 2.. = the correlated columns in ctx order. The ON
	 * equalities are copies of the original correlation OpExprs (preserving
	 * operator, collations and any RelabelType coercions) with their Vars
	 * retargeted: level-1 (outer c1) Vars drop to level 0, level-0 (inner c2)
	 * Vars re-target the grouped subquery's correlated columns.
	 */
	{
		List	   *onquals = NIL;

		for (i = 1; i <= list_length(ctx.corr_eqexprs); i++)
		{
			Expr	   *eq = (Expr *) copyObject(lfirst(list_nth_cell(ctx.corr_eqexprs, i - 1)));
			VarFixCtx	fc;

			fc.new_rtindex = ctx.new_rtindex;
			fc.new_resno = list_nth_int(ctx.corr_resnos, i - 1);
			eq = (Expr *) fix_corr_eq_vars((Node *) eq, &fc);
			onquals = lappend(onquals, eq);
		}
		if (list_length(onquals) == 1)
			j->quals = linitial(onquals);
		else
			j->quals = (Node *) make_andclause(onquals);
	}

	/*
	 * swap the correlated alias's fromlist member (verified above) for the
	 * join node; the join's ON references only its own children
	 */
	{
		ListCell   *fc;

		foreach(fc, parse->jointree->fromlist)
		{
			Node	   *m = lfirst(fc);

			if (IsA(m, RangeTblRef) && ((RangeTblRef *) m)->rtindex == ctx.outer_rtindex)
			{
				j->larg = m;
				lfirst(fc) = j;
				break;
			}
		}
	}

	/* swap the matched SubLink for a Var over the grouped subquery */
	{
		SublReplaceCtx rc;

		rc.target = sl;
		rc.new_rtindex = ctx.new_rtindex;
		rc.agg_resno = ctx.agg_resno;
		rc.agg_type = ctx.agg_type;
		rc.agg_typmod = ctx.agg_typmod;
		rc.agg_collid = ctx.agg_collid;
		parse->jointree->quals = (Node *)
			replace_sublink_mutator(parse->jointree->quals, &rc);
	}

	elog(DEBUG1, "lakehouse count_exec: pulled up correlated aggregate "
		 "over CTE %s as rtable index %d", ctx.cte_name, ctx.new_rtindex);
}

/*---------------------------------------------------------------------------
 * q4-pivot rewrite (spec 19 §5 entry 3, A2 phase 2 — supersedes the
 * phase-1 detection-only matcher).
 *
 * The q4 shape (receipts in spec 19 §5 entry 3): a UNION ALL of grouped
 * aggregate SELECTs, referenced once per (sale_type, year) pair — six
 * references — each carrying per-reference discriminator quals
 * (sale_type = 's', dyear = 2001 / 2001 + 1), a year_total > 0 guard,
 * and CASE ratio quals over the aggregates. The CTE materializes ~1.12M
 * rows and each reference scans it whole; because cost_ctescan excludes
 * the build cost, the planner races to Nested-Loop chains regardless of
 * how real the cardinality estimates get (A1 cte_stats_hook receipt:
 * estimates improved, plan stayed NL, query DNF'd at 900s).
 *
 * The rewrite folds each reference's discriminator filter INTO the CTE
 * aggregation as FILTERed aggregate clones — one clone per distinct
 * fold key (deduped conjunction of discriminator conditions) x aggregate
 * output — and coarsens GROUP BY by dropping the discriminator columns:
 *
 *   SUM(expr) FILTER (WHERE dyear = 2001 AND sale_type = 's')
 *
 * collapsing the CTE to one row per remaining group key with one column
 * per (key, aggregate). Each reference's discriminator quals are then
 * removed and its reads of an aggregate column are redirected to its
 * key's FILTERed clone (attno = base_natts + keyidx * nagg + ordinal).
 *
 * Equivalence obligations (spec 19 §5 entry 3 table):
 *   - SUM(expr) FILTER (WHERE p) over the coarsened groups = the exact
 *     subset sum the original per-year scan saw, for non-DISTINCT,
 *     non-FILTERed, non-ordered aggregates (validated per Aggref).
 *   - Empty subsets: the original shape has no (group, year) row; the
 *     pivot yields NULL. These behave identically ONLY under strict
 *     propagation, so the validator requires every remaining qual that
 *     touches a redirected column to be an AND-tree of strict operator
 *     comparisons, decline COALESCE/NullTest/OR/NOT and any other
 *     node that can absorb NULL (q4's CASE ratios are strict-comparison
 *     operands, which is the allowed form), and require each reference
 *     to consume its aggregate column in at least one remaining qual.
 *   - GROUP BY coarsening is safe because every non-aggregate output
 *     must already be a group key (or a constant) — validated.
 *
 * Everything is re-validated here in one pass BEFORE any mutation: any
 * decline leaves the parse untouched (fail-safe; the ordinary plan runs).
 *---------------------------------------------------------------------------
 */
typedef struct Q4DiscCond
{
	AttrNumber	attno;			/* CTE output attnum of discriminator column */
	Node	   *rhs;			/* RHS expression (Var/Param/SubLink-free) */
	Oid			opno;
	Oid			opfuncid;
	Oid			opresulttype;
	Oid			opcollid;
	Oid			inputcollid;
} Q4DiscCond;

typedef struct Q4RefInfo
{
	Index		rtindex;		/* rtable index of the CTE reference */
	List	   *conds;			/* List of Q4DiscCond* (>= 1 after matching) */
	int			keyidx;			/* deduplicated fold-key index */
	bool		agg_in_quals;	/* a remapped agg Var occurs in a remaining
								 * qual */
} Q4RefInfo;

typedef struct Q4PivotCtx
{
	CommonTableExpr *cte;
	char	   *ctename;
	int			base_natts;		/* CTE output width before appended columns */
	List	   *agg_resnos;		/* AttrNumber list: resnos of agg columns */
	int			nagg;
	List	   *keys;			/* List of List-of-Q4DiscCond* (deduped) */
	List	   *refs;			/* List of Q4RefInfo* */
	List	   *disc_qual_ptrs; /* Node* list: outer quals to remove */
	List	   *cond_exprs;		/* List of Node*: per-key leaf-side FILTER
								 * conds */
	List	   *new_types;		/* appended CTE column types */
	List	   *new_typmods;
	List	   *new_colls;
	List	   *new_names;		/* appended column names */
	List	   *leaves;			/* List of Query*: UNION-ALL branch SELECTs */
	List	   *leaf_rtes;		/* List of RangeTblEntry*: branch subquery
								 * RTEs */
	List	   *leaf_aggs;		/* per-leaf List of agg TargetEntrys
								 * (branch-native types: clones are per
								 * branch) */
} Q4PivotCtx;

/* banned-node walker for RHS discriminator expressions */
static bool
q4_rhs_banned_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var) || IsA(node, Param) || IsA(node, SubLink) ||
		IsA(node, Aggref) || IsA(node, WindowFunc) ||
		IsA(node, CaseTestExpr) || IsA(node, SubscriptingRef))
		return true;
	return expression_tree_walker(node, q4_rhs_banned_walker, context);
}

/* flatten an AND-only quals tree into a list of conjunct nodes */
static void
q4_flatten_and(Node *node, List **out)
{
	if (node == NULL)
		return;
	if (IsA(node, BoolExpr) && ((BoolExpr *) node)->boolop == AND_EXPR)
	{
		ListCell   *lc;

		foreach(lc, ((BoolExpr *) node)->args)
			q4_flatten_and((Node *) lfirst(lc), out);
	}
	else
		*out = lappend(*out, node);
}

static Node *
q4_rebuild_and(List *quals)
{
	if (quals == NIL)
		return NULL;
	if (list_length(quals) == 1)
		return (Node *) linitial(quals);
	return (Node *) make_ands_explicit(quals);
}

/* collect every SetOperationStmt node in a set-op tree */
static bool
q4_collect_setops_walker(Node *node, List **out)
{
	if (node == NULL || !IsA(node, SetOperationStmt))
		return false;
	*out = lappend(*out, node);
	{
		SetOperationStmt *s = (SetOperationStmt *) node;

		return q4_collect_setops_walker(s->larg, out) &&
			q4_collect_setops_walker(s->rarg, out);
	}
}

/*
 * Collect the SELECT leaves of a UNION-ALL set-operation tree. This fork
 * represents set-op leaves as rtable subqueries: children are
 * RangeTblRefs over RTE_SUBQUERY entries (PG18 set-op representation).
 * `scope` provides the rtable for RangeTblRef resolution.
 */
static bool
q4_setop_leaves_walker(Node *node, Query *scope, Q4PivotCtx *ctx)
{
	if (node == NULL)
		return false;
	if (IsA(node, SetOperationStmt))
	{
		SetOperationStmt *s = (SetOperationStmt *) node;

		if (s->op != SETOP_UNION || !s->all)
			return false;
		return q4_setop_leaves_walker(s->larg, scope, ctx) &&
			q4_setop_leaves_walker(s->rarg, scope, ctx);
	}
	if (IsA(node, RangeTblRef))
	{
		RangeTblEntry *rte = rt_fetch(((RangeTblRef *) node)->rtindex,
									  scope->rtable);

		if (rte->rtekind != RTE_SUBQUERY || rte->subquery == NULL)
			return false;
		ctx->leaf_rtes = lappend(ctx->leaf_rtes, rte);
		if (rte->subquery->setOperations != NULL)
			return q4_setop_leaves_walker(rte->subquery->setOperations,
										  rte->subquery, ctx);
		if (rte->subquery->commandType != CMD_SELECT)
			return false;
		ctx->leaves = lappend(ctx->leaves, rte->subquery);
		return true;
	}
	return false;
}

/*
 * Try to interpret `qual` as a discriminator condition of one of the
 * references: Var(ref) op rhs with a Var/Param/SubLink-free rhs.
 * Returns the matched reference and fills a Q4DiscCond, or NULL.
 */
static Q4RefInfo *
q4_match_disc_qual(Node *qual, Q4PivotCtx *ctx, Q4DiscCond **out_cond)
{
	OpExpr	   *op;
	Node	   *a,
			   *b;
	Var		   *var = NULL;
	Node	   *rhs = NULL;
	Q4RefInfo  *ri = NULL;
	ListCell   *lc;
	Q4DiscCond *cond;

	*out_cond = NULL;
	if (!IsA(qual, OpExpr) || list_length(((OpExpr *) qual)->args) != 2)
		return NULL;
	op = (OpExpr *) qual;
	a = linitial(op->args);
	b = lsecond(op->args);
	while (a != NULL && IsA(a, RelabelType))
		a = (Node *) ((RelabelType *) a)->arg;
	while (b != NULL && IsA(b, RelabelType))
		b = (Node *) ((RelabelType *) b)->arg;
	if (IsA(a, Var) && !IsA(b, Var))
	{
		var = (Var *) a;
		rhs = b;
	}
	else if (IsA(b, Var) && !IsA(a, Var))
	{
		var = (Var *) b;
		rhs = a;
	}
	else
		return NULL;

	foreach(lc, ctx->refs)
	{
		Q4RefInfo  *r = (Q4RefInfo *) lfirst(lc);

		if (r->rtindex == var->varno)
		{
			ri = r;
			break;
		}
	}
	if (ri == NULL)
		return NULL;
	/* aggregate outputs are redirected reads, not discriminator columns */
	if (list_member_int(ctx->agg_resnos, var->varattno))
		return NULL;
	if (contain_var_clause(rhs) || contain_volatile_functions(rhs) ||
		q4_rhs_banned_walker(rhs, NULL))
		return NULL;

	set_opfuncid(op);
	cond = (Q4DiscCond *) palloc(sizeof(Q4DiscCond));
	cond->attno = var->varattno;
	cond->rhs = rhs;
	cond->opno = op->opno;
	cond->opfuncid = op->opfuncid;
	cond->opresulttype = op->opresulttype;
	cond->opcollid = op->opcollid;
	cond->inputcollid = op->inputcollid;
	*out_cond = cond;
	return ri;
}

/* fold-key equality: same attnos/operators and equal(rhs) trees */
static bool
q4_conds_equal(List *c1, List *c2)
{
	ListCell   *lc1;
	ListCell   *lc2;

	if (list_length(c1) != list_length(c2))
		return false;
	forboth(lc1, c1, lc2, c2)
	{
		Q4DiscCond *a = (Q4DiscCond *) lfirst(lc1);
		Q4DiscCond *b = (Q4DiscCond *) lfirst(lc2);

		if (a->attno != b->attno || a->opno != b->opno ||
			!equal(a->rhs, b->rhs))
			return false;
	}
	return true;
}

/* validation state for one reference's remaining quals */
typedef struct Q4QualCheck
{
	Index		rtindex;
	List	   *disc_attnos;	/* AttrNumber list */
	List	   *agg_attnos;		/* AttrNumber list */
	bool		agg_seen;
	bool		ok;
} Q4QualCheck;

static bool
q4_has_special_walker(Node *node, void *context)
{
	Q4QualCheck *c = (Q4QualCheck *) context;

	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		Var		   *v = (Var *) node;

		return v->varno == c->rtindex &&
			(list_member_int(c->disc_attnos, v->varattno) ||
			 list_member_int(c->agg_attnos, v->varattno));
	}
	return expression_tree_walker(node, q4_has_special_walker, context);
}

static bool
q4_check_walker(Node *node, void *context)
{
	Q4QualCheck *c = (Q4QualCheck *) context;

	if (node == NULL || !c->ok)
		return true;			/* stop on first violation */
	if (IsA(node, Var))
	{
		Var		   *v = (Var *) node;

		if (v->varno == c->rtindex)
		{
			/* whole-row reference: the folded CTE row changes shape */
			if (v->varattno == InvalidAttrNumber)
			{
				c->ok = false;
				return true;
			}
			if (list_member_int(c->disc_attnos, v->varattno))
			{
				/* discriminator read outside its removal qual */
				c->ok = false;
				return true;
			}
			if (list_member_int(c->agg_attnos, v->varattno))
				c->agg_seen = true;
		}
		return false;
	}
	if (IsA(node, Const) || IsA(node, RelabelType) ||
		IsA(node, CaseExpr) || IsA(node, CaseWhen))
		return expression_tree_walker(node, q4_check_walker, context);
	if (IsA(node, OpExpr) || IsA(node, ScalarArrayOpExpr))
	{
		/*
		 * NULL from a redirected agg column must propagate and kill the row:
		 * every operator feeding on it has to be strict (the CASE ratios are
		 * consumed by strict comparisons — the allowed form).
		 */
		if (expression_tree_walker(node, q4_has_special_walker, c))
		{
			Oid			opfuncid = ((OpExpr *) node)->opfuncid;

			if (IsA(node, ScalarArrayOpExpr))
			{
				ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) node;

				if (saop->opfuncid == 0)
					saop->opfuncid = get_opcode(saop->opno);
				opfuncid = saop->opfuncid;
			}
			else
			{
				set_opfuncid((OpExpr *) node);
				opfuncid = ((OpExpr *) node)->opfuncid;
			}
			if (!func_strict(opfuncid))
			{
				c->ok = false;
				return true;
			}
		}
		return expression_tree_walker(node, q4_check_walker, context);
	}
	if (IsA(node, BoolExpr))
	{
		if (((BoolExpr *) node)->boolop != AND_EXPR &&
			expression_tree_walker(node, q4_has_special_walker, c))
		{
			/* OR/NOT can absorb a NULL (TRUE OR NULL = TRUE) */
			c->ok = false;
			return true;
		}
		return expression_tree_walker(node, q4_check_walker, context);
	}

	/*
	 * anything else (Coalesce, NullTest, FuncExpr, ...) is declined when it
	 * can touch a special Var — it may absorb NULLs
	 */
	if (expression_tree_walker(node, q4_has_special_walker, c))
	{
		c->ok = false;
		return true;
	}
	return false;
}

/*
 * Root requirement: a remaining qual touching a redirected column must
 * be an AND-tree whose leaves are strict comparisons. A CASE at the root
 * could return TRUE without consuming the NULL, emitting rows the
 * original shape never had.
 */
static bool
q4_qual_root_ok(Node *node)
{
	if (node == NULL)
		return false;
	if (IsA(node, BoolExpr) && ((BoolExpr *) node)->boolop == AND_EXPR)
	{
		ListCell   *lc;

		foreach(lc, ((BoolExpr *) node)->args)
		{
			if (!q4_qual_root_ok((Node *) lfirst(lc)))
				return false;
		}
		return true;
	}
	return IsA(node, OpExpr) || IsA(node, ScalarArrayOpExpr);
}

/* does this leaf expression read the discriminator source Var? */
static bool
q4_leaf_reads_var_walker(Node *node, void *context)
{
	Var		   *dv = (Var *) context;

	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		Var		   *v = (Var *) node;

		return v->varno == dv->varno && v->varattno == dv->varattno &&
			v->vartype == dv->vartype;
	}
	return expression_tree_walker(node, q4_leaf_reads_var_walker, context);
}

static bool
q4_leaf_reads_var(Node *node, Var *dv)
{
	return q4_leaf_reads_var_walker(node, (void *) dv);
}

/* remap Var(ref, agg_resno) -> Var(ref, FILTERed clone attno) */
static Node *
q4_remap_mutator(Node *node, void *context)
{
	Q4PivotCtx *ctx = (Q4PivotCtx *) context;

	if (node == NULL)
		return NULL;
	if (IsA(node, Var))
	{
		Var		   *v = (Var *) node;
		ListCell   *lc;

		foreach(lc, ctx->refs)
		{
			Q4RefInfo  *ri = (Q4RefInfo *) lfirst(lc);

			if (ri->rtindex == v->varno)
			{
				ListCell   *lc2;
				int			ord = 0;

				foreach(lc2, ctx->agg_resnos)
				{
					if ((AttrNumber) lfirst_int(lc2) == v->varattno)
					{
						Var		   *nv = copyObject(v);

						nv->varattno = ctx->base_natts +
							ri->keyidx * ctx->nagg + ord + 1;
						return (Node *) nv;
					}
					ord++;
				}
			}
		}
		return node;
	}
	return expression_tree_mutator(node, q4_remap_mutator, context);
}

/* does this expression read a redirected aggregate column of any ref? */
static bool
q4_reads_agg_walker(Node *node, void *context)
{
	Q4PivotCtx *ctx = (Q4PivotCtx *) context;

	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		Var		   *v = (Var *) node;
		ListCell   *lc;

		foreach(lc, ctx->refs)
		{
			Q4RefInfo  *ri = (Q4RefInfo *) lfirst(lc);

			if (ri->rtindex == v->varno &&
				list_member_int(ctx->agg_resnos, v->varattno))
				return true;
		}
		return false;
	}
	return expression_tree_walker(node, q4_reads_agg_walker, context);
}

/*
 * Validate one UNION-ALL leaf without mutating it. The discriminator
 * output columns must be simple Vars or Consts; every aggregate output
 * must be a clean (non-DISTINCT, non-FILTER, non-ordered, non-star)
 * Aggref over non-volatile args. Returns the list of agg TargetEntrys.
 */
static bool
q4_leaf_validate(Query *leaf, Q4PivotCtx *ctx, List **agg_entries_out)
{
	List	   *aggs = NIL;
	List	   *disc_attnos = NIL;
	ListCell   *lc;
	ListCell   *lc2;

	if (leaf->commandType != CMD_SELECT ||
		leaf->groupClause == NIL || !leaf->hasAggs ||
		leaf->hasWindowFuncs || leaf->hasDistinctOn ||
		leaf->distinctClause != NIL ||
		leaf->hasSubLinks || leaf->hasModifyingCTE ||
		leaf->havingQual != NULL ||
		leaf->limitOffset != NULL || leaf->limitCount != NULL ||
		leaf->sortClause != NIL || leaf->setOperations != NULL ||
		leaf->cteList != NIL)
		return false;

	/* the distinct discriminator attnos across all fold keys */
	foreach(lc, ctx->keys)
	{
		foreach(lc2, (List *) lfirst(lc))
		{
			Q4DiscCond *c = (Q4DiscCond *) lfirst(lc2);

			if (!list_member_int(disc_attnos, c->attno))
				disc_attnos = lappend_int(disc_attnos, c->attno);
		}
	}

	foreach(lc, leaf->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		if (te->resjunk)
			continue;
		if (IsA(te->expr, Aggref))
		{
			Aggref	   *agg = (Aggref *) te->expr;

			if (agg->aggdistinct != NIL || agg->aggfilter != NULL ||
				agg->aggorder != NIL || agg->aggdirectargs != NIL ||
				agg->aggstar ||
				contain_volatile_functions((Node *) agg->args))
				return false;
			aggs = lappend(aggs, te);
			continue;
		}
		if (list_member_int(disc_attnos, te->resno))
		{
			/* discriminator output: simple Var or Const only */
			if (!IsA(te->expr, Var) && !IsA(te->expr, Const))
				return false;
		}
	}

	*agg_entries_out = aggs;
	return true;
}

/*
 * Build one key's leaf-side FILTER condition from THIS leaf's own
 * discriminator expressions. In this fork grouped outputs are GROUP Vars
 * over the query's RTE_GROUP, and flatten_group_exprs explicitly does not
 * descend into Aggref args or aggfilter — so the condition must use the
 * underlying base expression from groupexprs, never the GROUP Var itself.
 */
static Node *
q4_build_leaf_cond(Query *leaf, List *conds)
{
	Node	   *cond = NULL;
	ListCell   *lc;

	foreach(lc, conds)
	{
		Q4DiscCond *cnd = (Q4DiscCond *) lfirst(lc);
		TargetEntry *te = list_nth(leaf->targetList, cnd->attno - 1);
		Node	   *lhs = (Node *) te->expr;
		OpExpr	   *o = makeNode(OpExpr);

		if (IsA(lhs, Var))
		{
			Var		   *v = (Var *) lhs;
			RangeTblEntry *rte = rt_fetch(v->varno, leaf->rtable);

			if (rte->rtekind == RTE_GROUP)
			{
				Assert(v->varattno > 0 &&
					   v->varattno <= list_length(rte->groupexprs));
				lhs = copyObject((Node *) list_nth(rte->groupexprs,
												   v->varattno - 1));
			}
		}

		o->opno = cnd->opno;
		o->opfuncid = cnd->opfuncid;
		o->opresulttype = cnd->opresulttype;
		o->opcollid = cnd->opcollid;
		o->inputcollid = cnd->inputcollid;
		o->location = -1;
		o->args = list_make2(lhs, copyObject(cnd->rhs));
		cond = cond == NULL ? (Node *) o
			: (Node *) make_andclause(list_make2(cond, o));
	}
	return cond;
}

/*
 * Pre-mutation check of the GROUP BY coarsening law: the discriminator
 * group expressions may only be removed as a trailing suffix of the
 * fork's RTE_GROUP groupexprs (GROUP Vars address them by position), and
 * a leaf with hasGroupRTE must actually carry the GROUP RTE. Read-only;
 * emits the same decline receipts the mutation phase would.
 */
static bool
q4_leaf_coarsening_ok(Query *leaf, Q4PivotCtx *ctx)
{
	List	   *disc_attnos = NIL;
	int			nexprs = 0;		/* groupexprs positions seen */
	int			keep_exprs = -1;
	bool		any_disc_group = false;
	ListCell   *lc;
	ListCell   *lc2;

	foreach(lc, ctx->keys)
	{
		foreach(lc2, (List *) lfirst(lc))
		{
			Q4DiscCond *cnd = (Q4DiscCond *) lfirst(lc2);

			if (!list_member_int(disc_attnos, cnd->attno))
				disc_attnos = lappend_int(disc_attnos, cnd->attno);
		}
	}

	foreach(lc, leaf->groupClause)
	{
		SortGroupClause *gc = lfirst_node(SortGroupClause, lc);
		bool		is_disc = false;
		ListCell   *lc3;

		foreach(lc3, leaf->targetList)
		{
			TargetEntry *te = lfirst_node(TargetEntry, lc3);

			if (te->ressortgroupref == gc->tleSortGroupRef &&
				list_member_int(disc_attnos, te->resno))
			{
				is_disc = true;
				break;
			}
		}
		if (get_sortgroupclause_tle(gc, leaf->targetList) != NULL)
			nexprs++;
		if (is_disc)
		{
			any_disc_group = true;
			if (keep_exprs < 0)
				keep_exprs = nexprs - 1;
		}
		else if (keep_exprs >= 0)
		{
			elog(DEBUG1, "q4-pivot: declined: discriminator group expr is not trailing");
			return false;
		}
	}

	if (leaf->hasGroupRTE)
	{
		RangeTblEntry *grte = NULL;
		ListCell   *lc3;

		foreach(lc3, leaf->rtable)
		{
			RangeTblEntry *r = lfirst_node(RangeTblEntry, lc3);

			if (r->rtekind == RTE_GROUP)
			{
				grte = r;
				break;
			}
		}
		if (grte == NULL)
		{
			elog(DEBUG1, "q4-pivot: declined: hasGroupRTE without GROUP RTE");
			return false;
		}
		(void) any_disc_group;
		(void) keep_exprs;
	}
	return true;
}

/*
 * Sweep outer-level expression locations the remap does not cover and
 * the validator does not walk: parse->havingQual, fromlist JoinExpr ON
 * quals, and the interiors of SubLink subselects (expression_tree_walker
 * does not descend into a SubLink's Query). Any Var belonging to one of
 * our references (aggregate, discriminator, or whole-row column) in
 * those positions would read the un-remapped or neutralized CTE after
 * the fold — decline the whole rewrite. `depth` is the query level: our
 * refs live at depth 0; inside a subselect they are varlevelsup >= 1.
 */
typedef struct Q4BannedCtx
{
	List	   *refs;			/* List of Q4RefInfo* */
	List	   *banned_attnos;	/* union of disc + agg resnos (whole-row
								 * checked separately) */
	int			depth;
	bool		ok;
	bool		skip_depth0;	/* true when sweeping locations that ARE
								 * remapped at depth 0 (remaining quals and
								 * targetList): only their SubLink interiors
								 * and whole-row uses are suspect */
} Q4BannedCtx;

static void q4_sweep_fromlist(Node *node, Q4BannedCtx *c);

static bool
q4_banned_var(Var *v, Q4BannedCtx *c)
{
	ListCell   *lc;

	if (v->varlevelsup != c->depth)
		return false;
	foreach(lc, c->refs)
	{
		Q4RefInfo  *ri = (Q4RefInfo *) lfirst(lc);

		if (ri->rtindex != v->varno)
			continue;
		/* whole-row reference: the folded CTE row is a different shape */
		if (v->varattno == InvalidAttrNumber)
			return true;

		/*
		 * depth-0 agg/disc reads in remaining quals and targetList are
		 * remapped — only flag them in locations the remap never touches.
		 */
		if (c->skip_depth0 && c->depth == 0)
			return false;
		if (list_member_int(c->banned_attnos, v->varattno))
			return true;
	}
	return false;
}

static bool
q4_banned_walker(Node *node, void *context)
{
	Q4BannedCtx *c = (Q4BannedCtx *) context;

	if (node == NULL || !c->ok)
		return true;
	if (IsA(node, Var))
	{
		if (q4_banned_var((Var *) node, c))
		{
			c->ok = false;
			return true;
		}
		return false;
	}
	if (IsA(node, Query))
	{
		/*
		 * A subselect Query: refs' columns reappear one level up. Only
		 * descend into its expressions (its own rtable/ctes are a different
		 * world; a same-indexed rel there is not ours).
		 */
		Query	   *q = (Query *) node;

		c->depth++;
		(void) expression_tree_walker((Node *) q->targetList,
									  q4_banned_walker, c);
		if (c->ok && q->havingQual != NULL)
			(void) expression_tree_walker(q->havingQual,
										  q4_banned_walker, c);
		if (c->ok && q->jointree != NULL)
		{
			(void) expression_tree_walker(q->jointree->quals,
										  q4_banned_walker, c);
			/* outer refs are visible to ON quals inside subqueries too */
			if (c->ok && q->jointree->fromlist != NIL)
				q4_sweep_fromlist((Node *) q->jointree->fromlist, c);
		}
		c->depth--;
		return true;
	}
	if (IsA(node, SubLink))
	{
		SubLink    *sl = (SubLink *) node;

		(void) expression_tree_walker(sl->testexpr, q4_banned_walker, c);
		if (c->ok && IsA(sl->subselect, Query))
			(void) q4_banned_walker(sl->subselect, c);
		return true;
	}
	return expression_tree_walker(node, q4_banned_walker, context);
}

/*
 * Walk the fromlist for JoinExpr ON quals (depth 0) — RangeTblRef leaves
 * carry no expressions; JoinExpr/FromExpr carry the ON quals we must
 * sweep.
 */
static void
q4_sweep_fromlist(Node *node, Q4BannedCtx *c)
{
	if (node == NULL || !c->ok)
		return;
	if (IsA(node, RangeTblRef))
		return;
	if (IsA(node, JoinExpr))
	{
		JoinExpr   *j = (JoinExpr *) node;

		(void) expression_tree_walker(j->quals, q4_banned_walker, c);
		q4_sweep_fromlist(j->larg, c);
		q4_sweep_fromlist(j->rarg, c);
		return;
	}
	if (IsA(node, FromExpr))
	{
		FromExpr   *f = (FromExpr *) node;
		ListCell   *lc;

		(void) expression_tree_walker(f->quals, q4_banned_walker, c);
		foreach(lc, f->fromlist)
			q4_sweep_fromlist((Node *) lfirst(lc), c);
	}
}

static bool
q4_pivot_apply(Query *parse)
{
	Q4PivotCtx	ctx;
	CommonTableExpr *best_cte = NULL;
	int			best_refs = 0;
	List	   *leaves = NIL;
	List	   *flat_quals = NIL;
	List	   *remaining = NIL;
	ListCell   *lc;
	ListCell   *lc2;
	int			i;
	int			leafno;

	if (parse->commandType != CMD_SELECT ||
		parse->cteList == NIL ||
		parse->jointree == NULL ||
		parse->jointree->quals == NULL ||
		parse->jointree->fromlist == NIL ||
		parse->distinctClause != NIL ||
		IsInParallelMode())
	{
		elog(DEBUG1, "q4-pivot: declined: outer query shape not supported");
		return false;
	}

	memset(&ctx, 0, sizeof(ctx));

	/* 1. pick the most-referenced non-recursive CTE with a Query body */
	foreach(lc, parse->cteList)
	{
		CommonTableExpr *c = (CommonTableExpr *) lfirst(lc);

		if (c->cterefcount < 2 || c->cterecursive)
			continue;
		if (!IsA(c->ctequery, Query) || c->aliascolnames != NIL)
			continue;
		if (c->cterefcount > best_refs)
		{
			best_refs = c->cterefcount;
			best_cte = c;
		}
	}
	if (best_cte == NULL)
	{
		elog(DEBUG1, "q4-pivot: declined: no multiply-referenced CTE candidate");
		return false;
	}
	ctx.cte = best_cte;
	ctx.ctename = best_cte->ctename;
	ctx.base_natts = list_length(best_cte->ctecoltypes);

	/* 2. leaves: a UNION-ALL tree of grouped SELECTs (or a single one) */
	{
		Query	   *cq = (Query *) best_cte->ctequery;

		if (cq->setOperations != NULL)
		{
			if (!q4_setop_leaves_walker(cq->setOperations, cq, &ctx))
			{
				elog(DEBUG1, "q4-pivot: declined: set-op tree is not UNION ALL of SELECTs");
				return false;
			}
		}
		else
			ctx.leaves = list_make1(cq);
	}
	leaves = ctx.leaves;

	/*
	 * Aggregate output resnos (tlist positions are identical across UNION ALL
	 * branches). Needed BEFORE qual matching: quals over an aggregate output
	 * column (q4's `year_total > 0` guards) are redirected reads, never
	 * discriminator conditions — swallowing them would delete the guards
	 * outright.
	 */
	{
		Query	   *leaf0 = linitial(leaves);
		ListCell   *lc0;

		foreach(lc0, leaf0->targetList)
		{
			TargetEntry *te = lfirst_node(TargetEntry, lc0);

			if (!te->resjunk && IsA(te->expr, Aggref))
				ctx.agg_resnos = lappend_int(ctx.agg_resnos, te->resno);
		}
		ctx.nagg = list_length(ctx.agg_resnos);
		if (ctx.nagg == 0)
		{
			elog(DEBUG1, "q4-pivot: declined: CTE has no aggregate outputs");
			return false;
		}
	}

	/* 3. references of this CTE in the rtable */
	i = 1;
	foreach(lc, parse->rtable)
	{
		RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);

		if (rte->rtekind == RTE_CTE && rte->ctename != NULL &&
			strcmp(rte->ctename, ctx.ctename) == 0 && !rte->self_reference)
		{
			Q4RefInfo  *ri = (Q4RefInfo *) palloc0(sizeof(Q4RefInfo));

			ri->rtindex = i;
			ctx.refs = lappend(ctx.refs, ri);
		}
		i++;
	}
	if (list_length(ctx.refs) != best_cte->cterefcount || list_length(ctx.refs) < 2)
	{
		elog(DEBUG1, "q4-pivot: declined: reference count mismatch");
		return false;
	}

	/*
	 * ORDER BY over a redirected column would sort on the FILTERed clone's
	 * attno while the SortGroupClause still references the original output —
	 * simplest to decline that family.
	 */
	if (parse->sortClause != NIL)
	{
		foreach(lc, parse->sortClause)
		{
			SortGroupClause *gc = lfirst_node(SortGroupClause, lc);
			ListCell   *lc3;

			foreach(lc3, parse->targetList)
			{
				TargetEntry *te = lfirst_node(TargetEntry, lc3);

				if (te->ressortgroupref == gc->tleSortGroupRef &&
					q4_reads_agg_walker((Node *) te->expr, &ctx))
				{
					elog(DEBUG1, "q4-pivot: declined: ORDER BY over redirected aggregate");
					return false;
				}
			}
		}
	}

	/* 4. match discriminator quals per reference */
	q4_flatten_and(parse->jointree->quals, &flat_quals);
	foreach(lc, flat_quals)
	{
		Node	   *qual = (Node *) lfirst(lc);
		Q4DiscCond *cond = NULL;
		Q4RefInfo  *ri;

		ri = q4_match_disc_qual(qual, &ctx, &cond);
		if (ri != NULL)
		{
			ri->conds = lappend(ri->conds, cond);
			ctx.disc_qual_ptrs = lappend(ctx.disc_qual_ptrs, qual);
		}
		else
			remaining = lappend(remaining, qual);
	}
	foreach(lc, ctx.refs)
	{
		Q4RefInfo  *ri = (Q4RefInfo *) lfirst(lc);

		if (ri->conds == NIL)
		{
			elog(DEBUG1, "q4-pivot: declined: reference %d has no discriminator qual",
				 ri->rtindex);
			return false;
		}
	}

	/* 5. deduplicate fold keys */
	foreach(lc, ctx.refs)
	{
		Q4RefInfo  *ri = (Q4RefInfo *) lfirst(lc);
		ListCell   *lc3;
		int			k = 0;

		foreach(lc3, ctx.keys)
		{
			if (q4_conds_equal(ri->conds, (List *) lfirst(lc3)))
			{
				ri->keyidx = k;
				break;
			}
			k++;
		}
		if (lc3 == NULL)
		{
			ctx.keys = lappend(ctx.keys, ri->conds);
			ri->keyidx = list_length(ctx.keys) - 1;
		}
	}

	/* 6. validate every leaf (no mutation yet) */
	{
		List	   *first_aggs = NIL;
		bool		first = true;

		foreach(lc, leaves)
		{
			Query	   *leaf = lfirst_node(Query, lc);
			List	   *this_aggs = NIL;
			ListCell   *lc3;
			ListCell   *lc4;

			if (!q4_leaf_validate(leaf, &ctx, &this_aggs))
			{
				elog(DEBUG1, "q4-pivot: declined: leaf shape not foldable");
				return false;
			}

			/*
			 * Pre-mutation coarsening checks (the mutation must never decline
			 * midway): the discriminator group expressions may only be
			 * removed as a trailing suffix of groupexprs, and a leaf with
			 * hasGroupRTE must actually carry the GROUP RTE.
			 */
			if (!q4_leaf_coarsening_ok(leaf, &ctx))
			{
				return false;	/* receipt emitted by the check */
			}

			ctx.leaf_aggs = lappend(ctx.leaf_aggs, this_aggs);
			if (first)
			{
				first = false;
				first_aggs = this_aggs;
			}
			else
			{
				/*
				 * Branch consistency: same agg count and resnos. Types may
				 * differ across branches (real TPC-DS schemas mix double
				 * precision and numeric money columns); the UNION already
				 * resolved a common output type in ctecoltypes, and the
				 * set-op machinery coerces each branch's clone to it —
				 * exactly as it does for the original column. Decline any
				 * branch type that is not implicitly coercible to that
				 * resolved type.
				 */
				if (list_length(this_aggs) != list_length(first_aggs))
				{
					elog(DEBUG1, "q4-pivot: declined: agg count differs across branches");
					return false;
				}
				forboth(lc3, first_aggs, lc4, this_aggs)
				{
					TargetEntry *a = lfirst_node(TargetEntry, lc3);
					TargetEntry *b = lfirst_node(TargetEntry, lc4);
					Aggref	   *aa = (Aggref *) a->expr;
					Aggref	   *ba = (Aggref *) b->expr;
					Oid			resolved;

					Assert(a->resno >= 1 &&
						   a->resno <= list_length(ctx.cte->ctecoltypes));
					resolved = list_nth_oid(ctx.cte->ctecoltypes,
											a->resno - 1);
					if (!can_coerce_type(1, &ba->aggtype, &resolved,
										 COERCION_IMPLICIT))
					{
						elog(DEBUG1, "q4-pivot: declined: branch agg type not coercible to resolved CTE type");
						return false;
					}
				}
			}
		}
	}


	/*
	 * agg resnos were captured from leaf0 before qual matching; nagg set
	 * there
	 */

	/*
	 * 7. validate remaining quals per reference: strict comparison
	 * consumption of redirected columns, no discriminator reads, and at least
	 * one redirected-column use per reference.
	 */
	foreach(lc, ctx.refs)
	{
		Q4RefInfo  *ri = (Q4RefInfo *) lfirst(lc);
		Q4QualCheck check;
		ListCell   *lc3;

		check.rtindex = ri->rtindex;
		check.disc_attnos = NIL;
		check.agg_attnos = ctx.agg_resnos;
		check.agg_seen = false;
		check.ok = true;
		foreach(lc3, ctx.keys)
		{
			foreach(lc2, (List *) lfirst(lc3))
			{
				Q4DiscCond *cnd = (Q4DiscCond *) lfirst(lc2);

				if (!list_member_int(check.disc_attnos, cnd->attno))
					check.disc_attnos = lappend_int(check.disc_attnos,
													cnd->attno);
			}
		}
		foreach(lc3, remaining)
		{
			Node	   *qual = (Node *) lfirst(lc3);

			if (!expression_tree_walker(qual, q4_has_special_walker, &check))
				continue;		/* qual does not touch this ref's columns */
			if (!q4_qual_root_ok(qual))
			{
				elog(DEBUG1, "q4-pivot: declined: qual root not a strict comparison");
				return false;
			}
			q4_check_walker(qual, &check);
			if (!check.ok)
			{
				elog(DEBUG1, "q4-pivot: declined: non-strict or NULL-absorbing use of redirected column");
				return false;
			}
		}
		if (!check.agg_seen)
		{
			elog(DEBUG1, "q4-pivot: declined: reference %d never consumes its aggregate column",
				 ri->rtindex);
			return false;
		}
		ri->agg_in_quals = check.agg_seen;
	}

	/*
	 * 7.5. sweep expression locations the validator/remap do not cover:
	 * HAVING, JoinExpr ON quals, and SubLink subselect interiors. Also
	 * decline whole-row references to our CTE (its row shape changes).
	 * Pre-mutation: any hit declines the whole rewrite.
	 */
	{
		Q4BannedCtx sweep;
		ListCell   *lc3;

		sweep.refs = ctx.refs;
		sweep.banned_attnos = NIL;
		sweep.depth = 0;
		sweep.ok = true;
		sweep.skip_depth0 = false;
		foreach(lc3, ctx.keys)
		{
			foreach(lc2, (List *) lfirst(lc3))
			{
				Q4DiscCond *cnd = (Q4DiscCond *) lfirst(lc2);

				if (!list_member_int(sweep.banned_attnos, cnd->attno))
					sweep.banned_attnos = lappend_int(sweep.banned_attnos,
													  cnd->attno);
			}
		}
		foreach(lc3, ctx.agg_resnos)
		{
			if (!list_member_int(sweep.banned_attnos, lfirst_int(lc3)))
				sweep.banned_attnos = lappend_int(sweep.banned_attnos,
												  lfirst_int(lc3));
		}

		if (parse->havingQual != NULL)
			(void) expression_tree_walker(parse->havingQual,
										  q4_banned_walker, &sweep);
		if (sweep.ok && parse->jointree->fromlist != NIL)
			q4_sweep_fromlist((Node *) parse->jointree->fromlist, &sweep);
		if (sweep.ok)
		{
			/*
			 * Remaining quals and targetList ARE remapped at depth 0 —
			 * sweep them only for unremapped uses: SubLink subselect
			 * interiors and whole-row references.
			 */
			sweep.skip_depth0 = true;
			foreach(lc3, remaining)
			{
				Node	   *qual = (Node *) lfirst(lc3);

				(void) expression_tree_walker(qual, q4_banned_walker, &sweep);
				if (!sweep.ok)
					break;
			}
			if (sweep.ok)
			{
				foreach(lc3, parse->targetList)
				{
					TargetEntry *te = lfirst_node(TargetEntry, lc3);

					(void) expression_tree_walker((Node *) te->expr,
												  q4_banned_walker, &sweep);
					if (!sweep.ok)
						break;
				}
			}
		}
		if (!sweep.ok)
		{
			elog(DEBUG1, "q4-pivot: declined: ref column in HAVING / join ON / SubLink subselect / whole-row use");
			return false;
		}
	}

	/* 8. leaf-side validation of discriminator source columns */
	{
		List	   *disc_attnos = NIL;

		foreach(lc, ctx.keys)
		{
			foreach(lc2, (List *) lfirst(lc))
			{
				Q4DiscCond *cnd = (Q4DiscCond *) lfirst(lc2);

				if (!list_member_int(disc_attnos, cnd->attno))
					disc_attnos = lappend_int(disc_attnos, cnd->attno);
			}
		}

		foreach(lc, leaves)
		{
			Query	   *leaf = lfirst_node(Query, lc);
			ListCell   *lc3;

			foreach(lc3, disc_attnos)
			{
				AttrNumber	attno = (AttrNumber) lfirst_int(lc3);
				TargetEntry *te;
				Node	   *src;

				if (attno < 1 || attno > list_length(leaf->targetList))
				{
					elog(DEBUG1, "q4-pivot: declined: discriminator attno out of range");
					return false;
				}
				te = list_nth(leaf->targetList, attno - 1);
				if (te->resno != attno)
				{
					elog(DEBUG1, "q4-pivot: declined: discriminator column is not a base column");
					return false;
				}
				src = (Node *) te->expr;
				if (IsA(src, Const))
					continue;	/* constant column: no coarsening hazard */

				/*
				 * Var source: after the GROUP BY coarsening no other leaf
				 * expression or qual may read this Var (its value would no
				 * longer be constant within a group).
				 */
				if (!IsA(src, Var))
				{
					elog(DEBUG1, "q4-pivot: declined: discriminator column is not a base column");
					return false;
				}
				else
				{
					Var		   *dv = (Var *) src;
					ListCell   *lc4;

					foreach(lc4, leaf->targetList)
					{
						TargetEntry *te2 = lfirst_node(TargetEntry, lc4);

						if (te2->resno == attno)
							continue;
						if (q4_leaf_reads_var((Node *) te2->expr, dv))
						{
							elog(DEBUG1, "q4-pivot: declined: discriminator Var re-read in leaf output");
							return false;
						}
					}
					if (leaf->jointree->quals != NULL &&
						q4_leaf_reads_var(leaf->jointree->quals, dv))
					{
						elog(DEBUG1, "q4-pivot: declined: discriminator Var re-read in leaf quals");
						return false;
					}
				}
			}
		}
	}

	/*
	 * 9/10. MUTATE the leaves: neutralize disc outputs, coarsen GROUP BY,
	 * append FILTERed agg clones per key. The FILTER conditions are built PER
	 * LEAF from that branch's own discriminator expressions — a const
	 * discriminator like q4's sale_type differs per branch ('s'/'c'/'w'), and
	 * sharing the first leaf's condition would contaminate every other
	 * branch's clone columns.
	 */
	ctx.new_types = NIL;
	ctx.new_typmods = NIL;
	ctx.new_colls = NIL;
	ctx.new_names = NIL;
	leafno = 0;
	foreach(lc, leaves)
	{
		Query	   *leaf = lfirst_node(Query, lc);
		List	   *disc_attnos = NIL;
		List	   *leaf_conds = NIL;	/* per-key FILTER conds */
		ListCell   *lc3;
		ListCell   *lc4;

		foreach(lc3, ctx.keys)
		{
			foreach(lc2, (List *) lfirst(lc3))
			{
				Q4DiscCond *cnd = (Q4DiscCond *) lfirst(lc2);

				if (!list_member_int(disc_attnos, cnd->attno))
					disc_attnos = lappend_int(disc_attnos, cnd->attno);
			}
		}

		/*
		 * Build this leaf's per-key FILTER conditions FIRST: they read
		 * groupexprs, which the GROUP BY coarsening below truncates.
		 */
		foreach(lc4, ctx.keys)
			leaf_conds = lappend(leaf_conds,
								 q4_build_leaf_cond(leaf,
													(List *) lfirst(lc4)));

		/*
		 * Drop the discriminator GROUP BY items, keeping the fork's RTE_GROUP
		 * machinery consistent: groupexprs mirror the GROUP BY traversal, and
		 * GROUP Vars address them by position — so the discriminator
		 * expressions may only be removed when they form a trailing suffix of
		 * groupexprs (q4: d_year is the last GROUP BY item).
		 */
		{
			List	   *kept = NIL;
			int			nexprs = 0; /* groupexprs positions seen */
			int			keep_exprs = -1;
			ListCell   *lc4;

			foreach(lc4, leaf->groupClause)
			{
				SortGroupClause *gc = lfirst_node(SortGroupClause, lc4);
				bool		is_disc = false;
				ListCell   *lc5;

				foreach(lc5, leaf->targetList)
				{
					TargetEntry *te = lfirst_node(TargetEntry, lc5);

					if (te->ressortgroupref == gc->tleSortGroupRef &&
						list_member_int(disc_attnos, te->resno))
					{
						is_disc = true;
						break;
					}
				}
				if (get_sortgroupclause_tle(gc, leaf->targetList) != NULL)
					nexprs++;
				if (is_disc)
				{
					if (keep_exprs < 0)
						keep_exprs = nexprs - 1;
				}
				else if (keep_exprs >= 0)
				{
					/* pre-checked in q4_leaf_coarsening_ok; unreachable */
					elog(ERROR, "q4-pivot: internal: non-trailing discriminator group expr");
				}
				if (!is_disc)
					kept = lappend(kept, gc);
			}
			leaf->groupClause = kept;

			/* keep RTE_GROUP.groupexprs in sync with the shorter GROUP BY */
			if (leaf->hasGroupRTE)
			{
				RangeTblEntry *grte = NULL;
				ListCell   *lc5;

				foreach(lc5, leaf->rtable)
				{
					RangeTblEntry *r = lfirst_node(RangeTblEntry, lc5);

					if (r->rtekind == RTE_GROUP)
					{
						grte = r;
						break;
					}
				}
				if (grte == NULL)
				{
					/* pre-checked in q4_leaf_coarsening_ok; unreachable */
					elog(ERROR, "q4-pivot: internal: hasGroupRTE without GROUP RTE");
				}
				if (keep_exprs >= 0)
					grte->groupexprs = list_truncate(grte->groupexprs,
													 keep_exprs);
			}
		}

		foreach(lc3, leaf->targetList)
		{
			TargetEntry *te = lfirst_node(TargetEntry, lc3);

			if (te->resjunk || !list_member_int(disc_attnos, te->resno))
				continue;
			if (IsA(te->expr, Var))
			{
				Var		   *v = (Var *) te->expr;

				te->expr = (Expr *) makeNullConst(v->vartype, v->vartypmod,
												  v->varcollid);
				te->ressortgroupref = 0;
			}
			/* Const discriminator outputs (e.g. q4's sale_type) stay */
		}

		/* append FILTERed agg clones: one per (key, agg) */
		{
			int			k = 0;
			ListCell   *lc4;
			List	   *this_leaf_aggs = (List *) list_nth(ctx.leaf_aggs,
														   leafno);

			foreach(lc4, ctx.keys)
			{
				Node	   *cond_k = (Node *) list_nth(leaf_conds, k);
				ListCell   *lc5;
				int			i = 0;

				/*
				 * Clone THIS branch's own aggregates: resno positions are
				 * identical across branches, but branch-native types differ
				 * (numeric vs double precision money) and a foreign branch's
				 * Aggref carries Vars over the wrong base relation.
				 */
				foreach(lc5, this_leaf_aggs)
				{
					TargetEntry *agg_te = lfirst_node(TargetEntry, lc5);
					Aggref	   *na = (Aggref *) copyObject(agg_te->expr);
					TargetEntry *new_te;
					char	   *nm = psprintf("lh_q4sum_%d_%d", k, i);

					na->aggfilter = (Expr *) cond_k;
					new_te = makeTargetEntry((Expr *) na,
											 list_length(leaf->targetList) + 1,
											 nm, false);
					leaf->targetList = lappend(leaf->targetList, new_te);
					if (lc == list_head(leaves))
					{
						/*
						 * Column metadata comes from the UNION's resolved
						 * output type (ctecoltypes at the original agg
						 * position) — branches with a different native agg
						 * type (numeric vs double precision money columns)
						 * are coerced by the set-op machinery, exactly as for
						 * the original column.
						 */
						Assert(agg_te->resno >= 1 &&
							   agg_te->resno <= list_length(ctx.cte->ctecoltypes));
						ctx.new_types = lappend_oid(ctx.new_types,
													list_nth_oid(ctx.cte->ctecoltypes,
																 agg_te->resno - 1));
						ctx.new_typmods = lappend_int(ctx.new_typmods,
													  list_nth_int(ctx.cte->ctecoltypmods,
																   agg_te->resno - 1));
						ctx.new_colls = lappend_oid(ctx.new_colls,
													list_nth_oid(ctx.cte->ctecolcollations,
																 agg_te->resno - 1));
						ctx.new_names = lappend(ctx.new_names,
												makeString(pstrdup(nm)));
					}
					i++;
				}
				k++;
			}
		}
		leafno++;
	}

	/* 11. CTE output metadata + reference RTE column lists */
	{
		Query	   *cq = (Query *) ctx.cte->ctequery;

		/*
		 * UNION-ALL branch planning projects per the SetOperationStmts' own
		 * colTypes lists — extend every node in the tree or the appended
		 * columns would be silently dropped by the Append projection.
		 */
		if (cq->setOperations != NULL)
		{
			List	   *setops = NIL;
			Node	   *node;

			(void) q4_collect_setops_walker(cq->setOperations, &setops);
			foreach(lc, setops)
			{
				SetOperationStmt *s = lfirst_node(SetOperationStmt, lc);

				s->colTypes = list_concat(s->colTypes, ctx.new_types);
				s->colTypmods = list_concat(s->colTypmods, ctx.new_typmods);
				s->colCollations = list_concat(s->colCollations,
											   ctx.new_colls);
			}

			/*
			 * A simple UNION ALL gets flattened into an appendrel by
			 * flatten_simple_union_all, which clears setOperations: the CTE's
			 * output then IS this query's targetlist (parse analysis built it
			 * as Vars over the leftmost leaf). Extend it with matching Vars
			 * or the appended columns vanish from the CTE's physical output.
			 */
			node = ((SetOperationStmt *) cq->setOperations)->larg;
			while (node != NULL && IsA(node, SetOperationStmt))
				node = ((SetOperationStmt *) node)->larg;
			if (node == NULL || !IsA(node, RangeTblRef))
			{
				/* the leaf walk already required RangeTblRef leaves */
				elog(ERROR, "q4-pivot: internal: no leftmost leaf for output extension");
			}
			{
				int			leftmost_rti = ((RangeTblRef *) node)->rtindex;
				int			resno = list_length(cq->targetList);
				int			attno = ctx.base_natts + 1; /* first appended attno */
				int			newidx = 0;
				ListCell   *lc3;

				foreach(lc3, ctx.new_names)
				{
					Var		   *v = makeVar(leftmost_rti,
											(AttrNumber) attno,
											list_nth_oid(ctx.new_types,
														 newidx),
											(AttrNumber) list_nth_int(ctx.new_typmods,
																	  newidx),
											list_nth_oid(ctx.new_colls,
														 newidx),
											0);
					TargetEntry *te = makeTargetEntry((Expr *) v,
													  ++resno,
													  strVal(lfirst(lc3)),
													  false);

					cq->targetList = lappend(cq->targetList, te);
					attno++;
					newidx++;
				}
			}
		}

		ctx.cte->ctecoltypes = list_concat(ctx.cte->ctecoltypes,
										   ctx.new_types);
		ctx.cte->ctecoltypmods = list_concat(ctx.cte->ctecoltypmods,
											 ctx.new_typmods);
		ctx.cte->ctecolcollations = list_concat(ctx.cte->ctecolcollations,
												ctx.new_colls);
		if (ctx.cte->aliascolnames != NIL)
			ctx.cte->aliascolnames = list_concat(ctx.cte->aliascolnames,
												 ctx.new_names);

		/*
		 * UNION-ALL branch subquery RTEs and the outer CTE references carry
		 * their own column metadata in this fork — extend all of them.
		 */
		foreach(lc, ctx.leaf_rtes)
		{
			RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);
			ListCell   *lc3;

			rte->coltypes = list_concat(rte->coltypes, ctx.new_types);
			rte->coltypmods = list_concat(rte->coltypmods, ctx.new_typmods);
			rte->colcollations = list_concat(rte->colcollations,
											 ctx.new_colls);
			foreach(lc3, ctx.new_names)
			{
				String	   *s = lfirst_node(String, lc3);

				rte->eref->colnames = lappend(rte->eref->colnames,
											  makeString(pstrdup(strVal(s))));
			}
		}
		foreach(lc, ctx.refs)
		{
			Q4RefInfo  *ri = (Q4RefInfo *) lfirst(lc);
			RangeTblEntry *rte = rt_fetch(ri->rtindex, parse->rtable);
			ListCell   *lc3;

			rte->coltypes = list_concat(rte->coltypes, ctx.new_types);
			rte->coltypmods = list_concat(rte->coltypmods, ctx.new_typmods);
			rte->colcollations = list_concat(rte->colcollations,
											 ctx.new_colls);
			foreach(lc3, ctx.new_names)
			{
				String	   *s = lfirst_node(String, lc3);

				rte->eref->colnames = lappend(rte->eref->colnames,
											  makeString(pstrdup(strVal(s))));
			}
		}
	}

	/* 12. outer query: drop discriminator quals, remap agg reads */
	{
		List	   *remapped = NIL;
		ListCell   *lc3;

		foreach(lc3, remaining)
		{
			Node	   *qual = (Node *) lfirst(lc3);

			remapped = lappend(remapped,
							   q4_remap_mutator(qual, &ctx));
		}
		parse->jointree->quals = q4_rebuild_and(remapped);

		foreach(lc3, parse->targetList)
		{
			TargetEntry *te = lfirst_node(TargetEntry, lc3);

			te->expr = (Expr *) q4_remap_mutator((Node *) te->expr, &ctx);
		}
	}

	elog(DEBUG1, "q4-pivot: folded %d references into %d keys over CTE %s",
		 list_length(ctx.refs), list_length(ctx.keys), ctx.ctename);
	return true;
}

static PlannedStmt *
lakehouse_count_exec_planner(Query *parse, const char *query_string,
							 int cursorOptions, ParamListInfo boundParams,
							 ExplainState *es)
{
	Oid			relid = InvalidOid;
	bool		bound = false;
	MemoryContext entries_ctx = NULL;

	extern bool lakehouse_cte_stats_hook;

	elog(DEBUG1, "lakehouse count_exec: hook entered");
	if (!count_exec_in_fallback && managed_count_wanted(parse, &relid,
														&bound))
	{
		elog(DEBUG1, "lakehouse count_exec: rewriting count(*) relid %u%s",
			 (unsigned) relid, bound ? " (cut fast path)" : "");
		rewrite_to_managed_count(parse, relid, bound);
	}

	if (lakehouse_agg_pullup_enabled)
		pullup_cte_agg_sublinks(parse);

	if (lakehouse_q4_pivot_enabled && q4_pivot_apply(parse))
		ereport(DEBUG1,
				(errmsg("lakehouse count_exec: q4-pivot fold applied")));

	/*
	 * family-C/E planning input (spec 19 §6.5): synthesize statistics for
	 * multiply-referenced CTE discriminator columns so the 6-way self-join
	 * shapes cost Hash Joins, not Nested Loop chains (q4 DNF root cause —
	 * CTE-output columns carry no pg_statistic). The collector walks the
	 * parse's own equality literals; entries live in a dedicated context that
	 * dies with this planning cycle. Gated by lakehouse.cte_stats_hook —
	 * off skips collect/begin so a failed planning cycle never consults stale
	 * entries (round-7 should-fix 1).
	 */
	if (lakehouse_cte_stats_hook)
	{
		entries_ctx = AllocSetContextCreate(CurrentMemoryContext,
											"lakehouse cte ndv entries",
											ALLOCSET_SMALL_SIZES);
		MemoryContext old_ctx = MemoryContextSwitchTo(entries_ctx);
		List	   *entries = collect_cte_ndv_entries(parse);

		MemoryContextSwitchTo(old_ctx);
		lakehouse_cte_stats_hook_begin(entries, entries_ctx);
	}

	{
		PlannedStmt *result;

		if (prev_planner_hook != NULL)
			result = prev_planner_hook(parse, query_string,
									   cursorOptions, boundParams, es);
		else
			result = standard_planner(parse, query_string,
									  cursorOptions, boundParams, es);
		lakehouse_cte_stats_hook_end();
		if (entries_ctx != NULL)
			MemoryContextDelete(entries_ctx);
		return result;
	}
}

void
install_lakehouse_count_exec(void)
{
	prev_planner_hook = planner_hook;
	planner_hook = lakehouse_count_exec_planner;
}

void
lakehouse_q4_pivot_guc_init(void)
{
	DefineCustomBoolVariable("lakehouse.q4_pivot",
							 "Fold per-reference discriminator quals of a multiply-referenced CTE into conditional aggregation (spec 19 §5 entry 3).",
							 NULL,
							 &lakehouse_q4_pivot_enabled,
							 true,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);
}

/*
 * lakehouse.managed_count(oid) returns bigint — the execution-time COUNT
 * described in the file header. VOLATILE (declared in the extension
 * script): re-evaluated per execution, so prepared plans stay fresh.
 * SECURITY INVOKER: the caller's permissions and RLS apply.
 */
static void count_exec_run_ordinary(Oid relid, int64 *result);

PG_FUNCTION_INFO_V1(lakehouse_managed_count_cut);

/*
 * Metadata-only COUNT for capture-bound tables (spec 03, "managed
 * COUNT"): the planner rewrite routes bound count(*) here. Computes the
 * installed cut's two disjoint parts — the published base from Parquet
 * metadata (no data-file reads) plus the overlay ledger row_count —
 * with the same admission, privileges and snapshot as the resolved
 * scan. Unadmitted/unsupported shapes refuse exactly as the scan does.
 */
Datum
lakehouse_managed_count_cut(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	AclResult	aclresult;

	aclresult = pg_class_aclcheck(relid, GetUserId(), ACL_SELECT);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_TABLE, get_rel_name(relid));

	LockRelationOid(relid, AccessShareLock);
	PG_RETURN_INT64(lakehouse_managed_cut_count(relid));
}


PG_FUNCTION_INFO_V1(lakehouse_managed_count);

Datum
lakehouse_managed_count(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	AclResult	aclresult;
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

	LockRelationOid(relid, AccessShareLock);
	lakehouse_check_capture_read(relid);

	/*
	 * The ledger receives staged rows at pre-commit. A backend with write
	 * state for this relation must use the same overlay as a normal scan. RLS
	 * and inherited rows also require the ordinary aggregate.
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
