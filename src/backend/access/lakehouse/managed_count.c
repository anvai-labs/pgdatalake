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
#include "fmgr.h"
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
#include "utils/rel.h"
#include "utils/rls.h"
#include "utils/syscache.h"

#include "lakehouse.h"
#include "lakehouse_tableam.h"
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
static PlannedStmt *
lakehouse_count_exec_planner(Query *parse, const char *query_string,
							 int cursorOptions, ParamListInfo boundParams,
							 ExplainState *es)
{
	Oid			relid = InvalidOid;
	bool		bound = false;


	elog(DEBUG1, "lakehouse count_exec: hook entered");
	if (!count_exec_in_fallback && managed_count_wanted(parse, &relid,
														&bound))
	{
		elog(DEBUG1, "lakehouse count_exec: rewriting count(*) relid %u%s",
			 (unsigned) relid, bound ? " (cut fast path)" : "");
		rewrite_to_managed_count(parse, relid, bound);
	}

	pullup_cte_agg_sublinks(parse);

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
