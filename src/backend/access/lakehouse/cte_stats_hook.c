/*---------------------------------------------------------------------------
 * CTE-output statistics synthesis (spec 19 §6.5 family-C/E remedy, q4).
 *
 * The q4 shape: a CTE referenced >= 2 times, self-joined with
 * per-reference equality quals (discriminator columns like d_year /
 * sale_type), executed as Nested-Loop chains because CTE-output columns
 * carry no pg_statistic — set_baserel_size_estimates applies
 * DEFAULT_EQ_SEL (0.005) per equality qual, so a 6M-row CTE scan
 * estimates ~150 rows and cost_ctescan (which excludes CTE build costs)
 * makes Nested Loop win the poisoned cost race.
 *
 * This hook gives the planner magnitude-real synthetic statistics for
 * exactly those columns: for each (CTE, column) pair observed with >= 2
 * DISTINCT equality literals across the reference quals, fabricate a
 * pg_statistic tuple with stanullfrac = 0 and n_distinct = the observed
 * literal count. eqsel then estimates ~1/ndv — same magnitude as the
 * real data (q4's d_year NDV ≈ #years, sale_type NDV = #channels).
 *
 * Soundness: this is a PLANNING INPUT ONLY (spec 19 §6.1 doctrine). It
 * changes cost estimates — never rows, never rewrites, never the
 * matcher. Gate: lakehouse.cte_stats_hook (PGC_USERSET, default on).
 *
 * The fabricated tuple is a real pg_statistic-shaped heap tuple (built
 * from the live catalog's own descriptor) so selfuncs' heap_getattr
 * reads of stanullfrac/n_distinct work by position; freefunc releases
 * it. Lifetime is one planning cycle.
 *---------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_type.h"
#include "utils/array.h"
#include "fmgr.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "optimizer/planner.h"
#include "parser/parsetree.h"
#include "parser/parse_relation.h"
#include "executor/executor.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"

#include "lakehouse_tableam.h"
#include "cte_stats_hook.h"

/*
 * Planning-cycle state: per-(CTE, column) synthesized distinct entries
 * built by the planner hook before standard_planner, cleared after (the
 * stats hook fires during standard_planner with no query context of its
 * own).
 */
static List *cte_ndv_entries = NIL; /* List of CteColNdv* */
static MemoryContext cte_stats_plan_ctx = NULL;

bool		lakehouse_cte_stats_hook = true;

void		lakehouse_cte_stats_hook_init(void);
void		lakehouse_cte_stats_hook_begin(List *entries,
										   MemoryContext plan_ctx);
void		lakehouse_cte_stats_hook_end(void);

static get_relation_stats_hook_type prev_stats_hook = NULL;
static bool stats_hook_installed = false;

extern bool lakehouse_q4_pivot_enabled;


/* synthetic pg_statistic tuple ------------------------------------------------ */

/*
 * Build a pg_statistic-shaped tuple with stanullfrac = 0 and
 * n_distinct = ndv (absolute). Attribute positions are resolved by NAME
 * against the live catalog descriptor so the fabricated tuple always
 * matches what selfuncs' heap_getattr reads.
 */
static HeapTuple
make_fake_stats_tuple(AttrNumber attnum, float4 ndv)
{
	Relation	rel;
	TupleDesc	tupdesc;
	Datum	   *values;
	bool	   *nulls;
	HeapTuple	tup;
	int			natt;

	rel = table_open(StatisticRelationId, AccessShareLock);
	tupdesc = CreateTupleDescCopy(RelationGetDescr(rel));
	table_close(rel, AccessShareLock);

	natt = tupdesc->natts;
	values = (Datum *) palloc0(sizeof(Datum) * natt);
	nulls = (bool *) palloc(sizeof(bool) * natt);

	/*
	 * Fill EVERY column: the fixed columns with sensible values, and the five
	 * varlen slots (stakind/staop/stacoll/stanumbers/stavalues) with kind-0 +
	 * empty arrays. NULL varlen columns would crash syscache —
	 * get_attstatsslot reads stavaluesN via SysCacheGetAttrNotNull in its
	 * slot loop regardless of kind match (2026-11-27 CI failure).
	 */
	for (int i = 0; i < natt; i++)
	{
		const char *name = NameStr(TupleDescAttr(tupdesc, i)->attname);

		if (strcmp(name, "starelid") == 0)
			values[i] = ObjectIdGetDatum(0);
		else if (strcmp(name, "staattnum") == 0)
			values[i] = Int16GetDatum(attnum);
		else if (strcmp(name, "stainherit") == 0)
			values[i] = BoolGetDatum(false);
		else if (strcmp(name, "stanullfrac") == 0)
			values[i] = Float4GetDatum(0.0f);
		else if (strcmp(name, "stawidth") == 0)
			values[i] = Int32GetDatum(10);
		else if (strcmp(name, "stadistinct") == 0)
			values[i] = Float4GetDatum(ndv);
		else if (strncmp(name, "n_mod_since_analyze", 19) == 0 ||
				 strncmp(name, "n_inherited_since_analyze", 25) == 0 ||
				 strncmp(name, "n_distinct_estimator", 20) == 0)
			values[i] = Float4GetDatum(0.0f);
		else if (strncmp(name, "stakind", 7) == 0)
			values[i] = Int16GetDatum(0);
		else if (strncmp(name, "staop", 5) == 0 ||
				 strncmp(name, "stacoll", 7) == 0)
			values[i] = ObjectIdGetDatum(0);
		else if (strncmp(name, "stanumbers", 10) == 0)
		{
			values[i] = PointerGetDatum(construct_empty_array(FLOAT4OID));
			nulls[i] = false;
		}
		else if (strncmp(name, "stavalues", 9) == 0)
		{
			values[i] = PointerGetDatum(construct_empty_array(ANYELEMENTOID));
			nulls[i] = false;
		}
	}

	tup = heap_form_tuple(tupdesc, values, nulls);
	FreeTupleDesc(tupdesc);
	pfree(values);
	pfree(nulls);
	return tup;
}

static void
cte_stats_free(HeapTuple tup)
{
	heap_freetuple(tup);
}

/*
 * Resolve a CTE by name within the query's own cteList (top level).
 */
static const CommonTableExpr *
find_cte_by_name(Query *parse, const char *ctename)
{
	ListCell   *lc;

	if (parse == NULL || parse->cteList == NIL)
		return NULL;
	foreach(lc, parse->cteList)
	{
		CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);

		if (cte->ctename != NULL && strcmp(cte->ctename, ctename) == 0)
			return cte;
	}
	return NULL;
}

static const char *
cte_column_name(const CommonTableExpr *cte, AttrNumber attnum)
{
	Query	   *q;

	if (!IsA(cte->ctequery, Query))
		return NULL;
	q = (Query *) cte->ctequery;
	if (attnum <= 0 || attnum > list_length(q->targetList))
		return NULL;
	return ((TargetEntry *)
			list_nth(q->targetList, attnum - 1))->resname;
}

/* get_relation_stats_hook ----------------------------------------------------- */

static bool
cte_stats_hook(PlannerInfo *root, RangeTblEntry *rte, AttrNumber attnum,
			   VariableStatData *vardata)
{
	const char *ctename;
	const char *colname;
	ListCell   *lc;

	/*
	 * Gate inside the hook too: the GUC-off planner path skips begin()/end()
	 * entirely, so a stale (dangling) entries list from a prior planning
	 * cycle must never be consulted while the gate is off (round-7 should-
	 * fix 1). Superusers/instances may disable mid-session.
	 */
	if (!lakehouse_cte_stats_hook || cte_ndv_entries == NIL)
		return false;
	ctename = rte->ctename;
	if (ctename == NULL)
		return false;

	/* resolve the CTE column's name via the CTE's own targetlist */
	{
		const CommonTableExpr *cte = find_cte_by_name(root->parse, ctename);

		if (cte == NULL)
			return false;
		colname = cte_column_name(cte, attnum);
	}
	if (colname == NULL)
		return false;

	foreach(lc, cte_ndv_entries)
	{
		CteColNdv  *e = (CteColNdv *) lfirst(lc);

		if (e->attnum == attnum && strcmp(e->colname, colname) == 0 &&
			strcmp(e->ctename, ctename) == 0)
		{
			vardata->statsTuple = make_fake_stats_tuple(attnum, (float4) e->ndv);
			vardata->freefunc = cte_stats_free;
			return true;
		}
	}
	return false;
}

/* init + lifecycle ------------------------------------------------------------ */

void
lakehouse_cte_stats_hook_init(void)
{
	if (stats_hook_installed)
		return;
	stats_hook_installed = true;

	DefineCustomBoolVariable("lakehouse.cte_stats_hook",
							 "Synthesize planner statistics for multiply-referenced CTE discriminator columns from equality literals in the reference quals (planning input only — changes cost estimates, never rows).",
							 NULL,
							 &lakehouse_cte_stats_hook,
							 true,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);

	prev_stats_hook = get_relation_stats_hook;
	get_relation_stats_hook = cte_stats_hook;
}

/*
 * Install the planning-cycle state. `entries` (list of CteColNdv*, built
 * by the caller in plan_ctx) is consumed here.
 */
void
lakehouse_cte_stats_hook_begin(List *entries, MemoryContext plan_ctx)
{
	cte_ndv_entries = entries;
	cte_stats_plan_ctx = plan_ctx;
}

void
lakehouse_cte_stats_hook_end(void)
{
	cte_ndv_entries = NIL;
	cte_stats_plan_ctx = NULL;
}

/*
 * collect_cte_ndv_entries (called from the planner hook): walk the TOP
 * LEVEL query's equality quals and record, per multiply-referenced CTE,
 * the DISTINCT literals its discriminator columns are compared against.
 * Top level only — q4's six references are top-level, and deeper levels
 * carry their own rtables (wrong-level Var resolution would miskey).
 * Returns entries built in the caller's current context.
 */
typedef struct CteNdvWalkContext
{
	Query	   *parse;
	List	   *entries;
	List	   *seen_keys;		/* int64 identity keys, per-walk dedupe */
} CteNdvWalkContext;

static bool
cte_qual_walker(Node *node, void *context)
{
	CteNdvWalkContext *ctx = (CteNdvWalkContext *) context;

	if (node == NULL)
		return false;

	if (IsA(node, OpExpr))
	{
		OpExpr	   *op = (OpExpr *) node;

		if (list_length(op->args) == 2)
		{
			Node	   *a = linitial(op->args);
			Node	   *b = lsecond(op->args);
			Var		   *var = NULL;
			Const	   *cst = NULL;

			/*
			 * strip RelabelType: text quals arrive as (Var::text = 'literal')
			 */
			while (a != NULL && IsA(a, RelabelType))
				a = (Node *) ((RelabelType *) a)->arg;
			while (b != NULL && IsA(b, RelabelType))
				b = (Node *) ((RelabelType *) b)->arg;

			if (IsA(a, Var) && IsA(b, Const))
			{
				var = (Var *) a;
				cst = (Const *) b;
			}
			else if (IsA(b, Var) && IsA(a, Const))
			{
				var = (Var *) b;
				cst = (Const *) a;
			}

			if (var != NULL && cst != NULL &&
				var->varno > 0 && var->varno <= list_length(ctx->parse->rtable))
			{
				RangeTblEntry *rte = rt_fetch(var->varno,
											  ctx->parse->rtable);

				if (rte->rtekind == RTE_CTE && rte->ctename != NULL &&
					!cst->constisnull)
				{
					const CommonTableExpr *cte =
						find_cte_by_name(ctx->parse, rte->ctename);

					if (cte != NULL)
					{
						const char *col = cte_column_name(cte,
														  var->varattno);

						if (col != NULL)
						{
							CteColNdv  *entry = NULL;
							ListCell   *lc;

							foreach(lc, ctx->entries)
							{
								CteColNdv  *e = (CteColNdv *) lfirst(lc);

								if (e->attnum == var->varattno &&
									strcmp(e->ctename, rte->ctename) == 0 &&
									strcmp(e->colname, col) == 0)
								{
									entry = e;
									break;
								}
							}
							if (entry == NULL)
							{
								entry = (CteColNdv *)
									palloc0(sizeof(CteColNdv));
								entry->ctename = pstrdup(rte->ctename);
								entry->colname = pstrdup(col);
								entry->attnum = var->varattno;
								entry->ndv = 0;
								ctx->entries = lappend(ctx->entries, entry);
							}

							/*
							 * dedupe literals: for by-value datums
							 * (int/float/date) the datum holds the value, so
							 * the 8-byte key is a true identity test; for
							 * pointer datums each occurrence counts — that
							 * can only OVER-estimate ndv, which
							 * over-estimates rows and pushes joins toward
							 * hashing (safe direction)
							 */
							{
								int64		key = ((int64) cst->consttype << 32) ^
									((int64) (uintptr_t) cst->constvalue &
									 0xFFFFFFFF);
								bool		found = false;

								foreach(lc, ctx->seen_keys)
								{
									if ((int64) (uintptr_t) lfirst(lc) == key)
									{
										found = true;
										break;
									}
								}
								if (!found)
								{
									ctx->seen_keys =
										lappend(ctx->seen_keys,
												(void *) (uintptr_t) key);
									entry->ndv++;
								}
							}
						}
					}
				}
			}
		}
	}

	return expression_tree_walker(node, cte_qual_walker, context);
}

List *
collect_cte_ndv_entries(Query *parse)
{
	CteNdvWalkContext ctx;
	List	   *kept = NIL;
	ListCell   *lc;

	ctx.parse = parse;
	ctx.entries = NIL;
	ctx.seen_keys = NIL;

	/* the top-level WHERE tree — q4's six CTE references live here */
	if (parse->jointree != NULL && parse->jointree->quals != NULL)
		cte_qual_walker(parse->jointree->quals, &ctx);

	/* keep entries with ndv >= 2 (single literals say nothing about ndv) */
	foreach(lc, ctx.entries)
	{
		CteColNdv  *e = (CteColNdv *) lfirst(lc);

		if (e->ndv >= 2)
			kept = lappend(kept, e);
	}
	ctx.entries = kept;

	return ctx.entries;
}
