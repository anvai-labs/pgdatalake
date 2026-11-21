/*-------------------------------------------------------------------------
 * count_elision.c — P0b4 slice C (spec 15 §6.6): COUNT(*) elision for
 * MANAGED lakehouse tables via the planner hook (RULES §3 allowlist —
 * TableAMs have no aggregate hook of their own).
 *
 * A trivial `SELECT count(*) FROM <managed lakehouse table>` is answered
 * from the delta ledger: SUM(row_count) over UNPUBLISHED segments of the
 * current snapshot. Exactness contract (spec §6.6):
 *   - the ledger sum covers only staged-but-unpublished rows; if the
 *     table has ANY published segment (rows already in parquet) or any
 *     NULL row_count (pre-1.2 rows) the elision DECLINES — the normal
 *     scan answers instead
 *   - no quals, no joins, no grouping: anything else declines
 *   - the SPI read uses the current statement snapshot: MVCC-exact,
 *     concurrent publishers cannot skew a pinned read
 * Declining means the plan stays a normal scan — never fabricate.
 *
 * The elision runs OFFLINE (no catalog session): the ledger lives in
 * Postgres.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relscan.h"
#include "access/table.h"
#include "executor/spi.h"
#include "catalog/pg_class.h"
#include "commands/explain.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "optimizer/planner.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"

#include "lakehouse.h"
#include "lakehouse_tableam.h"

extern Oid	lakehouse_am_oid(void);

static planner_hook_type prev_planner_hook = NULL;

/* managed lakehouse AM OID, resolved once per backend */
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
 * Trivial count(*) over exactly one managed lakehouse base relation,
 * no quals/joins/group/having/window/distinct/limit-offset?  On match,
 * returns the base rel's OID.
 */
static bool
elide_count_wanted(Query *parse, Oid *relid_out)
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
	if (!agg->aggstar || agg->aggdistinct != NIL || agg->aggfilter != NULL ||
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
		rte->relid == InvalidOid)
		return false;
	if (!managed_lakehouse_rel(rte->relid))
		return false;

	if (relid_out != NULL)
		*relid_out = rte->relid;
	return true;
}

/*
 * Ledger COUNT for `relid`: SUM(row_count) over UNPUBLISHED segments via
 * the current statement snapshot (MVCC-exact). Declines when the table
 * has published segments or any NULL row_count (pre-1.2 rows) — those
 * need the real scan.
 */
static bool
ledger_count(Oid relid, int64 *count_out)
{
	int			ret;
	int64		published = 0;
	int64		unpublished = 0;
	bool		has_null = false;
	TupleTableSlot *slot;
	bool		isnull_p,
				is_null,
				isnull_s;

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
	if (ret != SPI_OK_SELECT || SPI_processed != 1)
	{
		PopActiveSnapshot();
		SPI_finish();
		return false;
	}
	published = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
											SPI_tuptable->tupdesc, 1,
											&isnull_p));
	has_null = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],
										  SPI_tuptable->tupdesc, 2,
										  &is_null));
	unpublished = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
											  SPI_tuptable->tupdesc, 3,
											  &isnull_s));
	PopActiveSnapshot();
	SPI_finish();

	/* any published segment or NULL row_count -> decline */
	if (published > 0 || has_null)
		return false;
	*count_out = unpublished;
	return true;
}

/*
 * Rewrite the parse tree in place into `SELECT const::int8` — the
 * planner then produces a bare Result node (no scan at all).
 */
static void
replace_with_const(Query *parse, int64 total)
{
	Const	   *c = makeConst(INT8OID, -1, InvalidOid, sizeof(int64),
							  Int64GetDatum(total), false, true);
	TargetEntry *te = linitial_node(TargetEntry, parse->targetList);

	te->expr = (Expr *) c;
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
lakehouse_count_elision_planner(Query *parse, const char *query_string,
								int cursorOptions, ParamListInfo boundParams,
								ExplainState *es)
{
	Oid			relid = InvalidOid;
	int64		total;

	elog(DEBUG1, "lakehouse count-elision: hook entered");
	if (elide_count_wanted(parse, &relid))
	{
		if (ledger_count(relid, &total))
		{
			elog(DEBUG1, "lakehouse count-elision: eliding relid %u = %lld",
				 relid, (long long) total);
			replace_with_const(parse, total);
		}
		else
			elog(DEBUG1, "lakehouse count-elision: ledger declined");
	}

	if (prev_planner_hook != NULL)
		return prev_planner_hook(parse, query_string, cursorOptions,
								 boundParams, es);
	return standard_planner(parse, query_string, cursorOptions, boundParams, es);
}

void
install_lakehouse_count_elision(void)
{
	prev_planner_hook = planner_hook;
	planner_hook = lakehouse_count_elision_planner;
}
