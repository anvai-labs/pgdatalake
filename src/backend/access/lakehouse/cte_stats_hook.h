/*---------------------------------------------------------------------------
 * cte_stats_hook.h — CTE-output statistics synthesis (spec 19 §6.5
 * family-C/E remedy, q4). See cte_stats_hook.c for the full story.
 *---------------------------------------------------------------------------
 */
#ifndef CTE_STATS_HOOK_H
#define CTE_STATS_HOOK_H

/* synthesized-distinct entry for one (CTE, column) pair */
typedef struct CteColNdv
{
	char	   *ctename;
	char	   *colname;
	AttrNumber	attnum;
	int			ndv;			/* distinct equality literals observed (>= 2) */
} CteColNdv;

extern void lakehouse_cte_stats_hook_init(void);
extern void lakehouse_cte_stats_hook_begin(List *entries,
										   MemoryContext plan_ctx);
extern void lakehouse_cte_stats_hook_end(void);
extern List *collect_cte_ndv_entries(Query *parse);

#endif							/* CTE_STATS_HOOK_H */
