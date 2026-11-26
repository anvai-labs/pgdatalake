/*
 * lakehouse_fdw.h — shared surface of the lakehouse FDW (specs/lakehouse
 * 06-execution.md). fdw.c = assembly (handler/validator/options/sessions),
 * fdw_plan.c = planning hooks + elision + predicate translation,
 * fdw_exec.c = execution hooks + parallel coordination.
 */
#ifndef LAKEHOUSE_FDW_H
#define LAKEHOUSE_FDW_H

#include "c.h"
#include "port/atomics.h"
#include "foreign/fdwapi.h"
#include "nodes/pathnodes.h"
#include "utils/palloc.h"
#include "pg_datalake_core.h"
#include "lakehouse.h"
#include "batch_fill.h"

/*
 * parallel-scan coordination state (slice 17, 13-review #7): the DSM
 * queue shared by leader + workers of one scan
 */
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
} LakehouseShared;

/*
 * per-executor-node state (owned by fdw_exec.c; planned data flows in
 * via fdw_private)
 */
/* per-executor-node state */
typedef struct LakehouseExecState
{
	LakehouseOptions opts;
	char	   *predicate_json; /* translated quals (slice 15) */
	char	   *rte_selector;	/* time travel pin from fdw_private (slice 19) */
	bool		parallel;		/* running as a parallel worker (slice 17) */
	ScanSession *session;
	RowsHandle *rows;
	PgDatalakeCacheStats cache_at_begin;	/* block-cache counters at Begin */
	uint64		current_task;
	uint64		task_count;
	char	   *projection_csv; /* visible attribute names, comma-joined */

	/*
	 * THIS node's parallel coordination (13-review #7): per-executor-node,
	 * never a process global — two scans in one query (self-join) must not
	 * claim from or clear each other's queues.
	 */
	LakehouseShared *shared;
	int			natts;
	FmgrInfo   *in_funcs;		/* per visible attribute */
	Oid		   *typioparams;
	int		   *slot_index;		/* attribute slot per projection column */

	/*
	 * M3-R1 typed batch cursor: one arrow batch at a time, Datums built
	 * directly (batch_fill.c); the per-row renderer is gone from this path.
	 * rows_next is no longer called here.
	 */
	PgDatalakeRowBatch batch;
	bool		batch_valid;
	uint64		batch_cursor;
	LakehouseBatchMap *maps;	/* per visible attribute */
	Datum	   *row_values;		/* reused per row */
	bool	   *row_nulls;

	/*
	 * Execution-time COUNT (spec 16 successor): one-row upper scan whose
	 * answer is resolved in Begin — manifest sum when the delete-free proof
	 * holds on the current snapshot, the ordinary-aggregate fallback
	 * otherwise.
	 */
	bool		count_exec;
	bool		count_done;
	bool		count_resolved;
	bool		count_from_manifest;
	int64		count_value;
	char	   *count_selector; /* descriptor from the plan (no count) */
	Oid			count_ftid;
} LakehouseExecState;

extern bool lakehouse_trivial_count_shape(Query *parse);

/* fdw.c */
extern void ExtractOptions(Oid foreigntableid, Oid checkAsUser, LakehouseOptions *opts);

/* fdw_plan.c */
extern void lakehouse_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
										Oid foreigntableid);
extern void lakehouse_GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
									  Oid foreigntableid);
extern void lakehouse_GetForeignUpperPaths(PlannerInfo *root,
										   UpperRelationKind stage,
										   RelOptInfo *input_rel,
										   RelOptInfo *output_rel,
										   void *extra);
extern ForeignScan *lakehouse_GetForeignPlan(PlannerInfo *root,
											 RelOptInfo *baserel,
											 Oid foreigntableid,
											 ForeignPath *best_path,
											 List *tlist,
											 List *scan_clauses,
											 Plan *outer_plan);

/* fdw_analyze.c */
extern bool lakehouse_AnalyzeForeignTable(Relation rel,
										  AcquireSampleRowsFunc *func,
										  BlockNumber *totalpages);
extern int	lakehouse_AcquireSampleRows(Relation rel, int elevel,
										HeapTuple *rows, int targrows,
										double *totalrows,
										double *totaldeadrows);

/* fdw_exec.c */
extern void lakehouse_BeginForeignScan(ForeignScanState *node, int eflags);
extern TupleTableSlot *lakehouse_IterateForeignScan(ForeignScanState *node);
extern void lakehouse_ReScanForeignScan(ForeignScanState *node);
extern void lakehouse_EndForeignScan(ForeignScanState *node);
extern Size lakehouse_EstimateDSMForeignScan(ForeignScanState *node,
											 ParallelContext *pcxt);
extern void lakehouse_InitializeDSMForeignScan(ForeignScanState *node,
											   ParallelContext *pcxt,
											   void *coordinate);
extern void lakehouse_InitializeWorkerForeignScan(ForeignScanState *node,
												  shm_toc *toc,
												  void *coordinate);
extern void lakehouse_ShutdownForeignScan(ForeignScanState *node);
extern bool lakehouse_IsForeignScanParallelSafe(PlannerInfo *root,
												RelOptInfo *baserel, RangeTblEntry *rte);
extern void lakehouse_ExplainForeignScan(ForeignScanState *node,
										 ExplainState *es);

#endif							/* LAKEHOUSE_FDW_H */
