# 08 — Maintenance: Compaction, Snapshot Expiry, Orphan Cleanup, ANALYZE

Iceberg tables accumulate files and snapshots; without maintenance they degrade.
Postgres already has the right shape for this: autovacuum. We reuse its
infrastructure rather than inventing a scheduler.

## 1. Autovacuum integration

`pg_lakehouse_table` rows carry `last_maintenance_*` timestamps; the AM participates
in autovacuum's table selection:

| Trigger (default) | Action |
|---|---|
| dead-tuple estimate (from delete files/DVs) > `autovacuum_vacuum_scale_factor × reltuples` | compaction (rewrite affected files, apply deletes in place) |
| `n_inserted_batches` small-file ratio > 0.5 (files < 25% target size) | bin-pack compaction of small files |
| snapshot count > `lakehouse.snapshot_count_threshold` (default 200) | expire snapshots |
| orphan files older than grace period, daily per table | orphan removal |
| ANALYZE staleness per usual pgstat rules | statistics refresh |

Per-table overrides via reloptions: `lakehouse.compact_min_files`,
`lakehouse.compact_target_size`, `autovacuum_enabled = false` honored (maintenance
off for that table).

## 2. SQL-callable actions (also what autovacuum runs internally)

```sql
-- Compaction: bin-pack data files in partitions; apply DVs/eq-deletes in place
SELECT lakehouse.rewrite_data_files('public.sessions',
         where        => 'at >= ''2026-11-01''',   -- partition-scoped
         strategy     => 'binpack',                 -- | 'sort' (uses SortOrder) | 'zorder' (v2)
         target_size  => '128MB',
         min_input_files => 3,
         options      => '{"rewrite_all_delete_files": true}');

-- Snapshot expiry: drop snapshots older than X / keep last N / keep tags
SELECT lakehouse.expire_snapshots('public.sessions',
         older_than => now() - interval '30 days',
         retain_last => 10, retain_tags => true);   -- returns files removed + bytes

-- Orphan cleanup (crash residue): dry_run first, always
SELECT * FROM lakehouse.remove_orphan_files('public.sessions', dry_run => true);
SELECT * FROM lakehouse.remove_orphan_files('public.sessions', dry_run => false,
                                          older_than => now() - interval '3 days');
```

All three are the same Iceberg Actions semantics as Spark's
`rewriteDataFiles` / `expireSnapshots` / `removeOrphanFiles` — the acceptance test
is byte-identical outcomes when run against the same table state.

### 2.1 Layout optimization: automatic partition discovery + clustered writes

*(Added 2026-11-19, motivated by the M1 gate: an unpartitioned dsdgen
`store_sales` could not bounds-prune at all — every file spanned the whole
date range; an identity-partitioned rewrite pruned 18,230 of 18,240 files.
Layout is metadata, not physics — Iceberg partition values live in manifest
entries, so re-layouting is just a rewrite + snapshot commit.)*

`lakehouse.optimize` is the **orchestrator**: one call figures out on its own
what the table needs — vacuum-level cleanup, compaction, and re-layout —
following the HBase minor/major compaction model:

| Tier | Scope | What it runs | Interference |
|---|---|---|---|
| **minor** | recently-changed files only (small files, files carrying delete files/DVs) | bin-pack compaction + delete application in place | none — readers keep their pinned snapshot; concurrent writers only race at commit (optimistic, doc 07 retry) |
| **major** | entire table | full re-layout: partition spec change + zorder/sort rewrite (the `analyze_layout` suggestion) + delete application + small-file packing | advisory lock between optimizers; run in maintenance windows; readers still unblocked (snapshot isolation), writers may retry |

**Why no full-table lock**: snapshot isolation already gives better-than-lock
semantics — a rewrite commits a NEW snapshot and never mutates data a reader
is reading, so the only contention point is the final optimistic commit
(requirements + retry, doc 07). Locking the table for a multi-hour COW would
block writers for its whole duration to protect a microsecond commit.
`optimize` takes a `pg_advisory_lock` on the table OID for the *duration of
optimization* so two optimizers never race, and passes
`AssertRefSnapshotId`-style requirements so a concurrent DML commit fails
the optimizer's final commit into a bounded retry, never lost data.
Snapshot expiry (destructive to time travel) is NOT part of `optimize` by
default — the orchestrator reports expired-snapshot candidates and only
expires when the caller passes retention arguments.

```sql
-- Three modes, one entry point (mode => 'analyze' | 'plan' | 'exec'):
--
-- 'analyze' — read-only statistics pass: per-column cardinality, null
--   ratio, skew (z-score of histogram), entropy, layout correlation
--   (how well value ranges align with current file boundaries), file
--   size/delete-ratio profile. No recommendations attached.
--
-- 'plan' — the dry run: runs the analysis pass, then recommends the
--   full maintenance plan, chosen from measured data — not fixed
--   heuristics:
SELECT * FROM lakehouse.optimize('public.store_sales', mode => 'plan');
--   tier            minor|major            (from small-file ratio, delete
--                                           ratio, layout score)
--   actions         [apply_deletes, binpack, repartition, zorder]
--   partition_by    suggested transforms per column (identity for
--                   low-cardinality, temporal by value spread, bucket[N]
--                   for high-cardinality join keys, truncate[L] for strings)
--   sort_by         zorder over top-k columns ranked by
--                   predicate-frequency × pruning selectivity ×
--                   cardinality-below-explosion-limit, with skew/entropy
--                   flags
--   estimated_gain  files examined→tasks ratio, bytes-after (encoding
--                   win), est. duration; partition count capped by
--                   lakehouse.optimize_max_partitions (default 10k)
--
-- 'exec' — apply: runs the same plan for real (or the explicitly given
--   parameters), reporting bytes-before/after and files examined→tasks
--   so every claim the plan made is observable afterward.
SELECT lakehouse.optimize('public.store_sales', mode => 'exec');
-- Explicit control stays available; strategy => 'auto' defers to the
-- planner; mode => 'plan' output can be fed back verbatim as arguments.
SELECT lakehouse.optimize('public.store_sales',
         mode => 'exec',
         tier => 'major',
         partition_by => ARRAY['day(ss_sold_date_sk)'],
         sort_by      => ARRAY['zorder(ss_store_sk, ss_item_sk)'],
         strategy     => 'auto');
-- 'analyze' is also exposed standalone for dashboards:
SELECT * FROM lakehouse.analyze_layout('public.store_sales');
```

Design rules:

- **Everything rides on Iceberg-native machinery**: the chosen partition
  spec and sort order are table metadata (`partition-specs` /
  `sort-orders`), and every tier is a COW rewrite committed as a new
  snapshot (M2 write path + maintenance worker). No core changes.
- **Encoding efficiency is a side effect of clustering, not a separate
  pass**: sorting/z-ordering concentrates similar values, so parquet
  RLE/dictionary/delta encodings compress tighter AND row-group-level
  min/max stats become discriminating (row-group pruning on top of
  file pruning). Minor optimize gets this within rewritten files; major
  optimize gets it table-wide.
- **Automatic mode is a policy, not magic**: `lakehouse.optimize_auto =
  off` by default. When on, the maintenance worker (§1 triggers) runs the
  dry-run plan after compaction and applies minor tier automatically;
  major tier applies only when the estimated pruning ratio exceeds
  `lakehouse.optimize_auto_min_gain` (default 10× files examined →
  tasks), during low-traffic windows only.

### 2.2 Research grounding & cross-engine lessons

*(Added 2026-11-19; the design above validated against the literature and
against sibling-engine practice.)*

**Cost-model-driven compaction policy.** The LSM compaction literature
(Dostoevsky, SIGMOD 2017; Spooky, VLDB 2022; Sarkar's compaction-design
work) establishes that compaction scheduling must come from an explicit
cost model trading write amplification against read cost, not fixed
thresholds. Applied here: the `plan` mode's tier choice (minor vs major)
is derived from a measured amplification model — bytes to rewrite vs
expected read-pruning gain under the observed predicate mix — rather than
magic ratios; the `optimize_auto_min_gain` ratio is the first
approximation and the model refines it.

**Hilbert vs Z-order.** Faloutsos & Jagadish (analysis of the Hilbert
space-filling curve) and later analytical comparisons prove Hilbert
preserves multi-dimensional locality better than Z-order bit
interleaving — a 2D range query touches one Hilbert cluster but two
Z-order clusters, and the gap widens with dimensionality (Hudi/Delta
practitioner results agree). Design consequence: `sort_by` accepts both
`zorder(cols)` (cheap) and `hilbert(cols)` (better locality); the
planner prefers `hilbert` when the workload's predicate dimensionality
is ≥ 3, `zorder` below that or when CPU is the binding constraint.

**Reclustering decoupled from transactions, policy decoupled from key
selection.** Snowflake (SIGMOD 2016) runs reclustering as a background
service separated from DML — validating our maintenance-worker design —
and recent work on workload-aware incremental reclustering argues for
separating the clustering-key SELECTION from the reclustering POLICY.
That is exactly the `analyze_layout` / `plan` split: key selection is a
statistics problem, scheduling is a policy problem with its own GUCs.

**Encoding selection is per-column-chunk, and layout choice is a cost
model.** The empirical columnar-format evaluation (Zeng et al.,
VLDB 2024) and the BtrBlocks line show greedy per-chunk encoding
selection beats fixed encodings; Durner et al. (VLDB 2023) give an
object-store cost model where optimal file size and parallelism derive
from GET costs and I/O patterns. Design consequence: `optimize`'s
target-file-size suggestion comes from a GET-cost model (fixture- and
cloud-profile aware), not a constant; per-column-chunk parquet encoding
selection is a flagged M5 extension on the writer, not part of optimize.

**Cross-engine lessons adopted from a sibling columnar engine
(proximaDB)** — validated against its `UnifiedColumnarCompaction` /
`write_fence` / sidecar machinery:
- *Post-rewrite continuity assertion*: after any optimize rewrite,
  assert manifest sequence-number continuity and snapshot-graph
  integrity (analog of VersionContinuityMode Strict) — part of the exec
  mode's post-commit validation, matching the doc 10 §4 PyIceberg audit.
- *Sidecars migrate and validate atomically with their data files*
  (codebook-sidecar pattern): our DVs (puffin blobs referenced from
  manifests) and any bloom sidecars must be rewritten/validated in the
  same commit as the files they annotate; optimize must never leave a
  sidecar pointing at a rewritten-away file.
- *Ship-dark gating*: auto mode ships OFF and is byte-identical when
  off (write-fence discipline); enabling it changes scheduling, never
  semantics.
- *Deterministic rewritten-file naming*: data files produced by
  optimize encode table-UUID + origin so orphan GC can classify
  residue without catalog access.
- *Layering*: optimize is a thin orchestrator over `rewrite_data_files`
  (the one compaction core), exactly as engine wrappers delegate to the
  unified columnar compactor.

## 3. Safety rules

- Orphan removal refuses files newer than `orphan_grace_period` (default 3d,
  minimum enforced 1h) — an in-flight commit from a slow writer must never be
  deleted. **This is the one operation that can destroy data if wrong**; dry-run
  report lists paths + last-modified + estimated recoverability.
- Snapshot expiry never expires the current ref snapshot, tagged snapshots
  (unless `force`), or snapshots referenced by in-progress Postgres transactions
  (checked against the local `pg_lakehouse_table` pinned-snapshot registry).
- Compaction reads at the current snapshot, writes new files, commits
  atomically — the same optimistic protocol as DML (doc 07). A compaction that
  loses the commit race retries (up to 3) then reports; it never blocks user
  DML longer than that.
- Maintenance bgworkers are cancellable, capped at
  `lakehouse.maintenance_queue_concurrency` concurrent tables per server, and never
  run on a standby.

## 4. Statistics (ANALYZE)

Two-phase, cheap first:
1. **Metadata phase** (always, no data read): from Parquet footers + manifest
   stats — row counts, min/max/null-count/NDV-per-file aggregated per column, per
   partition. Populates `pg_statistic` slots for planner estimates.
2. **Sample phase** (when NDV from metadata is unreliable or user sets
  `lakehouse.analyze_sample_rows`): reservoir-sample rows during a parallel scan.

`autovacuum_analyze_scale_factor` applies as usual. Because Parquet stats are
always present, `ANALYZE` on Iceberg tables is nearly free vs heap.

## 5. Table health surface

```sql
SELECT * FROM lakehouse.table_health('public.sessions');
-- avg_file_size_mb | small_files_pct | dv_rows_pending | eq_delete_files
-- snapshot_count | oldest_unexpired_snapshot | bytes_to_reclaim | last_maintenance
SELECT * FROM pg_lakehouse.maintenance_log WHERE relid = 'public.sessions'::regclass;
```

Alerting threshold guidance ships in docs: act when `small_files_pct > 30` or
`dv_rows_pending / reltuples > 0.1`.
