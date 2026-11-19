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

Two surfaces on top of `rewrite_data_files`:

```sql
-- Analysis (read-only): score the current layout, propose a better one
SELECT * FROM lakehouse.analyze_layout('public.store_sales');
-- per column: cardinality, null_ratio, skew (z-score of value histogram),
-- entropy, and "layout correlation" — how well the column's value range
-- aligns with current file boundaries (i.e. how prunable it already is);
-- plus observed predicate frequencies when pg_stat_statements is loaded.
-- suggestions column proposes: partition spec (identity for
-- low-cardinality columns, temporal transforms by value spread, bucket[N]
-- for high-cardinality join keys, truncate[L] for strings), sort order
-- (zorder over the top-k correlated columns), target file size, and the
-- estimated pruning ratio under the observed predicate mix.

-- One-shot application (user-directed; dry_run reports the plan only)
SELECT lakehouse.optimize('public.store_sales',
         partition_by => ARRAY['day(ss_sold_date_sk)'],   -- transform syntax
         sort_by      => ARRAY['zorder(ss_store_sk, ss_item_sk)'],
         strategy     => 'auto',      -- | 'binpack' | 'sort' | 'zorder'
         dry_run      => true);
```

Design rules:

- **Everything rides on Iceberg-native machinery**: the chosen partition
  spec and sort order are table metadata (`partition-specs` /
  `sort-orders`), and the re-layout is a COW rewrite committed as a new
  snapshot (M2 write path + maintenance worker). No core changes.
- **Encoding efficiency is a side effect of clustering, not a separate
  pass**: sorting/z-ordering concentrates similar values, so parquet
  RLE/dictionary/delta encodings compress tighter AND row-group-level
  min/max stats become discriminating (row-group pruning on top of
  file pruning). `optimize` reports bytes-before/after so the win is
  observable.
- **Statistical scoring, not heuristics alone**: `analyze_layout` ranks
  candidate columns by (observed predicate frequency) × (pruning
  selectivity at file granularity) × (cardinality below the
  partition-explosion limit), with skew/entropy flags for sort candidates.
  Partition count per action is capped (`lakehouse.optimize_max_partitions`,
  default 10,000) to avoid small-file explosion.
- **Automatic mode is a policy, not magic**: `lakehouse.optimize_auto =
  off` by default. When on, the maintenance worker (§1 triggers) runs
  `analyze_layout` after compaction and applies suggestions whose
  estimated pruning ratio exceeds `lakehouse.optimize_auto_min_gain`
  (default 10× files examined → tasks), during low-traffic windows only.

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
