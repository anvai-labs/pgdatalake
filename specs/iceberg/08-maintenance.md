# 08 — Maintenance: Compaction, Snapshot Expiry, Orphan Cleanup, ANALYZE

Iceberg tables accumulate files and snapshots; without maintenance they degrade.
Postgres already has the right shape for this: autovacuum. We reuse its
infrastructure rather than inventing a scheduler.

## 1. Autovacuum integration

`pg_iceberg_table` rows carry `last_maintenance_*` timestamps; the AM participates
in autovacuum's table selection:

| Trigger (default) | Action |
|---|---|
| dead-tuple estimate (from delete files/DVs) > `autovacuum_vacuum_scale_factor × reltuples` | compaction (rewrite affected files, apply deletes in place) |
| `n_inserted_batches` small-file ratio > 0.5 (files < 25% target size) | bin-pack compaction of small files |
| snapshot count > `iceberg.snapshot_count_threshold` (default 200) | expire snapshots |
| orphan files older than grace period, daily per table | orphan removal |
| ANALYZE staleness per usual pgstat rules | statistics refresh |

Per-table overrides via reloptions: `iceberg.compact_min_files`,
`iceberg.compact_target_size`, `autovacuum_enabled = false` honored (maintenance
off for that table).

## 2. SQL-callable actions (also what autovacuum runs internally)

```sql
-- Compaction: bin-pack data files in partitions; apply DVs/eq-deletes in place
SELECT iceberg.rewrite_data_files('public.sessions',
         where        => 'at >= ''2026-11-01''',   -- partition-scoped
         strategy     => 'binpack',                 -- | 'sort' (uses SortOrder) | 'zorder' (v2)
         target_size  => '128MB',
         min_input_files => 3,
         options      => '{"rewrite_all_delete_files": true}');

-- Snapshot expiry: drop snapshots older than X / keep last N / keep tags
SELECT iceberg.expire_snapshots('public.sessions',
         older_than => now() - interval '30 days',
         retain_last => 10, retain_tags => true);   -- returns files removed + bytes

-- Orphan cleanup (crash residue): dry_run first, always
SELECT * FROM iceberg.remove_orphan_files('public.sessions', dry_run => true);
SELECT * FROM iceberg.remove_orphan_files('public.sessions', dry_run => false,
                                          older_than => now() - interval '3 days');
```

All three are the same Iceberg Actions semantics as Spark's
`rewriteDataFiles` / `expireSnapshots` / `removeOrphanFiles` — the acceptance test
is byte-identical outcomes when run against the same table state.

## 3. Safety rules

- Orphan removal refuses files newer than `orphan_grace_period` (default 3d,
  minimum enforced 1h) — an in-flight commit from a slow writer must never be
  deleted. **This is the one operation that can destroy data if wrong**; dry-run
  report lists paths + last-modified + estimated recoverability.
- Snapshot expiry never expires the current ref snapshot, tagged snapshots
  (unless `force`), or snapshots referenced by in-progress Postgres transactions
  (checked against the local `pg_iceberg_table` pinned-snapshot registry).
- Compaction reads at the current snapshot, writes new files, commits
  atomically — the same optimistic protocol as DML (doc 07). A compaction that
  loses the commit race retries (up to 3) then reports; it never blocks user
  DML longer than that.
- Maintenance bgworkers are cancellable, capped at
  `iceberg.maintenance_queue_concurrency` concurrent tables per server, and never
  run on a standby.

## 4. Statistics (ANALYZE)

Two-phase, cheap first:
1. **Metadata phase** (always, no data read): from Parquet footers + manifest
   stats — row counts, min/max/null-count/NDV-per-file aggregated per column, per
   partition. Populates `pg_statistic` slots for planner estimates.
2. **Sample phase** (when NDV from metadata is unreliable or user sets
  `iceberg.analyze_sample_rows`): reservoir-sample rows during a parallel scan.

`autovacuum_analyze_scale_factor` applies as usual. Because Parquet stats are
always present, `ANALYZE` on Iceberg tables is nearly free vs heap.

## 5. Table health surface

```sql
SELECT * FROM iceberg.table_health('public.sessions');
-- avg_file_size_mb | small_files_pct | dv_rows_pending | eq_delete_files
-- snapshot_count | oldest_unexpired_snapshot | bytes_to_reclaim | last_maintenance
SELECT * FROM pg_iceberg.maintenance_log WHERE relid = 'public.sessions'::regclass;
```

Alerting threshold guidance ships in docs: act when `small_files_pct > 30` or
`dv_rows_pending / reltuples > 0.1`.
