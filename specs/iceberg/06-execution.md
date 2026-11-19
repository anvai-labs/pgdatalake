# 06 — Parquet Execution: Scans, Pushdown, Parallelism

## 1. Scan planning pipeline

```
SQL query
  → planner: iceberg_scan_plan(rel, quals, targetlist, sortclauses)
      1. resolve snapshot (statement snapshot pinning, doc 07)
      2. Iceberg Expression from quals (only pushdown-safe node classes)
      3. partition pruning: prune manifests by partition stats
      4. metrics pruning: per-column min/max/null-count/NDV in manifest entries
      5. row-group pruning: Parquet footer statistics per task
      6. task emission: list of ScanTask {data_file, delete_files[], residual}
  → executor: N parallel workers each take K tasks (K = scan_tasks_per_worker)
      worker: open files → arrow RecordBatches → TupleTableSlot (virtual, then
      minimal materialization only where PG demands it)
      residual expressions evaluated by Postgres (rest of quals re-checked
      generically — never trust pushdown alone)
```

**Never trust, always re-check**: any predicate handed to the core (metrics/
row-group pruning) is re-evaluated on returned rows. Pushdown bugs degrade
performance, never correctness.

## 2. What pushes down (v1)

| Pushdown | Mechanism | Applies to |
|---|---|---|
| Projection (column selection) | Parquet column readers only materialize needed columns | both kinds |
| Equality/range on partition columns | manifest partition-value filtering | both |
| Min/max/null-count on any column with metrics | manifest entry statistics | both |
| `IS NULL`/`IS NOT NULL` | null-count statistics | both |
| Row-group stats | footer min/max per chunk | both |
| LIMIT without ORDER BY | task-count short-circuit | both |
| LIMIT with matching sort order | Iceberg SortOrder-aware short-circuit (v2) | both |
| Aggregates min/max/count on metrics (`iceberg.metadata_only`) | GUC-gated metadata-only plans answering from statistics (v2) | both |
| Joins → co-partitioned hash join (partition column equality) | distribution hint (v3) | both |

Not pushed down: text search, JSONB ops (unless variant — fast-follow), function
calls, volatile quals. Residual evaluation remains in Postgres in all cases.

## 3. Deletes strategy (UPDATE/DELETE/MERGE)

| Mode | Write | Read | When |
|---|---|---|---|
| `copy_on_write` (default for managed) | rewritten files per affected data file | plain reads, fast | OLTP-ish, few files touched |
| `merge_on_read` | v3 **deletion vectors** (Puffin DV blobs) for position deletes; equality deletes from other engines: read-side apply | reads apply DVs (bitmap build, bloom-filter assisted by file stats) | ETL-heavy, wide rows, many small updates |
| `equality_delete` (write-only) | emitted on UPDATE via `MERGE`-style when `write_mode = mor_eq` | read applies eq-deletes (slowest read) | interop with engines that expect eq-del semantics |

Default by table DDL; overridable per statement via `SET LOCAL
iceberg.write_mode`. v1 reads must apply **v2 position deletes, v2 equality
deletes, and v3 deletion vectors** from foreign tables (that's what other engines
produce); v1 managed writes produce DVs (v3) or COW only.

DV write format: Puffin blob with `deletion-vector-v1` spec (v3), referenced from
snapshot summary → manifest entries. Bloom-filter acceleration (table property
`write.dv.filter.bloom`), default on for tables > 1GB.

## 4. Slot & memory model

- Scan fills **virtual TupleTableSlots** from Arrow RecordBatches: per-column
  `Datum` extraction with Arrow→PG conversion kernels (zero-copy for by-reference
  types where layouts match: `int32/64`, `float32/64`, `bool`, `date32`,
  `utf8→text` pointer handoff only when Arrow buffers outlive the slot — they
  don't, so `text` is copied to PG palloc'd memory; acceptable v1, convert to
  batch-scoped detoast-style arena in v2).
- Batch size: `iceberg.scan_batch_rows` = 16384 default. Each batch is one
  `CHECK_FOR_INTERRUPTS()` window; long row-groups chunk at batch boundary.
- Worker memory bounded by `iceberg.scan_batch_rows × max_row_width` estimate;
  executor resource queue (`iceberg.work_mem_reserve`) optionally reserves
  `work_mem` per worker like a sort node (v2).
- Writers accumulate batches in `iceberg.write_buffer_mb` (default 256MB/backend)
  then spill-free roll files. Large single transactions targeting one file-set are
  streamed: no full-rowset materialization ever (this is the key difference from
  naive FDW INSERT loops).

## 5. Parallelism & EXPLAIN

- `Parallel Safe` AM scans: leader plans, workers claim tasks from a shared queue
  in the parallel DSM segment (task = data file + delete set; unit granularity
  `scan_tasks_per_worker` merges small files).
- Skew handling: repartition pass re-enqueues tasks when a worker finishes 80% of
  its claimed set (v2).
- EXPLAIN (ANALYZE on scans):
  ```
  Iceberg Scan on analytics.events  (snapshot 8995…317, branch main)
    Files: 412/1500 planned, 38 row-groups pruned by stats
    Partition pruning: 88/120 manifests kept
    Pushdown: cols [at,uid], quals [uid = $1 → partition col: no, metrics: yes]
    Deletes: 3 DVs, bloom-filtered, 12k rows suppressed
    Batches: 2048 × 16384 rows
  ```

## 6. Indexes

There are no B-tree indexes on Iceberg tables. The substitutes exposed as
"indexes" for the planner:
- **partition columns** (planner-aware, auto-detected from spec),
- **Parquet column statistics** (NDV/min/max/null-count),
- **Iceberg SortOrder** (range-scan benefit, merge-friendly compaction),
- v2: secondary index properties via Puffin `bloom` filters per column.

`CREATE INDEX` on a USING iceberg table → ERROR with hint listing the above.
`ANALYZE` refreshes pg_statistic from Parquet metadata + sample rows (doc 08).
