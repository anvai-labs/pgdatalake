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
row-group pruning) is re-evaluated on returned rows.

*(13-review #4 correction, 2026-11-20.)* The earlier claim — "pushdown bugs
degrade performance, never correctness" — is **wrong** and was demonstrated
live: a pushed predicate that mis-prunes eliminates FILES before any local
recheck can run, and re-checks only see surviving rows. Pushdown bugs can
silently lose rows. The pushdown path therefore admits only operators,
types, and collations whose semantics the byte-order pruner can model:

- **Operator identity, not spelling**: only pg_catalog operators push
  (`OPEROID` → `oprnamespace`); a user operator that shares the name
  `=`/`<` has different semantics and stays local.
- **Type allowlist**: int2/int4/int8 (exact binary), date (+10957 days),
  timestamp/timestamptz (+946684800000000 µs epoch), float4/float8
  (shortest round-trip out; NaN/Inf stay local), text/varchar/name.
  Everything else stays local — pushdown eligibility must not call
  arbitrary user-defined output functions. bpchar is excluded (its
  trailing-blank-insensitive equality is not byte equality).
- **Collation rules**: ordering (`< <= > >=`) pushes only under
  byte-ordered collations (C); equality (`=`) pushes only under
  deterministic collations. Otherwise the qual stays local — locale
  ordering (ICU en-US) and non-deterministic equality (case-folded)
  match rows byte comparisons reject.
- **Safe encoding**: values and field names are JSON-encoded with a
  growable buffer (quote, backslash, `\b\f\n\r\t`, other control bytes
  as `\u00XX`) — control characters in a constant previously produced
  JSON the Rust decode rejected outright.
- **Live acceptance** (Lakekeeper + S3 fixtures, ground truth via
  identical heap tables, two disjoint-range files with real bounds):
  inverted same-named operator, ICU ordering, non-deterministic eq and
  control-char eq — each previously returned wrong results or failed
  the scan; all now equal ground truth, while deterministic-eq and
  C-ordered pushdowns still prune correctly.

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
| Aggregates min/max/count on metrics (`lakehouse.metadata_only`) | GUC-gated metadata-only plans answering from statistics (v2) | both |
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
lakehouse.write_mode`. v1 reads must apply **v2 position deletes, v2 equality
deletes, and v3 deletion vectors** from foreign tables (that's what other engines
produce); v1 managed writes produce DVs (v3) or COW only.

*(Slice 20 amendment, 2026-11-19 — read-side v2 delete application implemented
in pgdatalake-core.)* Plan separates delete files (manifest `content` 1/2)
into `delete_tasks` — they are never planned as data scans. Delete files
load at scan planning (eager; small relative to data) into
`LoadedDeletes`, sliced per data file at row-read time:

- **Position deletes**: exact per-file application — absolute row positions
  keyed by the delete file's `file_path` column; file-scoped (no sequence
  rule, per spec).
- **Equality deletes**: applied to data files with a strictly lower
  sequence number than the delete file (Iceberg v2 applicability).
  M1 scope: **unpartitioned data files only** — a partitioned data file
  under an eq-delete snapshot errors loudly (partition-scoped application
  is a later slice; silently applying across partitions could over-delete).
- **v3 deletion vectors**: later slice (Puffin DV blobs; read half of the
  gate matrix) — slices V1/V3 in specs/lakehouse/14-v3-conformance.md.
  *(2026-11-20 v3 pass: until they land, v3 tables are REJECTED loudly at
  scan time — scan_table's format-version gate, doc 14 §3 V0 — and a
  manifest entry with an unknown `content` value (e.g. a DV entry) fails
  the manifest decode loudly. The read-correctness rule "never misread"
  applies: unimplemented delete constructs error, they never approximate.)*

*(13-review #5 amendment, 2026-11-20 — delete-specific pruning and
manifest inheritance.)* Predicate pruning is content-aware:

- **Data files** prune by partition applicability, then column min/max
  bounds.
- **Delete files** prune by **partition applicability only**. A delete
  file's bounds are over its delete keys, not the surviving rows; applying
  the data-bounds rule could eliminate a delete task whose deletions are
  still needed (position deletes carry no key bounds at all). Eliminating
  a delete task requires a proven delete-specific rule; partition
  applicability is that rule (a delete file touches only data files in
  its own partition).

*(13-review #4 planner amendment, 2026-11-20 — loud metadata errors and
field-ID partition matching.)*

- A manifest-list entry naming a partition spec the table no longer lists
  is a **loud decode error**. The old silent fallback to the default spec
  could prune against the wrong spec (evolved partitions), and local
  rechecks cannot recover what the planner already eliminated. Tolerance:
  a table with NO partition specs at all is unpartitioned — its spec-0
  references resolve to the implicit empty spec (provably cannot
  mis-prune).
- With a predicate and no current schema, planning **errors loudly**. The
  old per-file `continue` silently discarded every file, producing a
  plausible empty result.
- Identity-partition matching **proves the spec field's `source-id` equals
  the predicate column's schema field ID** before comparing values. A
  display name reused across schema evolution (drop + re-add with a new
  ID) must not compare the new column's values against the old column's
  partition member. Inconclusive cases keep the file.

Manifest-entry decode inherits a **null `snapshot_id`** from the
enclosing manifest's `added-snapshot-id` Avro container metadata
(older writers omit the entry-level ID). A null with no container
metadata to inherit from is a loud decode error, never a silent 0.
Schema list/map models retain **nested field IDs** (`element-id`,
`key-id`, `value-id`) and requiredness flags — required on the wire;
their absence fails the parse loudly since columns bind by ID through
schema evolution.

Interop note: PyIceberg 0.12 `table.delete()` performs COW rewrites (no
delete files); MoR delete fixtures for live verification come from
Spark/Trino in the M1 conformance gate, or from our own M2 writer.

*(M2 slice 28 amendment, 2026-11-20 — managed UPDATE/DELETE shipped as COW.)*
Managed-table DELETE/UPDATE record tombstones (base file + row index) and
PRE-COMMIT rewrites each affected file into the overwrite snapshot; the
originals become DELETED entries carried via MANIFEST REWRITE (doc 07 §2 —
pyiceberg ≤0.12 does not apply cross-manifest deletions, so a DELETED entry
in the new manifest alone resurrects the file for independent readers).
Live acceptance: mixed INSERT/UPDATE/DELETE transaction with in-transaction
read-your-writes, pyiceberg agreement after commit, subxact-rollback
tombstone pruning, and time-travel snapshots intact.

DV write format: Puffin blob with `deletion-vector-v1` spec (v3), referenced from
snapshot summary → manifest entries. Bloom-filter acceleration (table property
`write.dv.filter.bloom`), default on for tables > 1GB.

## 3a. Typed batch read ABI (M3-R1, DONE 2026-11-20)

*(13-review perf #3 landed: the per-cell hot path — one CString per cell
in Rust, one `InputFunctionCall` per value in C — is gone from both scan
consumers.)*

- `pg_datalake_rows_batch_next` advances one Arrow batch and returns
  typed per-column views (`Bool` u8, `Int64`/`TimestampUs`/`Date32` i64,
  `Float64` f64, zero-copy `Text`/`Bytes` spans, `Rendered` spans for
  date/time/decimal/uuid) plus a live-row list and a byte-per-row null
  map. Deletes (position/equality/DV) stay applied in Rust via the live
  list; the C side never sees dead rows.
- `batch_fill.c` maps each `(kind, atttypid, typmod)` to an exact fast
  Datum constructor; anything not provably identical to the old
  render+parse path (bpchar padding, name truncation, timestamp
  precision < 6 rounding, numeric widths, float4 double-rounding edge)
  falls back to `pg_datalake_rows_batch_cell_text` + `InputFunctionCall`
  — byte-identical text to the old renderer. Never a misread.
- **Row identity law:** `live_idx` is batch-LOCAL (a buffer index); the
  tombstone/TID identity is `start_abs + live_idx[k]` (position deletes
  are file-scoped, doc 07). Conflating the two silently retargeted
  tombstones at wrong rows in every batch after the first — caught by
  live multi-batch DML verification in this slice (see the incident
  note in 13-review §Perf-3); the ABI now carries `start_abs` and the
  consumers split local/absolute by construction.
- Lifetimes: batch pointers are valid until the next batch call or
  close; varlena results are palloc'd per row (same lifetime model as
  the input functions they replaced).
- Measured (500K×4-col managed table, same hardware): aggregate scan
  465 → 145 ms (~3.2×); full row materialization 0.69 → 0.31 s; FDW
  aggregate process-wall 0.51 → 0.18 s (~2.9×). Remaining hot-path cost
  is heap_form_tuple + executor work, not transport.

## 4. Slot & memory model

- Scan fills **virtual TupleTableSlots** from Arrow RecordBatches: per-column
  `Datum` extraction with Arrow→PG conversion kernels (zero-copy for by-reference
  types where layouts match: `int32/64`, `float32/64`, `bool`, `date32`,
  `utf8→text` pointer handoff only when Arrow buffers outlive the slot — they
  don't, so `text` is copied to PG palloc'd memory; acceptable v1, convert to
  batch-scoped detoast-style arena in v2).
- Batch size: `lakehouse.scan_batch_rows` = 16384 default. Each batch is one
  `CHECK_FOR_INTERRUPTS()` window; long row-groups chunk at batch boundary.
- Worker memory bounded by `lakehouse.scan_batch_rows × max_row_width` estimate;
  executor resource queue (`lakehouse.work_mem_reserve`) optionally reserves
  `work_mem` per worker like a sort node (v2).
- Writers accumulate batches in `lakehouse.write_buffer_mb` (default 256MB/backend)
  then spill-free roll files. Large single transactions targeting one file-set are
  streamed: no full-rowset materialization ever (this is the key difference from
  naive FDW INSERT loops).

## 5. Parallelism & EXPLAIN

- `Parallel Safe` AM scans: leader plans, workers claim tasks from a shared queue
  in the parallel DSM segment (task = data file + delete set; unit granularity


*(13-review #7 amendment, 2026-11-20 — per-node queues + pinned-snapshot
workers.)* Parallel coordination is per-executor-node — never a process
global (two scans in one query claimed from or cleared one another's
queue). The stable-task-mapping contract: the LEADER's Begin resolves the
concrete snapshot (per-execution — the plancache-fresh boundary; named
refs and "current" resolve once, here) and InitializeDSM records it;
each worker, whose own Begin may have resolved a different current
snapshot under a concurrent commit, RE-PINS its session to the leader's
id when they differ. Every process plans the same immutable snapshot, so
task index N means the same file everywhere. The partial path is created
only when `consider_parallel` holds (path invariant). Live acceptance:
serial == parallel(2 workers) == leader-participation-off multisets
(self-join, two distinct foreign scans, serial×parallel mix, correlated
rescan), each row exactly once; the pre-fix leader-off run duplicated
rows 4× (workers never received the shared queue — Begin runs before the
DSM hooks in workers too), caught and fixed by this matrix.  `scan_tasks_per_worker` merges small files).
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

`CREATE INDEX` on a USING lakehouse FORMAT iceberg table → ERROR with hint listing the above.
`ANALYZE` refreshes pg_statistic from Parquet metadata + sample rows (doc 08).
