# 10 — Testing Strategy

## 1. Environment matrix

| Tier | Stores | Catalogs | Runs where |
|---|---|---|---|
| **CI-fast** (every commit, < 20 min) | MinIO (docker), file:// | REST (in-process Lakekeeper container), Hadoop on local FS | GitHub Actions |
| **CI-matrix** (nightly) | MinIO + Azurite + fake-gcs-server | REST × profiles (generic, Nessie REST, Polaris-style auth mock), Glue (emulated via moto), Hadoop | GitHub Actions nightly |
| **CI-cloud** (weekly / pre-release) | real S3, ADLS Gen2, GCS | real Glue, real Nessie, Polaris sandbox, S3 Tables | org cloud account, secrets via OIDC federation |

Azurite and fake-gcs-server are mandatory parity targets — cloud-only code paths
(auth, listing semantics, checksum headers) must be exercised in CI-fast, not
just in cloud runs.

## 2. Conformance suite (the acceptance bar)

- **Iceberg spec conformance**: port the official `iceberg-python`/`iceberg-go`
  integration test corpus: table lifecycle, all type round-trips, all partition
  transforms, schema evolution sequences, snapshot expiry, v2 position/equality
  delete application, v3 deletion vectors. Run the *same fixtures* written by
  PyIcepire/Spark and read by us, and vice versa (write by us, read by PyIceberg).
- **REST catalog conformance**: the REST spec's official conformance suite
  (namespacing, config resolution order, commit requirements, token refresh).
- **Interop gates (must pass before any release)**:
  - Trino reads a table we wrote (all types, partition transforms, after compaction).
  - PyIceberg reads our DVs (merge-on-read output).
  - We read a Spark-written table with eq-deletes.
  - We read a table written by Trino with zstd/lz4/gzip/snappy/none codecs.

## 3. PostgreSQL regression suites

- `src/test/regress/iceberg/` — SQL/expected pairs (join iceberg vs heap,
  EXPLAIN plans, time travel, type matrix, error cases with SQLSTATE checks).
- isolation tests (`src/test/isolation/iceberg/`) — concurrent DML commit races
  (AssertRefSnapshotId retry paths), RR pinning, overlay visibility, compaction
  vs DML.
- `TAP` tests for bgworker maintenance, crash recovery (kill -9 between commit
  stages from doc 07 §2, verify orphan/reconcile outcomes).

## 4. Property-based / adversarial

- **Crash-point fuzz**: instrumented fault injection at every object-store call
  (fail Nth request with 500/timeout/slowdown) — the recovery invariants from
  doc 07 must hold: no partial visibility, no unbounded retry storms, orphans
  always collectable.
- **Metadata fuzz**: malformed metadata.json / manifest / footer → clean ERROR
  with file path + json-pointer location, never a crash (libFuzzer targets in the
  Rust core for the serde paths; oss-fuzz job in M3).
- **Concurrency fuzz**: multi-client random DML/DDL/maintenance against one table
  via psql sessions, assert Iceberg spec invariants post-hoc with PyIceberg audit
  (every committed snapshot valid, no orphaned commits).

## 5. Performance gates (block release)

| Metric | Gate |
|---|---|
| Scan throughput 1-column predicate, 10GB table, MinIO local | ≥ 60% of DuckDB-on-S3 single-node baseline |
| Point INSERT commit latency (1 row, warm catalog) | < 150ms p50 against MinIO |
| 1000-batch bulk load (10M rows) | ≥ 50 MB/s/backend into MinIO |
| Compaction 500 small files → 50 | < 5 min, 8 workers |
| Parallel scan scaling | ≥ 5× on 8 workers vs 1 (task-parallel table) |
| Metadata planning on 2000-manifest table | < 300ms warm |

Regressions > 10% on two consecutive runs block merge; `perf/` directory holds
the harness (`make perf-iceberg`).

## 6. Developer loop

```bash
make iceberg-up          # docker compose: minio + lakekeeper + azurite + fake-gcs
make iceberg-check       # cargo test -p pgiceberg-core + regess iceberg subset
make iceberg-conformance # interop suite vs pyiceberg (uv-managed venv)
make iceberg-down
```
All fixtures in `src/test/iceberg/fixtures/`, no network at unit-test time.
