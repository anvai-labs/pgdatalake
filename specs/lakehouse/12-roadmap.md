# 12 — Roadmap (M0 → M5)

Each milestone ends with the named acceptance gate; no milestone overlaps
another's gate. Rough sizing assumes 1–2 engineers + this agent.

## M0 — Repo & build foundation (days) — DONE 2026-11-19, tag `milestone/m0`
- [x] `main` as fork base (upstream `master` at fork creation `61f6021b110`,
      merge-synced with upstream per RULES §1 amendment)
- [x] Branch `lakehouse-dev` for integration work
- [x] Rust workspace `src/backend/access/lakehouse/core/` with empty `pgdatalake-core`
      crate, `cbindgen` header generation, meson feature `-Ddatalake=auto`
- [x] CI: build PG with feature on (ubuntu, cargo toolchain pinned), S3-compatible fixture (SeaweedFS, see doc 10) +
      Lakekeeper docker-compose fixture
- **Gate**: `meson setup build -Ddatalake=enabled && ninja && meson test --suite
  regress` green on CI (meson-only tree: autoconf `make check` cannot coexist
  with meson in the same checkout, and the upstream `parallel_schedule` suite
  runs unchanged under `meson test`); `SELECT lakehouse.version()`
  returns from a stub catalog module.

## M1 — Read path, external tables (2–4 weeks)
- REST catalog client (config/loadTable, OAuth2 + token refresh, SigV4)
- Metadata model v2+v3 (schema, snapshots, manifests, partition specs)
- MinIO/S3 file IO (read only), range GETs, footer parsing
- `lakehouse_fdw`: `CREATE FOREIGN TABLE`, projection + basic qual pushdown
  (partition & metrics pruning), parallel scans, batch→slot conversion
- Type matrix read-side (doc 05) incl. v2 eq/pos delete application
- **Gate**: conformance suite read-half green on the S3 fixture + REST; Trino-written table
  queryable; TPC-DS subset (q1–q10) runs against a 10GB fixture-S3 table with EXPLAIN
  showing file/row-group pruning.

## M2 — Managed tables, writes (3–5 weeks)
- `lakehouse` TableAM: CREATE/ALTER/DROP, `pg_am` bootstrap row + allowlist patches
- Write path: batch accumulator, Parquet writer, snapshot commit protocol with
  requirements + retry (doc 07 §2), overlay for own-writes visibility
- INSERT, UPDATE (COW), DELETE (COW), TRUNCATE, MERGE
- Managed type matrix incl. NOT NULL, defaults, schema evolution (add/rename/drop)
- **Gate**: PyIceberg reads our managed table (all types + partition transforms);
  round-trip write→read→update→read green; crash-point fuzz suite (doc 10 §4)
  shows zero partial-visibility violations.

## M3 — Semantics & cloud parity (2–4 weeks)
- `lakehouse.mvcc` models, RR/SERIALIZABLE conflict semantics, journal two-phase
  commit for non-REST catalogs, mixed heap+iceberg transactions
- ADLS Gen2 + GCS IO paths, full auth chains (doc 09), Azurite + fake-gcs parity
- Time travel (`FOR TIMESTAMP/VERSION/TAG`), branches/tags, rollback
- Logical-replication decision implemented (v1: clear error) + doc finalization
- **Gate**: CI-matrix green on all four stores; isolation tests for commit races;
  nightly cloud run green against real S3/ADLS/GCS.

## M4 — Maintenance & production hardening (2–3 weeks)
- Autovacuum integration, compaction (binpack+sort), expire_snapshots,
  remove_orphan_files, ANALYZE two-phase statistics
- `lakehouse.optimize` orchestrator (doc 08 §2.1, designed 2026-11-19):
  three modes — analyze (read-only stats: cardinality/skew/entropy/
  layout correlation per column), plan (dry run recommending tier,
  actions, partition transforms, zorder columns, estimated gains from
  measured data), exec (apply, observable outcomes) — with HBase-style
  minor (incremental binpack + delete application) and major (full
  re-layout) tiers; snapshot isolation + optimistic commit instead of
  table locks; auto mode off by default, policy-gated
- `pg_lakehouse` metadata views + table_health
- Glue catalog, Nessie native, REST profiles (Polaris/Unity/S3 Tables quirks)
- oss-fuzz metadata targets; perf gates (doc 10 §5) measured & passing
- **Gate**: 7-day soak with continuous mixed DML+maintenance on the S3 fixture — no
  invariant violations, compaction keeps small_files_pct < 30 under load; a
  major-tier `optimize` run on an intentionally de-optimized layout measurably
  restores pruning (files examined→tasks ratio ≥ 10×) and byte reduction.

## M5 — The seamless layer (ongoing)
- Deletion vectors as managed write mode (merge-on-read), bloom-filtered DV reads
- `metadata_only` plans (count/min/max from Parquet stats), metadata cache tuning
- variant type read/write, `lakehouse.map`/`struct` domain types (doc 05 v2)
- Logical decoding from commit journal; `lakehouse.migrate_table` GA
- Upstream PR extraction of `[core-allowlist]` patches (the fork's exit strategy:
  everything that can move upstream, does)

## Out-of-roadmap backlog (documented, unprioritized)
- Hive/JDBC catalogs, Lake Formation credential vending, v1 table reads,
  envelope encryption (v3), `ALTER TABLE EXECUTE` syntax, zorder compaction,
  Spark-connect-style remote plan pushdown, foreign-data-wrapper write batching
  across servers, pg_dump of managed tables (metadata-only dumps), tiered
  storage (S3 + cache disk).
