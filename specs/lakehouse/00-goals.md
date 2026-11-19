# 00 — Goals, Non-Goals, Principles

## Goals

0. **Format-generic lakehouse, iceberg-first.** The shared core
   (`pgdatalake-core`) and the SQL surface (`lakehouse` schema, `USING
   lakehouse` TableAM with a `FORMAT` reloption, `pg_lakehouse` views,
   `lakehouse.*` GUCs) are table-format-agnostic: Delta Lake, plain
   Parquet/Avro and engine integrations (DataFusion, Polars) are future
   FORMAT layers, not rewrites. Iceberg-specific concepts never leak into
   generic naming. *(Added 2026-11-19 with the naming generalization.)*
1. **Iceberg as a native storage engine.** `CREATE TABLE … USING lakehouse FORMAT iceberg` stores the
   table as a real Apache Iceberg table (Parquet data files, `metadata.json`,
   manifests, snapshot protocol) in object storage. Any Iceberg engine (Spark, Trino,
   Flink, DuckDB, Snowflake) must be able to read tables this fork writes, and vice
   versa. **No proprietary on-disk format.**
2. **External table access.** Tables already registered in an Iceberg catalog
   (written by other engines) can be attached and queried with Postgres, including
   joins against local heap tables, with predicate/projection pushdown; INSERT /
   UPDATE / DELETE / MERGE supported on external tables that permit it.
3. **Postgres-native experience.** Indexes where they make sense (via Iceberg
   metadata + Parquet statistics — *not* local B-trees), `ANALYZE`/statistics,
   EXPLAIN visibility, psql introspection (`\d+`), information about snapshots,
   autovacuum-style background maintenance.
4. **Transaction semantics that are Postgres-honest.** Snapshot Isolation is the
   contract for Iceberg tables; every deviation from heap behavior (e.g., read
   visibility within a statement, cross-table commit atomicity) is documented and
   detectable, never silent.
5. **All four storage targets are first-class:** S3 (and S3-compatible: **MinIO**,
   R2 via endpoint override), Azure ADLS Gen2, GCS. Same feature set everywhere;
   MinIO is the default dev/test target.
6. **Upstream-forkable.** The delta against upstream postgres stays as small,
   reviewable, and rebase-friendly as possible (see [RULES.md](RULES.md)).
7. **Strict Iceberg conformance.** Target the current Iceberg **v3** table spec +
   REST catalog spec; implement v2 fallback (position/equality deletes) read-only,
   and v1 read-only.

## Non-goals (for v1)

- **No custom Parquet writer/reader from scratch** — use a proven library
  (arrow-rs / parquet crate, see [06-execution.md](06-execution.md)).
- **No query engine replacement.** The Postgres executor stays the executor; we do
  not embed DuckDB for general planning (embedding is evaluated as an alternative in
  [11-related-work.md](11-related-work.md) and rejected as primary path).
- **No Iceberg *catalog server* implementation.** We are a *client* of catalogs.
- **No v1 support for** Iceberg views, procedures, partition-prog SQL, encryption
  (v3 envelope encryption is a fast-follow), multi-table REST transaction commits on
  non-REST catalogs, row-level security on external tables (grant-level RBAC only).
- **No PG16/17 backports.** Master (PG20-dev) only; stable branches cut from tagged
  upstream releases later.
- **No write path to v1 tables.**

## Principles

1. **Conformance over convenience.** If Iceberg says so, we do that, even when a
   Postgres-native shortcut exists. Interop with Spark/Trino is the acceptance bar.
2. **Postgres first.** Iceberg tables are relations. They appear in `pg_class`,
   participate in `ANALYZE`, permissions, EXPLAIN, `pg_stat_*`. If a feature can be
   expressed through existing Postgres extension points (TableAM, FDW, hooks), we
   use them before patching core.
3. **Honest semantics.** Any place Iceberg behavior differs from heap (visibility,
   durability windows, conflict behavior) is surfaced in docs, `EXPLAIN`, and where
   feasible in SQL-visible state (e.g., `pg_lakehouse.snapshot_of(rel)`).
4. **Small core delta.** New code lives in `src/backend/access/lakehouse/` +
   `src/include/access/iceberg/` + a linked Rust core library; core-file patches are
   confined to a reviewed allowlist (see [RULES.md](RULES.md)).
5. **MinIO is the CI baseline.** Every feature must pass against MinIO first; cloud
   SDK behaviors are layered on top.
