# PostgreSQL Lakehouse (`pgdatalake`) — Fork Design Specs

This directory contains the design specifications for making this PostgreSQL fork
(`anvai-labs/postgres`, tracking `postgres/postgres` master) a **native lakehouse**:
query and manage open table-format tables (external tables that already live in a
catalog, and internal managed tables whose on-disk format *is* the table format).
**Apache Iceberg is the first and initially only FORMAT**, implemented on the
format-generic `pgdatalake-core` Rust core; Delta, Parquet/Avro files and engine
integrations (DataFusion, Polars) are future FORMAT layers by design — hence the
generic `lakehouse` SQL surface chosen up front (2026-11-19; see
01-architecture.md, RULES.md §4).

## Goal

A user of this fork should be able to do all of the following with plain SQL:

```sql
-- 1. Point Postgres at an Iceberg catalog (REST, Glue, Nessie, Polaris, Unity, JDBC, Hadoop)
SELECT lakehouse.create_catalog('prod', type => 'rest', endpoint => 'https://rest.lakehouse.example.com/v1');

-- 2. Attach an EXISTING Iceberg table (external): full read, DML where the engine allows
CREATE FOREIGN TABLE analytics.events () SERVER prod OPTIONS (table 'warehouse.events');

-- 3. Create a MANAGED table stored natively as Iceberg on S3 / ADLS / GCS / MinIO
CREATE TABLE public.sessions (sid bigint, uid int, payload jsonb, at timestamptz)
  USING lakehouse FORMAT iceberg
  LOCATION 's3://anvai-warehouse/public/sessions'
  CATALOG prod;

-- 4. Mix both with local heap tables in transactions, joins, CDC consumers, etc.
-- 5. Let background maintenance (compaction, snapshot expiry) happen automatically.
```

Iceberg becomes a first-class storage engine of Postgres: catalog clients, file IO for
S3/ADLS/GCS/MinIO, a Parquet execution path, Postgres-native transaction semantics,
maintenance infrastructure, and type mapping — all behind the format-generic core so
later formats inherit them.

## Reading order

| Doc | Contents |
|---|---|
| [00-goals.md](00-goals.md) | Goals, non-goals, product principles |
| [01-architecture.md](01-architecture.md) | System architecture, layering, managed vs external, code layout |
| [02-sql-surface.md](02-sql-surface.md) | SQL statements, GUCs, reloptions, metadata views |
| [03-catalog.md](03-catalog.md) | Iceberg catalog integrations (REST, Glue, Nessie, …) |
| [04-storage.md](04-storage.md) | Object store file IO: S3, ADLS Gen2, GCS, MinIO |
| [05-types.md](05-types.md) | Postgres ↔ Iceberg type mapping and schema evolution |
| [06-execution.md](06-execution.md) | Parquet read/write, scan planning, pushdown, parallelism |
| [07-transactions.md](07-transactions.md) | MVCC, snapshot isolation, commit protocol, recovery |
| [08-maintenance.md](08-maintenance.md) | Compaction, snapshot expiry, orphan cleanup, ANALYZE |
| [09-security.md](09-security.md) | Credentials, auth per cloud, RBAC mapping |
| [10-testing.md](10-testing.md) | Test strategy: MinIO/Azurite/fake-gcs, regression suites |
| [11-related-work.md](11-related-work.md) | Prior art: pg_lake, pg_duckdb, Aurora, PXF, DuckDB Iceberg |
| [12-roadmap.md](12-roadmap.md) | Phased milestones M0–M5 with acceptance criteria |
| [RULES.md](RULES.md) | Fork discipline: branching, upstream sync, patch rules, code style |
| [HANDOFF.md](HANDOFF.md) | State snapshot for the next working session |

## Status

- 2026-11-19: fork created (private repo `anvai-labs/postgres`), specs drafted at
  PostgreSQL master `61f6021b110` (PG 20 dev, `CATALOG_VERSION_NO 202611121`).
- 2026-11-19: **naming generalized** — core `pgdatalake-core`, SQL surface
  `lakehouse` (`USING lakehouse FORMAT iceberg`); specs renamed from
  `specs/iceberg/` to `specs/lakehouse/` before M1 starts, to avoid a sweeping
  rename later. Upstream baseline: `make check` 243/243 green on master.
- M0 in progress: meson `-Ddatalake` feature, `pgdatalake-core` Rust workspace,
  `lakehouse` extension with `lakehouse.version()`, docker fixtures, CI.
  Start at [12-roadmap.md](12-roadmap.md) M0.
