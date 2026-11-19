# 11 — Related Work & Prior Art

What exists, what we learn from each, and why this fork's design differs.

| Project | Approach | Take |
|---|---|---|
| **pg_lake** (Snowflake, 2026 open-sourced) | Postgres extension; DuckDB-powered reads/writes of Iceberg + files in S3 | Validates demand. Embeds a whole engine: heavy, license-bound (MIT now), execution semantics diverge from PG planner. We keep PG as the executor. |
| **pg_duckdb** (Hydra/DuckDB) | DuckDB embedded as an executor extension | Great interop hammer, but DuckDB owns the scan; pushdown/AM integration is black-box. Our Rust-core approach gets the same Parquet library quality (arrow-rs is DuckDB-grade) without a second planner. |
| **pg_mooncake** | columnstore + object storage extension | Proves the TableAM-plus-object-store pattern works in-process. No Iceberg catalog integration. |
| **Amazon Aurora PostgreSQL** (2026) | managed direct query of Iceberg/Parquet in S3 | Confirms the external-table use case is the wedge feature. Closed; no managed-table story for on-prem. |
| **Broadcom PXF `iceberg_pxf_fdw`** | read-only FDW, Greenplum lineage | Read-only + JVM. Our FDW is native and read-write. |
| **DuckDB httpfs + iceberg extension** | read-only Iceberg scan, no catalogs | The reference for scan-planning simplicity; its manifest pruning design informs doc 06. |
| **Trino/Presto Iceberg connector** | the gold standard: full REST/Glue/HMS coverage, COW+MOR, compaction | Feature checklist source (their `system` metadata tables → our `pg_iceberg` views; their `ALTER TABLE EXECUTE` → our function surface). |
| **Apache Paimon / Hudi** | alternative lake formats | Not targets; but their merge-on-read durability bugs are cautionary reading for doc 07. |
| **Zero-stack option: postgres_fdw → Thrive/trino-gateway** | query Trino from PG | Two-engine latency + no managed tables. Explicitly what users try to escape. |

## Why not embed DuckDB (the pg_lake/pg_duckdb road)?

Considered seriously (fastest time to "SELECT * from iceberg works"):

1. **Two planners, one table** — types, predicates, permissions, statistics and
   EXPLAIN diverge between what PG thinks and what DuckDB does. Users discover
   this in prod (pg_lake's docs spend pages on exactly these caveats).
2. **License/maintenance mass** — embedding keeps a C++ engine ABI-compatible
   across our fork's releases; every PG major rebases it. arrow-rs is a library,
   not an engine: we compose primitives, own the planner story fully.
3. **Managed tables need PG-native writes** — overlays, triggers, RLS, logical
   semantics (doc 07) live in the PG executor; a foreign engine can't host them.
4. Where DuckDB genuinely wins (aggressive columnar scan), we compensate with
   vectorized batch slots + arrow-rs kernels; doc 06 gates performance against a
   DuckDB baseline so the gap is measured, not assumed.

## Why a fork, not an extension?

Allowed by both; fork chosen because:
- TableAM registration + grammar hooks (`FOR VERSION/TIMESTAMP AS OF`) and
  bootstrap catalog rows need core changes; an extension can shim some of it but
  ships `CREATE EXTENSION`-gated semantics that fight the "native engine" goal.
- The org (anvai-labs) wants opinionated defaults (iceberg-first GUCs, maintenance
  workers, bundled Rust core) that an extension would still have to document as
  "install the extension".
- Upstream delta stays small and extractable (RULES.md): every `[core-allowlist]`
  patch is a candidate upstream PR, so the fork remains a proposal, not a rift.
