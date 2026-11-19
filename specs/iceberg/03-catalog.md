# 03 — Iceberg Catalog Integrations

The catalog is the source of truth for table existence, schema, and current
snapshot. Postgres keeps only a **cache** of it (relcache entry per attached
table), never authoritative metadata, with one exception: managed tables carry a
`pg_iceberg_table` row so a dropped/renamed Postgres table can be reconciled
against the remote catalog at next refresh.

## 1. Priority order

| Tier | Catalog | v1 | Notes |
|---|---|---|---|
| 1 | **REST** (`rest`) | create/read/write | The universal target: Polaris, Unity Catalog, Nessie REST, S3 Tables, SigV4 Glue REST, Tabulario, Lakekeeper, Gravitino. Auto-profile via `GET /v1/config`. |
| 2 | **AWS Glue** (`glue`) | create/read/write | Native SigV4; Iceberg REST endpoints on Glue also reachable as `rest` profile; `glue` type exists because Lake Formation credentials differ. |
| 3 | **Nessie** (`nessie`, native API) | create/read/write | Branch/commit model maps beautifully onto our ref model; REST profile preferred when available. |
| 4 | **Hadoop/HMS** (`hadoop`, `hive`) | read-only (v1) | Legacy; sufficient for migration reads. |
| 5 | JDBC (`jdbc`) | deferred | Requires an embedded client; REST covers most JDBC deployments' equivalents. |

## 2. REST catalog conformance (the big one)

Implement the Iceberg REST openapi spec exactly:

- `GET /v1/config` → server capabilities, defaults (`defaults`, `overrides` config
  merge order: catalog `overrides` > table properties > `defaults` > client).
- OAuth2 client credentials flow (`/v1/oauth/tokens`) with proactive refresh at
  80% of token TTL; also accept bearer tokens, SigV4 signing (AWS REST profiles),
  and Azure Entra tokens (Polaris-on-ADLS deployments).
- Namespace ops: `listNamespaces`, `createNamespace` (map to Postgres schemas on
  managed side: one Iceberg namespace = one schema, enforced 1:1 at attach time).
- Table ops: `loadTable` (returns `metadata_location` + config), `createTable`,
  `renameTable`, `dropTable` (with purge semantics flag), `reportMetrics`.
- **Commit**: `commitTable` with `requirements` (AssertRefSnapshotId,
  AssertTableUUID, AssertSchemaUUID…) — every requirement type implemented, since
  this is our optimistic-concurrency instrument (doc 07).
- **Multi-table atomic commit**: `POST /v1/{ns}/transactions` (`CommitTransactionRequest`)
  implemented for catalogs advertising support; on non-supporting catalogs, our
  cross-table commit strategy degrades per doc 07 §4 (staged sequential commits with
  a transaction journal in `pg_iceberg_table`).
- View endpoints, `registerTable`, plan-table endpoints: v2.

Header/feature negotiation table lives in code as a
`RestProfile { polaris, unity, nessie_rest, s3tables, glue_rest, generic }` enum —
profile quirks (e.g., Polaris's namespace-flattening, S3 Tables' regional
endpoints, Unity's `catalog.schema.table` three-level namespacing) are data, not
code paths.

## 3. Namespace ↔ Postgres mapping

- Iceberg `namespace` levels: Glue/REST-generic have multi-level; Unity has 3
  (catalog.schema.table). Postgres has schema.table.
- Rule: **the last namespace level maps to the Postgres schema**; higher levels are
  folded into the catalog's `namespace_prefix` option
  (`namespace_prefix 'analytics'` → always within that namespace).
- Case policy: Iceberg identifiers are case-sensitive; Postgres folds unquoted to
  lower. CREATE/attach always quotes; `IMPORT FOREIGN SCHEMA` emits quoted
  identifiers, and name collisions resolved with `_N` suffix + NOTICE.

## 4. Caching & invalidation

- Per-table metadata cache keyed by `(catalog_oid, table_identity, branch)`;
  entries hold `TableMetadata` (parsed), manifest-list handle, and a `loaded_at`.
- TTL: `iceberg.metadata_ttl` (default 10s) **plus** proactive invalidation for
  managed tables (we know when we commit). External tables are eventually-consistent
  by design: an out-of-date snapshot read by Postgres is a documented consequence,
  mitigated by `AssertRefSnapshotId` on write attempts (we never write on a stale
  read snapshot — we re-plan instead).
- Memory: metadata cache is bounded by `iceberg.metadata_cache_mb` (default 256MB,
  shared via DSA), LRU eviction. Manifests are streamed, not retained, except
  during planning.

## 5. Failure semantics

| Failure | Behavior |
|---|---|
| Catalog unreachable at plan time | ERROR (fail fast); retryable at executor via `iceberg.commit_retries` only for commits |
| Catalog unreachable at commit time | Transaction stays open, commit retried with backoff; on exhaustion → ERROR, staged files orphaned, cleaned by orphan GC |
| Remote table deleted under us | `loadTable` 404 → rel marked invalid; SELECT errors `iceberg_table_dropped`, DDL hint to re-attach |
| Schema drift by another engine | On next metadata load: if schema is *compatible* (additive), transparently adapted; if incompatible (type narrowed / column dropped that we use) → ERROR with diff printout |
