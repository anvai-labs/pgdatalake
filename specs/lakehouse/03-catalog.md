# 03 — Iceberg Catalog Integrations

The tier table below describes protocol design goals. Advertised operations require a [profile receipt](modules/09-cloud-profiles.md). For ambiguous commit outcomes use the [single-send recovery contract](modules/04a-publisher-recovery.md).

The catalog is the source of truth for table existence, schema, and current
snapshot. Postgres keeps only a **cache** of it (relcache entry per attached
table), never authoritative metadata, with one exception: managed tables carry a
`pg_lakehouse_table` row so a dropped/renamed Postgres table can be reconciled
against the remote catalog at next refresh.

## 1. Delivery order

| Track | First capability | Release dependency |
|---|---|---|
| Reference Iceberg REST | Read-only FDW on Lakekeeper. Pin the catalog, storage and independent engine used in the fixture. | Complete the self-hosted read pilot and keep the Lakekeeper publication and recovery test separate. |
| Additional REST profiles | Begin with Apache Polaris on the same object-store fixture. Use one profile each for Nessie REST, Unity Catalog OSS and managed provider services. | The same read oracle plus each provider's route, authorization, credential and table-class controls. |
| Hive Metastore and Hadoop | Read-only migration and reconciliation for an already-published Iceberg table. | Use a supported Iceberg REST bridge or explicitly migrate the table using Iceberg tooling. Direct legacy clients require a separate demand and authorization gate. |
| Additional protocols | Demand-driven follow-up, including native Nessie APIs and JDBC. | Identify a concrete operation that the REST or migration path cannot serve. |

The [catalog profile roadmap](modules/09-cloud-profiles.md#catalog-expansion-after-lakekeeper) assigns ownership, distinct profiles and promotion tests. The [Iceberg REST specification](https://iceberg.apache.org/rest-catalog-spec/) is the protocol baseline.

## 2. REST catalog conformance (the big one)

Implement the Iceberg REST openapi spec exactly:

- Where the profile uses `GET /v1/config`, allowlist the returned configuration fields.
  Keep the operator-enrolled authority, expected UUID, permitted ref and storage roots
  authoritative. The bound Lakekeeper pilot uses a preconfigured route and storage
  settings. A profile that vends credentials needs an explicit trust and expiry contract.
- OAuth2 client credentials flow (`/v1/oauth/tokens`) with proactive refresh at
  80% of token TTL; also accept bearer tokens, SigV4 signing (AWS REST profiles),
  and Azure Entra tokens (Polaris-on-ADLS deployments).
- Namespace ops: `listNamespaces`, `createNamespace` (map to Postgres schemas on
  managed side: one Iceberg namespace = one schema, enforced 1:1 at attach time).
- Table ops: `loadTable` (returns `metadata_location` + config), `createTable`,
  `renameTable`, `dropTable` (with purge semantics flag), `reportMetrics`.
- **Commit**: `commitTable` with `requirements` (AssertRefSnapshotId,
  AssertTableUUID, AssertSchemaUUID…) — every requirement type implemented, since
  this is our optimistic-concurrency instrument (doc 07). Wire contract
  (verified against pyiceberg 0.12, the interop oracle): requirements
  discriminate on `"type"` (`assert-table-uuid`, `assert-ref-snapshot-id` with
  `snapshot-id` ALWAYS present — null means "ref must not exist"), updates
  discriminate on `"action"` (`add-snapshot`, `set-snapshot-ref` with
  `ref-name`/`type`/`snapshot-id`). A typed definitive rejection permits a compatible replan under the attempt policy. An HTTP status alone cannot establish absence of an effect.
- **Multi-table atomic commit**: `POST /v1/{ns}/transactions` (`CommitTransactionRequest`)
  a future capability requiring a separate atomicity gate; on non-supporting catalogs, our
  cross-table commit strategy degrades per doc 07 §4 (staged sequential commits with
  a transaction journal in `pg_lakehouse_table`).
- View endpoints, `registerTable`, plan-table endpoints: v2.

Treat route selection, required headers, credential mode and operation
support as explicit profile capabilities. Keep the same pinned reader and
commit/recovery contracts across compatible profiles. Add provider-specific code
only where its authentication, routing or protocol semantics require it.

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
- TTL: `lakehouse.metadata_ttl` (default 10s) **plus** proactive invalidation for
  managed tables (we know when we commit). External tables are eventually-consistent
  by design: an out-of-date snapshot read by Postgres is a documented consequence,
  mitigated by `AssertRefSnapshotId` on write attempts (we never write on a stale
  read snapshot — we re-plan instead).
- Memory: metadata cache is bounded by `lakehouse.metadata_cache_mb` (default 256MB,
  shared via DSA), LRU eviction. Manifests are streamed, not retained, except
  during planning.

## 5. Failure semantics

| Failure | Behavior |
|---|---|
| Catalog unreachable at plan time | ERROR (fail fast); retryable at executor via `lakehouse.commit_retries` only for commits |
| Catalog unreachable at commit time | Ambiguous direct completion returns `40003`. A staged send with unknown disposition stays lookup-only; retain the intent and its objects |
| Remote table deleted under us | `loadTable` 404 → rel marked invalid; SELECT errors `iceberg_table_dropped`, DDL hint to re-attach |
| Schema drift by another engine | On next metadata load: if schema is *compatible* (additive), transparently adapted; if incompatible (type narrowed / column dropped that we use) → ERROR with diff printout |
