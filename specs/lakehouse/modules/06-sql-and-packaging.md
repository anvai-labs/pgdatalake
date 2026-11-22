# Anvai Lake SQL integration and stock package

Implement the [artifact and capability handshake](06a-release-contract.md) and [SQL receipt semantics](03a-client-receipts.md) on the appropriate build flavor.

**Read** [shared contracts](00-integration-contracts.md) and [packaging direction](../17-anvai-lake-product.md#2-how-much-of-postgresql-is-forked). **Own** `fdw.c`, `fdw_plan.c`, `fdw_exec.c`, `lakehouse_sql.c`, and package/build orchestration. The ledger owner defines its SQL tables. Any extension version or installed SQL change goes through the shared integration change. Reader and catalog modules own the underlying native behavior.

## Stock PG19 source-build preview

The [PGXS package](../../../src/backend/access/lakehouse/stock/README.md) builds the shared FDW C sources and Rust core against installed PostgreSQL 19 headers. Its SQL extension version is `1.0-fdw`. It accepts the foreign table's `snapshot_id` option. The stock profile uses serial scans until parallel startup proves an identical table identity and pin in every worker. The managed distribution retains its native query-level syntax and upgrade graph.

The [stock smoke](../../../src/test/lakehouse/fixtures/stock-fdw-check.py) uses an unmodified 19.0 server, an external Trino write and a real v2 position delete. It checks the original and restored databases against Trino at both exact snapshot IDs, a generic prepared current-state query across the external commit, and rejection of foreign INSERT, UPDATE and DELETE. This is source-build and correctness evidence for the local REST/S3-compatible profile. Signed binary distribution, an upgrade from a released stock predecessor, permission/tenant scope and additional real cloud profiles require separate receipts.

## Implementation

1. Prototype a read-only FDW package on a declared unpatched PostgreSQL version. Split build dependencies from the custom TableAM and parser. Use table options for exact snapshot pinning and documented attach/import UX using the existing validator and mappings where applicable.
2. Retain SQL identifiers and credentials semantics across stock and fork packages. Resolve the permission-checking identity, including view contexts, before external I/O. Preserve SQL quals, RLS and typed errors. Plain EXPLAIN should avoid data and delete reads by default. Workers receive the same pinned work description and correct ownership of each task.
3. Preserve the fork's native time-travel SQL in deparse and logical restore. Test compatible migrations and explicit unsupported-feature errors for stock runtimes. Expose sanitized scan metrics and publication receipt lookups as integration surfaces. A stock reader has no dependency on a publisher, its ledger or TableAM code.
4. Keep the existing FDW COUNT optimization gated until an execution-time plan preserves permissions and snapshot/delete eligibility and has a correct scan fallback. Preserve SQL return types and plan dependencies.

## Independent test package

- Build the FDW using stub native providers with the shared input/result contract. Verify lifecycle, cancellation and mapping authorization. Then link the real reader and run the same SQL assertions.
- Install on stock PostgreSQL and on the fork. Test build/install with feature on and off where applicable, fresh extension install, upgrade from the supported version, and logical schema restore. Verify which native features are available in each distribution.
- Use a Spark-written fixture and compare with an independent reader. Record time to first correct query, required privileges, and roundtrips. Check prepared plans, role changes, invoker/definer views, and deletes with an unchanged manifest row count.
