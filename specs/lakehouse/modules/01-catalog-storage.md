# Anvai Lake catalog and storage module

**Read** [shared contracts](00-integration-contracts.md). **Own** Rust `catalog/`, `storage.rs`, `cache.rs` and the remote table resolution/commit portions of `session.rs`. Reuse its existing client for Lakekeeper. The SQL package owns mapping selection, the reader owns interpretation of Iceberg files, and the publisher owns durable retry decisions.

## Implementation

1. Resolve catalog configuration, scoped credentials and table binding. Validate the table UUID and ref at the execution boundary. Return a supported profile, metadata identity and concrete snapshot or explicit empty state. Support exact ID and ref selectors. Implement timestamp selectors with historical current-snapshot transitions and an explicit failure when history is insufficient.
2. Support scoped HEAD, full reads and bounded ranges under deadline and byte budgets. Authorize each delegated location and enforce configured origin and storage scope. Reuse clients within that scope. Cache immutable bytes by authority, canonical object identity/version and range. Expose misses and fetched bytes independently of decoded bytes.
3. Classify commit outcomes and provide operation lookup to the publisher. A verified successful result includes the UUID/ref and committed snapshot. A CAS rejection means the caller can replan against a fresh verified base. A failed connection after the commit may have been accepted yields unknown. Reconcile the exact operation marker on the intended table/ref or report insufficient evidence.
4. Qualify Lakekeeper first. Stage Glue/EMR, Unity and Snowflake in per-profile changes. Publish a capability table for format version, auth, create/load/commit, credential refresh, object access and operation evidence. Fail an unverified capability before making mutations.

## Independent test package

The commit transport now sends one POST per attempt and recognizes a requirement rejection from the typed catalog response. Redirects, ambiguous gateway failures and unusable success replies surface as unknown. Append planning can still retry a proven conflict. Durable operation markers and lookup on takeover remain delivery items. [Catalog implementation and fault tests](../../../src/backend/access/lakehouse/core/pgdatalake-core/src/catalog/commit.rs)

- Stateful mock REST catalog and inert object store. Verify ref advance, empty snapshot, absent in-band metadata, table replacement and timestamp history after rollback.
- Simulate rejection, reply lost before execution, reply lost after success, and reconciliation with missing or mismatched markers. Run the shared `publish-reply-lost` vector and the [staged recovery checkpoints](04a-publisher-recovery.md#when-a-second-append-is-safe). Observe actual commit requests at the catalog boundary to catch any retry hidden inside the transport.
- Verify credential-scope isolation and range identity across refresh, cancellation and cache reuse. Record expected request counts and fetched bytes. Release gate includes Lakekeeper plus an independent reader checking exact pinned rows.
