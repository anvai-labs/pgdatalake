# Anvai Lake catalog and storage module

Use the [per-cloud adapter contracts](09-cloud-profiles.md) for authority, credential rotation, conditional creates and real-profile release tests.

**Read** [shared contracts](00-integration-contracts.md). **Own** Rust `catalog/`, `storage.rs`, `cache.rs` and the remote table resolution/commit portions of `session.rs`. Reuse its existing client for Lakekeeper. The SQL package owns mapping selection, the reader owns interpretation of Iceberg files, and the publisher owns durable retry decisions.

## Implementation

1. Resolve catalog configuration, scoped credentials and table binding. Validate the table UUID and ref at the execution boundary. Return a supported profile, metadata identity and concrete snapshot or explicit empty state. Support exact ID and ref selectors. Implement timestamp selectors with historical current-snapshot transitions and an explicit failure when history is insufficient.
2. Support scoped HEAD, full reads and bounded ranges under deadline and byte budgets. Authorize each delegated location and enforce configured origin and storage scope. Reuse clients within that scope. Cache immutable bytes by authority, canonical object identity/version and range. Expose misses and fetched bytes independently of decoded bytes.
3. Classify commit outcomes and provide operation lookup to the publisher. A verified successful result includes the UUID/ref and committed snapshot. A CAS rejection means the caller can replan against a fresh verified base. A failed connection after the commit may have been accepted yields unknown. Reconcile the exact operation marker on the intended table/ref or report insufficient evidence.
4. Qualify Lakekeeper first. Stage Glue/EMR, Unity and Snowflake in per-profile changes. Publish a capability table for format version, auth, create/load/commit, credential refresh, object access and operation evidence. Fail an unverified capability before making mutations.

## Bounded observation for enrolled reads

The enrolled read adapter uses one explicit `loadTable` route and decodes
only the metadata object pointer. The response body has an 8 MiB limit
and a four-second HTTP deadline that covers headers and the body. The
scoped catalog client disables HTTP replay, redirect following and
ambient proxies. In-line metadata and credential overrides supply no
authority for this profile.

The adapter checks the pointer against the independently enrolled S3
root before making a storage request. A cache-bypassing ranged read
requires one complete metadata object within 8 MiB, with an independent
four-second deadline covering the SDK request and body. Gzip expansion
also stops at 8 MiB. The decoded object must match the enrolled table UUID
and root and use Iceberg v2. The ceilings apply to this scoped profile
and can cause a provider-unavailable response for a large inventory.

The internal `ReadBinding::observe` method returns the metadata with its
same bound S3 scope. The caller can inspect this snapshot inventory and
consume the observation to plan one exact snapshot without loading the
catalog or metadata object again. The managed reader can build its ancestry
proof over the same borrowed metadata once its PostgreSQL authority and
membership adapters are joined. Stock FDW planning continues to use the
observed current schema. Managed visibility must prove the selected pin's
schema and its compatibility with the bound PostgreSQL source before
enabling a managed scan. Manifest and data access retain their existing
root checks. These budgets cover the metadata phase. PostgreSQL controls
the statement deadline.

## Prepared staged append

The Rust catalog provider packages a writer-produced append snapshot into one immutable REST request. It uses the claim's table UUID, operation UUID and input digest, plus validated Iceberg v2 metadata for an unpartitioned table on `main`. The request asserts table UUID, exact branch head (explicit null for an empty ref), schema ID and default spec ID. It carries `pgdatalake.operation-id`, `pgdatalake.input-sha256` and `pgdatalake.manifest-list-sha256` in the snapshot summary. The writer computes the manifest-list digest over the same bytes it uploads.

Preparation returns up to 64 KiB of exact request bytes, their SHA-256 digest, expected parent and proposed snapshot ID without contacting the catalog. It rejects credential-bearing storage locations and locations with a query or fragment. The prepared value can be consumed by one `submit` call on its original client and route. A route-prefix change fails before dispatch. The common POST helper disables transport replay and redirects. The worker must commit send authorization for the exact digest before submitting. Extension 1.6 stores the prepared bytes through the send-authorizing transaction. The lookup provider below supplies acceptance evidence. Verified claim capture and worker integration remain delivery gates. [Local send fence](04a-publisher-recovery.md#send-intent-and-receipts-in-extension-15)

## Acceptance lookup for a retained proposal

The worker supplies the authority-scoped REST client and locator, together with the operation, table UUID, input digest, expected parent, proposed snapshot ID, exact request bytes and their retained SHA-256. The provider checks the recorded body against that identity before fetching metadata. A valid body must describe one append and its update to `main`. A body without the manifest-list digest remains unresolved.

The first lookup profile reads fresh in-band Iceberg metadata. It verifies the table UUID, identifies `main` as a branch at the current head, and follows retained parent links with decreasing sequence numbers. It compares the introducing snapshot's fields and recorded summary to the authorized proposal. A matching marker elsewhere in the table, missing ancestry, duplicate operation markers, and conflicting metadata remain unresolved. An append followed by later compatible commits is verified at its own introducing snapshot.

The provider reads the object only after the catalog evidence matches. The manifest-list URI must remain under both the catalog metadata location and the independently bound table root. `PublicationStorage` supplies that root, the scoped credentials and the object byte and time budgets. One cache-bypassing ranged GET requests at most the byte limit plus one probe byte. The adapter accepts only a complete object within the limit and streams its SHA-256 without retaining the body. The deadline covers the object request, retries and body. The worker integration owns binding resolution and immutable manifests and data. This lookup verifies the introducing snapshot and its manifest list. Full table-data qualification belongs to the worker and independent-reader smoke. Its outcomes are verified acceptance at an exact snapshot or unresolved. Transport errors leave the attempt unresolved for scheduling purposes. An inconclusive lookup never authorizes another send.

## Independent test package

The commit transport now sends one POST per attempt and recognizes a requirement rejection from the typed catalog response. Redirects, ambiguous gateway failures and unusable success replies surface as unknown. Append planning can still retry a proven conflict. The standalone acceptance lookup is available. Bounded digest reads are integrated into the Rust lookup. Worker takeover and durable definitive rejection remain delivery items. [Catalog implementation and fault tests](../../../src/backend/access/lakehouse/core/pgdatalake-core/src/catalog/commit.rs)

The [live reply-loss smoke](../../../src/test/lakehouse/fixtures/catalog-reply-loss-check.py) waits for Lakekeeper to accept a real direct append before substituting a gateway timeout. It requires PostgreSQL SQLSTATE `40003`, one successful catalog POST and exactly the old and new rows in Trino. Worker takeover adds the durable ledger and reconciliation checks in the [staged recovery slice](04a-publisher-recovery.md).

- Stateful mock REST catalog and inert object store. Verify ref advance, empty snapshot, absent in-band metadata, table replacement and timestamp history after rollback.
- Simulate rejection, reply lost before execution, reply lost after success, and reconciliation with missing or mismatched markers. Run the shared `publish-reply-lost` vector and the [staged recovery checkpoints](04a-publisher-recovery.md#when-a-second-append-is-safe). Observe actual commit requests at the catalog boundary to catch any retry hidden inside the transport.
- Verify credential-scope isolation and range identity across refresh, cancellation and cache reuse. Record expected request counts and fetched bytes. Release gate includes Lakekeeper plus an independent reader checking exact pinned rows.
