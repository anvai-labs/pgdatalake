# Exact-pin read enrollment

**Proposed conformance contract v1.** A self-hoster must be able to use the PostgreSQL and storage checks independently. This first slice covers resolving one explicit snapshot for one read-only relation. At PR #47 the FDW calls `GetUserMapping(GetUserId(), ...)`. Passing the stock package smoke alone cannot establish which mapping a view uses, or that a warm cache respects revocation. The shared fixture runs a provider model. Live SQL, storage instrumentation and a downstream login are separate gates. [FDW mapping](../../../src/backend/access/lakehouse/fdw.c) · [Decision cases](fixtures/scoped-read-v1.json)

| Owner | Input → result | Durable state and failure boundary |
|---|---|---|
| SQL/packaging (06) | PostgreSQL-authorized relation access, effective mapping and enrolled binding generation → scoped lookup request | Persist the local relation/server/mapping association and binding ID/generation. Derive the check-as identity from PostgreSQL permissions at each relevant access, including planning and prepared execution. The user of a view still needs the privilege to invoke it. Revocation, mapping change, role change or reattachment invalidates prior eligibility. A client-supplied role name cannot establish identity. |
| Catalog/storage (01) | Scoped lookup + explicit snapshot string → `pin`, `denied`, `changed_binding`, `pin_unavailable`, `unsupported` or `unavailable` | Bind catalog authority/profile, table UUID, ref, approved storage roots and credential scope. Check the independently configured root and catalog-resolved root for every referenced object before fetching or returning cached bytes. Credential rotation and changed binding require a new generation. Preserve the requested ID on retries. |
| Downstream consumer | Independently read current grant + exact requested pin + observation over a trusted channel → eligibility for an authenticated SQL canary or a refusal | Resolve the grant from the authenticated principal and installation. Bind ID and generation must stay unique across reattach, restore and credential rotation within that installation. Keep the current route/credentials independent of sample evidence. A stale positive observation cannot revive a replaced or revoked grant. |
| Qualification (08) | Same case IDs, provider trace, consumer decision and independently observed SQL result → profile receipt | Archive both revisions, runtime digest, profile, binding generation, requested and observed pin, access outcomes, row multiplicity and request counters. Only the live gate can establish isolation for that combination. |

## Ordinary SQL mapping evidence

The stock PG19 fixture exercises an owner-rights view and a security-invoker view over the same pinned foreign table. The caller has SELECT on both views. Initially only the owner has a working mapping. On the PR #47 build the authorized owner-view read failed with `42704` for the caller. The corrected ordinary FDW scan passes the plan’s check-as identity into option resolution at execution and uses local statistics for sizing. With generic prepared plans, the owner view succeeds, the unmapped invoker view is refused, adding the invoker mapping enables it, and dropping the owner mapping makes its next execution fail. The surviving invoker view still returns exactly `[10,20,30]`. The additive stock receipt check is `view_mapping_roles`. [Live fixture](../../../src/test/lakehouse/fixtures/stock-fdw-check.py)

## Remote byte-cache authorization

Whole objects and ranges cached from a network store are reusable within one open storage handle, including its clones. A newly opened handle must fetch through the object store again, even when it uses the same credentials and object key. Cache keys contain a process-local handle identity and no raw credentials. This narrows reuse because bytes that remain immutable can still become inaccessible after a provider policy change. The bounded in-memory cache retains old entries until eviction. A fresh remote handle cannot reach those entries.

A loopback S3 provider test warms an object and a range, observes two hits from a clone, then returns HTTP 403 on both reads from a fresh handle using the same credentials. The test records four HTTP requests and seven fetched bytes. The bound server pilot below adds a SQL fixture that warms two roots in one backend and checks both a database-side refusal and a fresh storage authorization failure. [Cache provider test](../../../src/backend/access/lakehouse/core/pgdatalake-core/src/cache.rs)

## Bound server pilot

The v2 REST and S3 pilot accepts four additional foreign **server** options. Use `expected_table_uuid` for the canonical lowercase table UUID, `storage_root` for the exact canonical `s3://bucket/table-directory`, `catalog_prefix` for the approved REST route prefix (an explicit empty string selects the unprefixed route), and `s3_region` for the signing region. If any of these options is set, the server requires all four plus its configured `endpoint`, `warehouse` and `s3_endpoint`. The effective PostgreSQL role supplies both static S3 credentials through its user mapping. An attachment supplies the exact snapshot ID through `snapshot_id` or a native exact selector. Existing servers without these options keep their previous behavior.

Provision a server for one enrolled table. Keep server ownership and the source of the UUID, directory and routes under the operator's control. The foreign table owner can choose a locator and pin but cannot remove server scope through table options. Bound servers are read-only FDW inputs and managed table resolution refuses them. HTTPS is required outside the numeric loopback test profile. Bound reads skip catalog profile discovery, ignore storage overrides returned by the load and follow neither REST nor object redirects. Ambient HTTP proxies and AWS credential fallback are outside this profile.

The reader checks the metadata object against the approved directory before opening storage. It compares the loaded UUID and exact table directory before opening manifests, then checks manifest lists, manifests, data and delete objects before use. Canonical S3 path components consist of ASCII letters, digits, periods, hyphens and underscores. Empty or dot segments and escaped components are outside the pilot profile. A bound read rejects a missing snapshot before manifests and never falls back to the latest snapshot. The pilot proves an exact pin within the enrolled table. Ref ancestry and the current service grant generation remain responsibilities of the joined admission gate. SQLSTATEs at the bound session boundary are `42501` for a denied object location, `55000` for a changed table UUID or root, `22023` for an absent snapshot, `0A000` for unsupported binding, and `08000` for provider or decode failure. A missing PostgreSQL role mapping still uses `42704`, separate from an absent snapshot. Option validation retains its existing SQLSTATEs.

Bound scans load one [bounded metadata observation](01-catalog-storage.md#bounded-observation-for-enrolled-reads).
The HTTP outcome determines the SQL result before storage opens. A catalog
`404` yields `42P01` for a missing or concealed relation at the enrolled route.
A catalog `401` or `403` yields `42501`. A catalog `5xx` or a transport failure
yields `08000`. The client uses the enrolled route once and discards pointers
in error bodies. A missing pin remains `22023` after a successful table
observation. A new statement after recovery resolves the same configured pin.

The same observation supplies the chosen snapshot, its manifest list and
the current schema used for stock FDW compatibility. A malformed, oversized or stalled catalog response stops before
any storage request. A metadata object that exceeds its wire or gzip
budget stops before any manifest request. The read returns `08000` when
a metadata wire, expansion or deadline check fails. A fresh statement
resolves the operator-approved route and metadata again.

The native CI fixture runs on stock PostgreSQL and the fork. It creates two v2 tables with distinct UUIDs under separately authorized SeaweedFS prefixes. Trino independently verifies the exact parent `[10,20,20,30]`, delete-bearing child `[10,20,30]`, and sibling `[90,90,99]` at their recorded snapshot IDs. Two PostgreSQL login roles reuse generic plans through direct SQL and owner-rights or security-invoker views. The test warms the sibling root in the first role’s backend through an authorized owner view. Returning that table’s metadata location for the first role’s pin fails with `42501` after one allowed catalog load and zero object requests. Changed table identity and a missing snapshot stop after the metadata GET. Both redirect canaries receive zero requests.

The same fixture remaps each role in turn to the other root’s valid S3 key. Each statement returns `08000` after one SeaweedFS HTTP 403 on its own enrolled metadata object. The recorder forwards the original SigV4 request and independently counts requests and fetched bytes. Switching to the replacement mapping reads the original pin and multiplicity using the replacement key, both in the warm backend and on a fresh connection. Dropping the mapping yields `42704` with zero catalog or storage requests for the affected prepared plans. The other role still reads its own pinned rows.

The catalog recorder tests the mapped token and allowed route and uses live Lakekeeper responses. The fixture exercises provider prefix policy, SQL mapping rotation and revocation in a disposable, self-hosted profile. Service-issued grant generations and authenticated Lakekeeper, hosted login and cloud IAM qualification remain release gates. [Scoped read fixture](../../../src/test/lakehouse/fixtures/scoped-read-check.py)

The 2026-11-27 stock PostgreSQL 19 release run used Lakekeeper, SeaweedFS and
Trino 484. Trino independently matched the parent, child and sibling row
multisets. HTTP 401, 403, 404, 500 and 503 recorder responses each stopped after
one catalog request and zero object requests. Prepared reads exercised
credential rotation and revocation. The running fixture's failed S3 key probe
was isolated by starting Trino from the tracked catalog settings while
retaining the same Lakekeeper and SeaweedFS services.

## Observation and decision

The fixture encodes a provider observation as JSON. A positive observation has integer `version: 1`, `status: "pin"`, `binding_id`, positive integer `generation`, `mapping_role`, canonical lowercase `table_uuid`, `ref` and `snapshot_id` as a canonical signed 64-bit decimal **string**. Binding ID scopes the full enrolled authority, storage roots and credential identity, which need not appear in the observation. Scope it to the installation that issued it. Config, authority or trust-domain changes require re-enrollment. The examples use symbolic role identities. A deployed adapter must also distinguish a dropped/recreated role or mapping by changing the generation. A negative observation carries just version and status. V1 is an offline interoperation format. The online adapter and SQL diagnostics still require implementation.

Consumer decision order for v1:

1. Read the authoritative grant for the authenticated request. A revoked or missing grant returns `denied`, even if a previous pin is cached. An unavailable grant store cannot authorize a canary.
2. Validate version, status and the typed fields required for that status. An unknown version returns `unsupported_contract`. A malformed result returns `invalid_evidence`. Forward a recognized negative status without an automatic change of pin or authority.
3. Match the positive observation to the current binding ID, generation, mapping identity, UUID and ref. A mismatch returns `changed_binding`. Then require the exact requested snapshot string. A different ID returns `pin_mismatch`. Only a complete match returns `eligible_for_pinned_canary`. That outcome authorizes a test decision; a serving backend still enforces PostgreSQL privileges and its current binding.

The provider model starts with direct access to the fixture grant and a cold catalog. Case overrides control the session/check-as roles, SELECT privilege, available generation, catalog result and object scope. `catalog_calls` counts logical loads in that one-table, no-retry fake. `cache_hit` seeds the live object-cache condition for the joint test. Construct downstream inputs from the base grant, requested pin, the expected provider status and the case patches. A `consumer_grant` of null represents revocation after lookup. A `reply_patch` deliberately changes the observation in transit between the two test doubles. Implement each side against the file independently. Change status precedence or field meaning only in a new major; additive optional fields can be ignored.

## Joint gate and order of work

1. Freeze the fixture, implement 06 role selection and 01 object scoping against fakes, and let the operator implement its refusal policy on the same cases. Core owns the SQL callback and credential-generation invalidation. The operator owns selecting the live grant and obtaining an authenticated provider observation. Both qualify the trusted mapping of installation plus binding ID before composing them.
2. Create separately authorized tables named `orders` under sibling prefixes such as `a/orders/` and `a/orders-old/`. Record two real UUIDs, parent and child snapshot IDs. Prime the second root's metadata and object cache with an authorized read. Run both normal and prepared queries with the first role, through direct access, owner-rights views and security-invoker views. Revoke between PREPARE and EXECUTE and between observation and canary. Recheck at executor start and before using cached data. A denied request returns no rows, no sibling cache bytes and **zero** catalog requests using the forbidden identity or object requests to the sibling root. Authorized discovery can still load its own catalog. Count SDK requests by principal and root using an independent recorder.
3. An allowed canary compares exact parent `[10,20,20,30]` and child `[10,20,30]` with an independent reader. Alias the catalog name to another UUID, expire the requested pin, rotate credentials and stall the catalog. Require the named refusals, no implicit latest-snapshot retry and zero forbidden calls. Report any in-flight query separately. A revocation boundary for a new statement does not retroactively retract rows already delivered. Alongside stage, case ID and version, record cache state, permitted and forbidden request counts, bytes, rows, durations and sample count. Repeat per real cloud profile. [Storage scoping](09-cloud-profiles.md) · [Operator handoff](10-downstream-handoff.md)

S2 unblocks a live pinned-reader admission test. Managed publication, writer failover and COUNT optimization can proceed on their own contracts. Passing the v1 model gives no permission to expose a route or advertise a latency target.
