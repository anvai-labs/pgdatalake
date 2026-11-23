# Anvai Lake managed visibility and local ledger

The [write handle and receipt contract](03a-client-receipts.md) specifies membership across batches, client-visible failures, rollback, multi-table reads and the failover durability gate.

**Read** [shared contracts](00-integration-contracts.md) and [DML rules](../18-anvai-lake-dml.md#5-read-rules-mvcc-and-conflict-handling). **Own** the PostgreSQL snapshot/command rules, TableAM staged capture and overlay in `tableam.c`, and `managed_count.c`. Own local ledger schema and migrations through the shared SQL integration change. The publisher owns transitioning a publication claim. Catalog and reader supply immutable pinned results and operation membership.

The [staged capture enrollment contract](03b-managed-capture-binding.md)
defines the handoff from an approved local generation to the publisher's
durable claim. Its executable red exercises ordinary INSERT and the 1.6
ledger before the enrollment provider is wired.

## Implementation

1. Capture supported staged INSERTs and their stable operation/segment IDs with their PostgreSQL transaction. Include an immutable table binding or verifiable durable reference, payload version and logical row count. Preserve savepoint semantics and relation lifecycle. Use normal PostgreSQL visibility for committed entries and a command-aware overlay for own earlier commands.
2. Pair a PostgreSQL snapshot with a compatible published pin. Apply only PG-visible operations absent from that pin. Derive membership from the publication evidence, including after a lost catalog reply. Preserve pending rows for older supported pins. An unknown proof or a future operation requires a compatible retained pin or an explicit consistency error. Never treat a remote failure as an empty base.
3. Expose one local adapter to claim and finalize exact segments. The ledger schema owns uniqueness, segment/payload integrity, claim lifetime, lease generation checks and retention. Group readers and reclaim data by certified retention/cut validity. A relid drop or rebound must not redirect prior work to a new remote UUID.
4. Derive metadata COUNT from the same published pin and local overlay proof, with PostgreSQL permissions, view behavior and RLS intact. Fall back to an ordinary scan when proof fails. Keep a single owner for scan-time visibility and COUNT. First delivery is append only.

## Independent test package

### First red case for managed COUNT

Reproduced on the local assertion-enabled fork used for PR #47 with extension 1.6. On an attached, initially empty table `events(id bigint)` using `write_mode='staged'`, issue the following as one backend. Both input IDs are non-NULL. The ordinary scan is the row oracle.

```sql
INSERT INTO events VALUES (10);
PREPARE n AS SELECT count(*) FROM events;
BEGIN;
INSERT INTO events VALUES (20);
SELECT array_agg(id ORDER BY id), count(id) FROM events;
EXECUTE n;
ROLLBACK;
EXECUTE n;
```

The scan observed `[10,20]` and `count(id) = 2`. The optimized COUNT returned **1** inside the transaction and **1** after rollback. Required results are **2** then **1**. The ledger sum is available for the committed row. The new row is still in backend-local staged capture until pre-commit. Recomputing the ledger sum at execution cannot see it. [Managed COUNT](../../../src/backend/access/lakehouse/managed_count.c) · [TableAM capture](../../../src/backend/access/lakehouse/tableam.c)

The executor now falls back to the ordinary aggregate while this backend has write state for the relation. A read uses the same non-creating lookup as the TableAM scan. The generic-plan regression exercises seven committed rows and two new rows, a rollback to one new row, full rollback, another insert and commit. Expected counts are 9, 8, 7, 8 and 8. [Regression SQL](../../../src/test/regress/lakehouse/sql/count_elision.sql)

An FDW successor can share a proof for the published pin. The managed caller must also account for this backend's writes and for the PostgreSQL-visible segments already included in the selected pin.

### Rescanning staged rows

A serial TableAM rescan rewinds the current file cursor and both staged
row sources. It keeps the original scan session and retained ledger
payloads. The offline staged scan regression forces a nested-loop rescan
with committed and transaction-local duplicates. Its inner scans return
`[1,2,2,3,4]` and `[2,2,3,4]`. The baseline returned no rows on the
second pass. Scan teardown releases the Rust cursors and session, including
when a PostgreSQL error discards the executor context.
[Regression SQL](../../../src/test/regress/lakehouse/sql/staged_insert.sql)

### Compatible published pin

For managed current append reads, the pin decision precedes the first emitted row. The proof accounts for prior local publications as well as the newest claim. Within the statement's allowed candidate pins, the decision is:

```text
for pin in candidates_newest_first:
    membership = resolve_local_membership(pin, retained_ledger)
    if membership is incomplete or includes a segment invisible to this PG snapshot:
        continue
    overlay = PG-visible local rows whose segments are proven absent from pin
    return pinned_rows(pin) UNION ALL overlay
return consistency_error
```

The statement's own earlier commands follow the same visibility rules. An explicit historical published read uses the requested pin directly. A managed COUNT shortcut must produce the same multiplicity as the resolved scan.

#### Membership and visibility seam

The catalog-side proof supplies candidates in certified ancestry order for the
enrolled authority, table UUID, ref and local source lineage. Each candidate
carries an exact pin and either a complete included segment set or an explicit
unknown result. Completeness includes prior capture generations in that
lineage and source transactions the PostgreSQL reader cannot see. A provider
can assert an empty set only with proof of absence within a protected retention
horizon. Scope mismatch stops selection. Unknown membership or retention loss
permits trying an earlier candidate only when its proof is complete.

The PostgreSQL-side observation holds the statement's original MVCC snapshot
and enumerates visible segment IDs with retained payload. Publication completion
can become visible at a different time from source INSERT. Compare the two
immutable identity sets before decoding or emitting rows. Reuse the selected
pin and overlay IDs for aliases, rescans and COUNT. Preserve separate own-command
visibility and require the complete vector for joins before output. Returning a
partial result would be a different consistency contract.

The [live decision fixture](../../../src/test/lakehouse/fixtures/managed-cut-check.py)
starts with ordinary bound INSERTs, two held PostgreSQL snapshots and a real
two-member append discovered through 1.9. The worker receives an independently
approved target. A mismatched approval produces zero catalog calls. The matching
send is accepted once and its catalog reply is withheld. Lookup verifies the
original intent and finalizes both segments. The fixture reads the verified
receipt and members from PostgreSQL, checks the actual parent and child in
Lakekeeper, and compares exact pinned rows with Trino. It captures the fresh
PostgreSQL view before local completion. After both source commits the child
contains `[10,20,20]`. The middle snapshot uses the retained parent and the
one visible member, yielding `[10,20]`. The old snapshot yields `[10]`.

The shared Rust ancestry adapter and cut selector make the decision in this
fixture. The fixture passes the real Lakekeeper `loadTable` metadata and the
approved route identity, using the persisted pre-publication pin as its
enrollment anchor. The selector consumes visible PostgreSQL segment IDs and
whether their exact payloads remain available. Candidate states distinguish complete
membership, unknown proof and expired pins. The selector checks exact IDs and
monotone append membership across the complete candidates.
It refuses a changed scope, contradictory proof, or the absence of a usable
cut. An older pin is usable only if each needed overlay payload remains
available. The output is one exact pin and sorted segment IDs. The fixture
compares its row multiset with the independent oracle and the pinned external
engine. [Selector](../../../src/backend/access/lakehouse/core/pgdatalake-core/src/managed_visibility.rs)

#### Certified retained ancestry

The initial adapter certifies one Iceberg v2 `main` observation through a
positive retained enrollment pin. Its caller supplies the authority and local
incarnation from the approved route. It compares the reported table UUID and
root to the enrolled values and requires the explicit `main` branch to agree
with the reported current snapshot. It follows actual `parent-snapshot-id`
links with strictly decreasing sequence numbers, scoped manifest-list paths and
append summaries above the anchor. Snapshot order and numeric ID magnitude
have no bearing on ancestry. The bounds are 4,096 snapshot records and 512
chain pins, including the head and anchor. The adapter refuses missing or
detached anchors, duplicates and contradictory links, with no usable partial
chain on failure. The live fixture removes the parent record from the observed
metadata and also reverses candidate order to check both refusals before row
assembly. [Iceberg snapshots and references](https://iceberg.apache.org/spec/#snapshots)

The successful adapter accepts membership only in the certified chain's order.
An omitted pin has no usable membership.

#### Enrollment and ledger evidence

Opt-in extension 1.15 records the operator's independently verified retained
`main` pin in the first capture enrollment transaction. The existing
relation lock serializes capture, and the source ledger must be empty.
The private scope accessor requires the active binding, current source shape
and continuous scope across every generation in the enrollment lineage.
Rotation with identical scope retains the anchor. Returning to an earlier
route after a change leaves the proof unavailable. Upgrade leaves existing
bindings without inferred anchors. An empty remote table needs a separate
empty-base proof.

The original SQL snapshot supplies visible segment IDs, row counts and sealed
payload digests, including locally published rows. A fresh ledger observation
supplies edge additions for the independently certified chain. An edge is
complete only when one exact retained attempt agrees with the certified parent,
the verified snapshot and the digest of all its sealed published members.
The current bounds are 512 pins, 1,024 members per publication, and 16,384
visible segments or cumulatively proved members. Indexed pin and segment
lookups serve these private accessors. The executor will still need to retain
actual payloads and protect object retention for the selected pin.

The finalized-edge accessor reports the accepted child as unknown before
local finalization. Its descendants also remain unknown. A read-only verifier
can prove one pending head above a complete parent. It binds the exact
retained body and complete sealed members to the certified parent/head,
schema/spec and scope. It validates the member digest, proposed snapshot
markers and one scoped manifest-list digest using the metadata already held by
the ancestry proof. The existing one-open-claim rule bounds the pending
case. An unavailable or unproved head leaves the earlier cut available when
it has complete evidence and retained overlay payloads.

The live fixture uses the approved route for one size-bounded metadata GET and
a separate committed read-only SQL observation for the pending attempt and
members. Each held snapshot uses the same catalog observation and no additional
catalog GET. The digest read uses a separate scoped S3 reader and a 1 MiB,
four-second limit. At the accepted POST barrier, old and middle snapshots use the
retained parent with the exact visible overlay. The fresh snapshot uses the
accepted child with an empty overlay while the ledger remains unfinalized.
Changed proposal bytes, a missing member and a changed manifest leave the head
unknown. A missing complete parent prevents proof. The test compares the
retained ledger before and after the decisions and then separately reconciles
through the publication lookup worker.

#### Scoped ledger reads

Opt-in extension 1.18 separates two private accessors. The fixture runs them as
the extension owner against the authorized relation and binding. Both operate
with caller privileges and revoke execution from PUBLIC. Application SQL roles
get neither publication evidence nor payloads through these entry points.

The caller runs `get_managed_read_pending_head` with a fresh committed ledger
snapshot after observing the certified catalog head. It returns the one scoped
pending attempt and up to 1,024 sealed member identities, or NULL when the
edge has no unique evidence. It supplies bytes to the read-only verifier and
does not prove remote acceptance. The caller, rather than the stable SQL
function, supplies snapshot freshness.

After the selector chooses the overlay, the caller runs
`load_managed_read_overlay` under the original statement data snapshot. The
same snapshot covers scope, budget checks and payload loading. The loader
accepts up to 16,384 strictly ascending exact IDs and returns all their payloads
with sealed digests. It refuses an invisible or missing member or a batch over
the caller's row or byte budget. The current maximum is 65,536 rows and 8 MiB.
Neither the finalization status nor a newer source commit changes that
snapshot's visible bytes. The held-snapshot fixture loads the exact chosen
IDs and checks the resulting bytes again after native finalization.

This fixture qualifies the two ledger reads and their snapshot roles. The
overlay accessor sees binding state through the data snapshot. During
rotation, a fresh read refuses the old binding and a held older snapshot
refuses the successor. The accessor alone cannot detect a later committed
revocation of the binding it still sees. The executor must identify the
latest active binding and compare it with the original statement's scope
before loading bytes.

The executor must copy selected payloads into scan-owned memory and verify
SHA-256 of each copied payload against the sealed expected digest before
emitting any row. Rescans use the retained copy. The C executor also needs
to consume the shared [bounded observation](01-catalog-storage.md) and secure
a retained object lease or equivalent provider proof. Reanchoring beyond retained history needs a
durable membership checkpoint.

TableAM must additionally integrate own-command visibility and prove the whole
statement pin vector before first output. The fixture continues to require
`0A000` from the bound SQL path. Held snapshots exercise the selector under
real MVCC and leave application Repeatable Read support unqualified.

#### Native statement cut boundary

Start with one supported serial append-only base relation. The executor
keeps the PostgreSQL statement snapshot and normal permission checks. A
trusted, fixed SQL ledger call can read private proof data as the ledger
owner after those checks. Do not select storage credentials as that owner.
Derive the approved reader binding from the authorized relation and
current enrollment. Require the original snapshot to recognize that scope.
Both checks must succeed even after a prepared plan, credential rotation
or binding invalidation. Decline unsupported query shapes before output
until every bound relation can be preflighted as one statement cut.

Use one scoped catalog observation for certification and scan planning.
Obtain current binding, finalized edges and the one pending candidate with
a bounded fresh committed ledger observation. Restore the original executor
snapshot before loading visible segments and payloads. Copy the bounded
proof and payload data out of SPI before remote I/O. Rust only receives
owned or call-scoped values. PostgreSQL error cleanup stays on the C side.
Verify the payload bytes against the selected sealed digests and the
chosen pin against the enrolled source shape before installing the scan
state. A missing compatible pin, scope mismatch, unavailable payload
or unproved shape prevents the first row. Keep the legacy published-flag
loader outside this path.

The retained scan state owns the immutable pin, the ordered overlay
members, decoded readers and their resource lifetimes. Rescan rewinds its
base and overlay readers without another catalog resolution or ledger
selection. EndScan and the executor's error path release both. Portals
continue using their installed cut across fetches. Object retention
must protect the selected pin for the supported portal lifetime. Bound
COUNT continues to decline until it can preserve the same privileges,
snapshot and cut. Add distinct telemetry for the compatibility refusal,
origin timeout and oversized proof so operators can distinguish a safe
refusal from a slow authorized read.

#### Admission before the native cut

The first executor slice supports a direct, serial, append-only base scan.
Use statement-owned state after PostgreSQL's normal permission checks.
Derive the checked role from the scanned RTE's permission information.
Decline redirected permission identities, views, RLS, inheritance, joins,
parallel plans and unsupported own-command state before any origin request
until each has a separate qualified path. Plain EXPLAIN opens no objects.
Repeat permission and grant checks for each prepared execution.

An operator-approved Read enrollment binds the exact managed incarnation,
current capture generation, checked role, catalog route, table UUID,
`main` anchor, source shape and root. Resolve the scoped credential for that
role. Do not accept a PUBLIC mapping or a Send or Lookup grant. Treat a
same-OID mapping replacement as a credential change. A fixture holding the
ledger owner role cannot qualify this gate. Acquire and compare current and
original-snapshot scope before catalog access. Keep the execution relation
lock while verifying scope. Recheck the read grant and exact scope before
installing the cut.

Use one bounded Rust observation. Take the fresh committed proof snapshot
after observing the catalog so the proof can see the already-durable
intent for the observed candidate. Use explicit read-only SPI snapshots
and copy the bounded result before ending the SPI frame. The separate
fresh snapshot supplies finalized edges and the pending intent. The
registered executor snapshot supplies only visible segment metadata and
selected payload bytes. Restore the role, resource owner and snapshot
state on errors and release all owned handles. The first native smoke
must fail on the unchanged `0A000` baseline, then pass the lost-reply
fixture through normal SQL under an admitted reader role. Wrong or stale
grants must issue zero catalog and object requests. Exercise a nested SPI
caller and prove the next ordinary statement retains its original role
and snapshot after a forced proof error.

#### Native observation and exact-pin open

The internal C ABI starts with `pg_datalake_managed_read_observe` after
PostgreSQL has checked permissions and resolved the statement's approved
route, table UUID and root, authority and incarnation, retained `main`
anchor and enrolled Iceberg schema ID. The caller uses the current
static S3 credentials from the explicitly approved mapping, as in the
scoped FDW read profile. Expiring purpose-bound Read grants and provider
credential issuance require a separate admission gate. The opaque
process-local owner retains one bounded catalog/storage observation. It certifies the existing
append ancestry and exposes positive signed 64-bit pins in newest-first
order, including the retained anchor. It returns zero for an unavailable
pin index. The existing limits are 4,096 snapshot records and 512 ancestry
pins. Reading an index makes no provider request.

After the ledger owner has proved the statement's compatible cut,
`pg_datalake_managed_read_plan_exact` consumes the caller's owner slot and
plans the selected certified pin from its original metadata. A pin
outside the certified chain returns a pin refusal. A selected snapshot
needs an explicit schema ID equal to the enrolled schema and the schema
used by the planner, with one matching schema definition. The ABI refuses
unproved or changed schemas before manifest I/O. A successfully planned
session owns the original root and uses the regular scoped scan and delete
loader. The caller applies its statement qualifications across base and
overlay and checks actual PostgreSQL source-column compatibility.
The stock FDW continues to use its observed current schema.

The consume call clears the slot even on error. Use
`pg_datalake_managed_read_observation_drop` to clear an unused owner and
`pg_datalake_scan_free` for a planned session. The C owner releases both on
its executor error path. The loopback ABI fixture observes one catalog GET
and one metadata GET, then exactly the selected manifest for an accepted
pin above 2^53. Bad schema, an off-chain pin and an unused owner make no
manifest request. The ABI allows a later same-handle pending-head proof.
PostgreSQL still owns original-snapshot ledger visibility, payload and
object retention, statement-wide correctness and all runtime activation.

#### SQL executor acceptance

Status (2026-11-23): the first executor slice is wired in `tableam.c` —
admission runs at scan_begin under the ledger owner's context for the
checked role (`lakehouse.admit_managed_read`, grant table 1.18→1.19) and
declines catalog-locally, with zero origin requests, for parallel plans,
scan keys, RLS, inheritance, own staged writes and non-staged write modes.
Before the observation makes any request, install runs a zero-origin
statement-scope preflight: a reader whose registered snapshot predates a
binding rotation fails with the consistency error before the first
origin contact. Admission returns a mapping-options fingerprint and the
fresh-proof frame compares it field by field, so a same-OID mapping
replacement is a detected credential change. The pending-head proof is
head-only (the #88 verifier resolves the attempt against the certified
head): once the provider head moves past the accepted child, the cut
falls back to the retained parent with the full overlay. The cut itself
installs on the first fetch: preflight → observe → fresh-proof frame
(re-admission compare, certified addition chain, pending-head proof) →
statement frame (registered snapshot: visible segments + payload
retention) → compatible-pin selection → digest-verified overlay load →
exact-pin plan. `lakehouse.managed_read_cut_evidence()` exposes the
installed pin, proof flag and overlay IDs of the current backend.
DOCUMENTED GAP (until RTE-aware admission lands): scans derived from
views and redirected permission identities (`checkAsUser`) are not
detectable at scan_begin; the role-specific `purpose=read` credential
mapping structurally prevents a definer view from widening admission
(the invoker's identity admits), and the fresh-frame scope recheck
rejects mapping rotation, revocation, prefix, region and option
changes mid-scan. Bound COUNT keeps declining through
`lakehouse_check_capture_read`. NOT IMPLEMENTED in this slice, tracked
for the governed-reads band: the retained object lease / provider proof
across portal lifetime, and distinct telemetry for the compatibility
refusal, origin timeout and oversized-proof channels.

Use a v2 Lakekeeper table with parent pin `P` containing `[10]`. Hold one
PostgreSQL snapshot before both source commits and another between them.
Commit two segments `A` and `B`, each containing one row with value `20`.
Publish both as one append on child pin `C`. Use the catalog reply-loss
barrier after the single accepted POST and before reconciliation or local
finalization. Independently observe `C` in Lakekeeper and check that the
claim's `verified_snapshot_id` is still NULL. Run ordinary serial TableAM
SQL at the barrier and repeat after finalization.

| PostgreSQL snapshot | Exact selected pin | Exact overlay segment IDs | Sorted rows |
| --- | --- | --- | --- |
| Before both source commits | `P` | empty | `[10]` |
| Between `A` and `B` | `P` | `[A]` | `[10,20]` |
| After both source commits | `C` | empty | `[10,20,20]` |

Record the real parent and child IDs, actual SQL scan pin and overlay IDs
through the test harness. Row equality alone leaves open which cut the SQL
executor used. Validate the published pins separately with Trino. Hold the
same statement open for a forced rescan, change the observable catalog head,
and prove that the rescan uses the original pin and payloads with zero
additional catalog resolutions. Remove the compatible parent or the needed
overlay payload before opening a fresh older-snapshot scan. A simple
`SELECT id` cursor must report a consistency error by its first fetch, with
zero rows emitted. Keep separate logs for provider request counts, exact
pin and member evidence, and SQL results. The native scan milestone uses
held snapshots as test controls. Qualify application transaction isolation,
joins and managed COUNT through their own gates.

- Run the managed overlay vectors with a fake published view and ledger membership responses. Verify older base/new ledger, new base/stale ledger, future operation and unknown-membership cases. Assert exact rows, including duplicates.
- Run `mixed-visibility-claim` through pin selection and ordinary SQL scans. Two segments share one verified publication. A reader between their source commits falls back to the retained parent and adds just its visible segment. Dropping that parent gives an explicit consistency error. A reader after both commits can use the published pin directly.
- SQL regression for 65 relations, nested SAVEPOINT rollback, prepared plans, ordinary roles/RLS, mode change, relid reuse and catalog outage. Isolate a provably empty offline case to exercise staged reads during an outage.
- Two sessions plus a controlled publication barrier verify one compatible cut per statement, stable rescans and no lost or double-counted rows. Compare COUNT fast path against the ordinary scan. Gate higher isolation levels separately.
