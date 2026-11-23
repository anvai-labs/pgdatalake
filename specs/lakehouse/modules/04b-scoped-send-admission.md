# Durable send admission and scoped publisher transport

Extension 1.8 is opt-in. It consumes captured source binding records from
[1.7](03b-managed-capture-binding.md), the durable one-attempt protocol from
[04a](04a-publisher-recovery.md), and the [catalog and storage scope](01-catalog-storage.md).
The default install remains 1.6. The native production scheduler stays gated.

| Operation | Owner and durable result |
|---|---|
| `grant_publication_send(binding, catalog_prefix, s3_region)` and `revoke_publication_send(binding)` | The enrollment operator allows one capture binding generation to submit. The grant records the approved explicit prefix and object region. It keeps a revocation tombstone. A new grant requires a new capture binding. An empty prefix is explicit. A missing prefix is invalid. |
| `claim_managed_publication(operation, segment_ids)` | The SQL ledger derives schema/spec, relation incarnation and scope from the one binding shared by all immutable input members. Stamp the first claim in its original INSERT. Missing, mixed-generation and unstamped inputs fail as a unit. |
| `get_publication_send_binding(operation, owner_generation)` | The worker obtains the descriptor for one unresolved owner generation before provider access. The descriptor separates mutable claim ownership from the immutable capture binding and its generation. An inactive capture binding, absent grant, or altered members fail closed. |
| Standalone `CALL authorize_publication_send(...)` | The ledger revalidates the member set, capture state and grant in the same transaction that records the exact request. Scope and grant locks protect the decision until the locally WAL-flushed commit. This is the one-send boundary. A revocation committed before it stops authorization. A later revocation stops new sends and preserves the original intent. |
| `get_publication_lookup_binding(operation)` | Read-only reconciliation uses the exact binding stamped on the retained attempt and ignores the later send-grant state. Prior unstamped attempts remain reserved and available for existing reconciliation with independent original-scope evidence. The migration cannot infer that evidence. |

Transport consumes the descriptor returned by SQL. It binds the catalog
endpoint/prefix/namespace/table, expected authority UUID, approved Iceberg UUID,
ref, schema/spec, storage origin, region and exact root. It requires separate
compatible credentials and uses a single pinned load before storage I/O.
Validate the inline metadata and snapshot identity before assembling files.
Restrict reads and immutable writes to the approved root. Reject endpoint
overrides, redirects and changed identities. Reserve the original request after
an ambiguous reply and reconcile it with read-only provenance checks.

Test adapters use locally known fixture credentials. A deployed resolver needs
to obtain least-privilege current credentials through a trusted reference for
the exact enrolled scope, with expiry and rotation semantics. The scheduler
must manage stable ownership and retry policy while keeping operations durable
across process restart. HA and compatible managed reads retain distinct gates.

## Independent and joined acceptance

- The PostgreSQL provider demonstrates the original 1.7 mixed-generation claim,
  then rejects it in 1.8, checks ACLs and rollback, proves revocation wins before
  a blocked send, and proves an authorized attempt retains its original lookup
  descriptor after revocation. Check prior 1.6/1.7 attempts and no new send for
  an unstamped claim.
- The Rust transport provider independently refuses wrong binding, metadata
  scope and redirected requests. Check denied requests before storage work and
  exact submitted bytes against immutable preparation.
- The joined fixture captures ordinary `INSERT`, claims its exact member,
  sends once, and restarts PostgreSQL after remote acceptance. The worker input
  carries the database and operation identity. SQL supplies all route and scope
  fields. Use one crash-after-acceptance case and one returned HTTP 504 with
  grant revocation after acceptance. Require verification on the same original
  authority, WAL intent/body match, exact rows `[10]` and `[10,20]` from Trino,
  no replay POST, and a later segment retained for its own admission. Require
  zero remote calls when trying to claim against the revoked grant.

## Native worker integration

At public #70 the [Rust driver](../../../src/backend/access/lakehouse/core/pgdatalake-core/src/publication_worker.rs) implements `drive_publication` over `PublicationLedger` and `PublicationCatalog`. The [joined test adapter](../../../src/backend/access/lakehouse/core/pgdatalake-core/examples/bound_publication_fixture.rs) shells out to psql and supplies fixture credentials. The [native publisher](../../../src/backend/access/lakehouse/publisher.c) excludes bound segments from its legacy path. Native dispatch and a runtime credential resolver are proposed work.

| Parallel owner | Input and result | First failing test |
|---|---|---|
| Ledger and scheduler (03/04) | Schedule by database and operation ID. Load claim and intent together, acquire by generation and reload before preparing. Drive the existing transitions on short SQL transactions. Keep source members, identity, ownership, attempt and receipt in PostgreSQL. | Stop the first owner after its intent commits. A second owner must reload the same attempt, retain its original members and perform lookup only. A late ordinary INSERT remains separately pending. |
| Credential resolution and transport (01) | Resolve the approved descriptor for the operation and purpose. Preparation and first submission use the current send binding. Recovery obtains read authority for the retained lookup binding. Return compatible catalog and object credentials with their scope and validity, or an explicit refusal. Challenge a new owner with a valid credential for a different binding. | A missing, mismatched, expired or unrepresentable credential blocks the corresponding I/O. A revoked send grant can still reconcile under separately authorized read access to the original scope. When that access is unavailable, keep the intent pending and back off. |
| Runtime adapter and qualification (04/08) | Feed the two adapters into the existing driver. Preserve its distinction between an unsent preparation failure, unconfirmed authorization, and a reserved attempt awaiting acceptance. Count catalog commit POSTs independently of the worker; report preparation PUTs and lookup GETs separately. | Trigger both barriers below using the same binary and ordinary bound INSERT path. Reuse the receipt consumer only after the native path passes. |

### Native retained-intent preflight

Opt-in extension 1.11 adds `lakehouse.preflight_managed_publication_lookup(operation, owner_generation)`. It launches a separately named, one-shot background worker in the current database under the approved effective non-superuser role. The role needs explicit launch and ledger read grants. The worker opens a short read-only SPI transaction and compares the typed operation, generation, immutable retained request and body digest. Its receipt reports a local preflight decision. The default extension install, legacy worker activation and provider dispatch retain their existing release gates.

A stale generation or an unsent job cannot qualify. A missing or damaged retained proposal and an ACL refusal surface as typed local outcomes. Cancellation of the requesting backend cannot leave an active publication duty. Native preflight neither acquires ownership nor consumes staged row payloads. The 1.12 recovery stage below consumes the retained decision. The 1.11 preflight remains available for independent local diagnosis. [Native fixture](../../../src/test/lakehouse/fixtures/native-managed-lookup-check.py)

### Native retained-intent reconciliation

Opt-in 1.12 exposes `lakehouse.reconcile_managed_publication_lookup(operation, owner_generation)`. A trusted internal non-superuser has the minimum ledger transition grants for this worker. Give application and general reader roles neither the launcher nor the direct generation and finalize permissions. The operator approves a separate lookup mapping in `lakehouse.publication_lookup_grants` keyed by binding and worker role, with an exact PostgreSQL mapping OID and expiry. The operator owns the credential server. The worker gets no server ownership or `USAGE`, and its credential mapping must be role-specific. The mapping carries a scoped catalog bearer and static S3 key pair in this profile. The current binding and grant are read under SPI. Secrets stay in worker memory and outside the cross-process receipt.

The worker checks the expected generation and retained body, copies the typed request into operation-owned memory, and commits the generation claim. It calls the Rust lookup-only ABI outside SPI and outside a PostgreSQL transaction. The current caps are 16 KiB for the descriptor, 64 KiB for the retained request, and 1 MiB and four seconds for the manifest proof. The underlying scoped REST timeout still applies. A provider acceptance matching the exact retained pin and digests can finalize only under the claimed generation in another short transaction. Missing, contradictory or unavailable proof leaves the original intent held. If a caller loses its reply it rereads the ledger before another lookup. Native recovery has no provider append path.

CI exercises a lost catalog reply and a lost PostgreSQL authorization acknowledgement through native 1.12 after immediate restart. It moves the owner generation while an exact provider reply is in flight and expects `40001` at finalization with no published flip. A fresh native lookup can finish the same intent. The real joined cut also exercises missing, expired and stale approvals. Native scheduling, shorter-lived provider session tokens and native send activation have separate gates. [Mixed-cut fixture](../../../src/test/lakehouse/fixtures/managed-cut-check.py) · [Restart fixture](../../../src/test/lakehouse/fixtures/bound-publication-restart.py)

### Native send authorization hold

Opt-in 1.13 adds `lakehouse.hold_managed_publication_send` for a trusted internal
role and one bounded prepared proposal. The function launches a one-shot
worker on the current active primary. The worker runs the existing
`authorize_publication_send` procedure through a top-level PostgreSQL portal.
That procedure locks and rechecks the approved send binding, reads the exact
membership and persists the prepared request and its digests. Its synchronous
local commit precedes the returned intent XID. The launcher transfers at most
64 KiB of prepared bytes and carries no provider credentials in shared memory.

The launcher command must run standalone on an active primary with no assigned
transaction ID. It returns `55000` before starting a worker if the surrounding
command assigned an XID, which can happen in autocommit SQL. A monotonic
10-second launcher deadline applies even with `statement_timeout` disabled.
On cancellation or deadline expiry, the launcher requests worker termination
and returns `57014`. Resolve an uncertain outcome by lookup. The procedure can
already have committed when a receipt is lost.

The worker stops after the transaction and reports the held intent as
lookup-only. Keep it in a controlled operator context. Holding an unsent intent
reserves the operation and requires later lookup or operator reconciliation.
The hold receipt supplies no dispatch capability, and the worker makes no
provider request. The default extension and scheduler retain their existing
activation gates.

CI races two native hold requests and requires a single authorization. The
lost-acknowledgement test pauses the worker after the actual WAL-flushed
authorization, cancels the launcher and restarts PostgreSQL immediately.
Reopening the ledger must find the original proposal, membership and commit
record, preserve the later segment and discover the held operation for lookup.
A repeated attempt cannot authorize another send. Tying this barrier to a
one-use prepared provider submit capability, independently issued credentials
and native polling has a separate gate. [Native barrier fixture](../../../src/test/lakehouse/fixtures/native-managed-send-hold-check.py)

### Scoped native send preparation

`prepare_publication_send` is the Rust boundary for a future native dispatcher.
Load the exact acquired job and bounded member payloads from PostgreSQL and
release the ledger transaction before calling it. The send observation names
the acquired operation, binding and owner generation. Compare it to the job
before provider work. The scoped transport compares the immutable binding and
table UUID to its independently admitted `Send` credential bundle. Refuse a
reserved or complete job, expired lease, misordered or changed member payloads
and a batch above its byte, row or member budget.

The owned `PreparedManagedSend` keeps the original catalog client and route in
the current process. Its shared request view exposes the exact bounded body,
digest, parent and proposed snapshot for the eventual standalone authorization.
The handle has no public submit method. Dropping it or observing cancellation
before or after preparation sends zero catalog commit POSTs. An in-flight
request may finish before cancellation is observed. Immutable prepared objects
can remain unreferenced after cancellation or a stale authorization; their
retention and cleanup follow the existing writer object policy.

The existing Rust driver still consumes its prepared request only after the
ledger confirms the exact committed authorization. A future native dispatcher
must recheck the current grant and generation at that same transaction boundary
and consume the original in-memory request in the same process. Treat serialized
bytes, the hold receipt and a restarted worker as lookup-only. The native C
executor and its two reply-loss drills have separate gates.

The provider test drops a prepared handle after the transport is released and
separately cancels during the third create-only PUT. Each case observes one
catalog GET, one manifest GET, three object PUTs and zero catalog POSTs. The
parent remains pinned at snapshot 100. Stale generation, wrong scope or purpose,
expired credentials, changed members, oversize and initial cancellation observe
zero requests. [Scoped transport tests](../../../src/backend/access/lakehouse/core/pgdatalake-core/src/publication_transport/tests.rs)

### Process-local native preparation ABI

The generated C header exposes `pg_datalake_prepare_managed_send` and
`pg_datalake_drop_managed_send`. The native caller supplies committed copies
of one acquired unsent job, its source column layout and its ordered members.
The request JSON carries `target`, `operation_id`, `owner_generation`,
`input_digest` as 64 hex characters, and the independently observed
`observation` with mode, operation, binding and owner generation. The
credential adapter separately supplies the approved send target, expiry and
catalog and object credentials, including an optional SigV4 session token.
The Rust transport compares the two targets before provider I/O. The native
caller owns role-specific credential enrollment and purpose checks.

Each descriptor is limited to 16 KiB and the schema to 64 KiB. Credentials
are length-delimited UTF-8 fields of at most 4 KiB each. A preparation
accepts at most 1,024 members, 65,536 rows and 8 MiB of capture payload. The
member payloads use the existing UTF-8 JSON capture format. A refused
preparation clears the result view and returns no owner. On success, the
view borrows the exact request bytes and exposes the parent, proposed
snapshot and SHA-256 digest. Copy what authorization needs while the
owner lives. Drop consumes the owner in the preparing process and clears
its pointer slot. SQL, DSM and WAL never carry the handle or credentials.

The caller closes its PostgreSQL transaction before preparation and checks
for cancellation after Rust returns. An in-flight immutable object write
can complete during cancellation. Preparation and discard send no commit POSTs.
The scoped provider fixture exercises this boundary with one catalog GET,
one manifest GET, three create-only PUTs and zero commit POSTs. Parent 100
stays current. The same test requires zero provider calls for wrong
credential scope, stale ownership, expired credentials and excess members.
This qualifies the preparation and lifetime boundary. One-shot native
authorization and dispatch still require the two reply-loss drills below.

### Native prepare-only diagnostic

Opt-in extension 1.14 adds `lakehouse.prepare_managed_publication_send` for a
trusted operator worker. Request an acquired operation and generation on an
active primary from a standalone read-write command with no assigned XID.
The one-shot PostgreSQL worker loads the exact source layout, ordered
captured members and current binding under short ledger locks. The operator
approves a Send-purpose mapping by binding and role in
`lakehouse.publication_native_send_grants`. This separate record supplies the
exact approved target, mapping OID and expiry. Keep the server and credentials
operator-owned and give the worker neither server USAGE nor direct ledger
mutation privileges. The bounded loader lets this trusted role read its own
scoped source members. Rotate with a newly approved mapping. The initial
mapping adapter supplies a catalog bearer and static S3 keys. Hosted issuer
rotation and temporary session credentials have separate gates.

The worker copies bounded data into its own memory and commits the load
transaction before provider preparation. It drops the Rust owner before
processing pending PostgreSQL cancellation, then reports the parent,
candidate snapshot and request byte count with status
`prepared_discarded`. A launcher deadline of 20 seconds requests worker
termination. A provider operation in progress can finish before the worker
observes termination. A stale or unavailable prerequisite rolls back the
load and returns SQLSTATE without transmitting provider diagnostics.
The receipt carries no credentials, exact proposal body or handle. The
diagnostic creates no WAL send intent and changes no catalog reference.

CI runs the one-shot worker over ordinary captured INSERT with Lakekeeper,
S3-compatible storage and a Trino pin oracle. It checks wrong role, missing
or expired approvals, wrong target and stale generation before catalog I/O.
It also cancels a worker during catalog preparation and forces catalog
unavailability. The claim, generation and staged duplicates stay unchanged
on every path. The
catalog must see zero commit POSTs. Immutable prepared objects may remain
unreferenced until normal cleanup. Use this diagnostic with a disposable or
explicitly approved test target. The default extension and publication
scheduler retain their existing activation gates.

### Native one-shot dispatch

Opt-in extension 1.16 adds `lakehouse.dispatch_managed_publication_send`
for an acquired operation and owner generation. A trusted internal
non-superuser needs the existing send authorization procedure and ledger
transition privileges, the metadata-only scope loader, and the explicitly
granted launcher. Keep the role separate from application and reader
logins. The 1.14 preparation diagnostic retains its narrower grants. The
initial adapter uses the same operator-owned static Send mapping as 1.14.

The worker loads exact captured members, closes its PostgreSQL transaction
and prepares one process-local Rust proposal. It then opens a short
transaction to recheck the generation, current Send approval, exact mapping,
source layout and approved scope. It holds the approval, mapping, foreign
server, binding and claim locks until the authorization commits. The second
load reads metadata and checks member digests under the existing ledger
fence. The original payload bytes stay in the preparing worker.

The same worker runs the top-level authorization portal with the exact
parent, proposed snapshot, input digest, proposal digest and raw body.
A synchronous-local PostgreSQL commit establishes the durable intent. The
portal returns a verified transaction ID before any POST. The worker
consumes its sole Rust owner outside PostgreSQL transaction and SPI state.
The internal ABI compares the exact committed fields to the owned proposal
and clears the owner before calling the provider once. PostgreSQL errors
and cancellation are checked outside Rust. Recovered state and SQL
receipts have lookup authority only.

An acknowledged response and an unknown catalog outcome both require
separately admitted read-only verification before finalization. A lost
PostgreSQL acknowledgement leaves a reserved intent with zero POSTs. An
expired Send credential may decline the attempt after the durable intent.
The durable authorization commit is the boundary for ledger revocation.
Revocation committed first makes the worker refuse authorization. A
revocation committed after authorization leaves the original reservation
for fresh Lookup reconciliation, even if the live worker has yet to send.
A database approval change cannot recall that send. An immediate-disable
contract needs independent network or provider-credential fencing. Repeat
dispatch refuses retained operations before provider I/O.

The [native fixture](../../../src/test/lakehouse/fixtures/native-managed-dispatch-check.py)
forces cancellation before authorization, expiry and owner takeover during
preparation, authorization reply loss and catalog acceptance reply loss. Its proxy
compares the actual POST to the original WAL-flushed body before forwarding
it. The fixture repeats the exact read-only lookup after physical PostgreSQL
restart and keeps the later captured segment pending. This qualifies the
one-shot static fixture path. Scheduled polling, temporary hosted
credentials, promoted-primary failover and managed overlay visibility
have separate gates.

### Discovery and dispatch

Extension 1.9 adds two read-only keyset pages. Both accept a page size of 1 to 1,024. Start with a NULL cursor, advance using the last row even if held and wrap after an empty page. The page reports candidates from one database snapshot. Owners must still use the transactional claim, compare-and-advance and WAL authorization operations after a page is read.

| SQL boundary | Consumer and next action |
|---|---|
| `discover_managed_publication_segments(after_segment, page_size)` | Read segment ID, binding ID, captured row count, payload bytes and eligibility. Group eligible rows under one binding within the worker's byte, row and member limits. An active target reservation or revoked grant holds the work. Claim those exact members transactionally before reading payloads or preparing files. |
| `discover_managed_publication_claims(after_operation, page_size)` | Read operation ID, owner generation, binding ID and `send`, `lookup` or `hold`. New sends still require a current grant. A retained attempt uses lookup even after send-grant revocation or source relation removal. A held operation advances the scan cursor and stays reserved. Reload by operation and verify ownership at dispatch. |

The discovery indexes restrict pages to pending or unresolved managed work. Both interfaces require explicit operator/worker access. A native scheduler consumes these pages, owns poll cadence and fairness, and obtains credential approval from operator state. The deployment issuer integrates through the credential handoff below. The joined test exercises the sequence in the disposable adapter. The default extension version and native background-worker activation retain their existing gates.

### Bounded ledger adapter

Opt-in extension 1.10 supplies the two read boundaries a native worker needs after discovery. `load_managed_publication_job(operation)` obtains the immutable binding and operation, current owner generation, original digest and retained intent in one PostgreSQL snapshot. Reconciliation can load that job after send-grant revocation and source removal. It uses the retained proposal and never needs to read staged payloads.

For new work, `load_managed_publication_members(operation, owner_generation, max_rows, max_bytes)` checks the existing send grant, generation and exact membership under the ledger locks. It measures logical payload length before returning bytes, including TOASTed values. Invalid budgets return `22023`, a stale owner returns `40001`, and a budget refusal returns `54000`. The worker opens no provider connection for a refused load. It keeps the statement transaction short and uses the separate WAL authorization call before any catalog commit.

The SQL-backed worker fixture uses both typed functions and passes a `PublicationDispatchHint` from discovery into either `dispatch_publication_send` or `dispatch_publication_lookup`. The hint carries the operation UUID, binding UUID, owner generation and discovered mode. Hold and wrong-purpose hints do not load a job. The dispatcher checks the fresh job before ownership acquisition. A send wake that observes a retained intent before or during acquisition returns to discovery. A lookup wake receives only a read capability, reloads the same retained attempt under the acquired owner and can finalize only on matched acceptance. `None` requests rediscovery without remote work. Ledger and timeout errors retain their stage and must be isolated so one binding cannot block a poll page.

The native background-worker adapter owns the SPI transactions, cancellation and provider issuer integration. End the discovery transaction before remote calls. Preserve separate committed authorization and finalization boundaries and use current purpose-specific credentials. Reuse the same dispatch functions, exact-pin oracle and fault barriers. [Ledger smoke](../../../src/test/lakehouse/fixtures/publication-ledger-load-check.py) · [Discovery and cut smoke](../../../src/test/lakehouse/fixtures/managed-cut-check.py)

### Credential handoff

The native worker selects the purpose before consulting an operator-approved resolver. For a new send resolve against `get_publication_send_binding`. For lookup resolve against `get_publication_lookup_binding` and the retained operation. The resolver matches the SQL request against an independent approved grant indexed by binding and purpose before invoking the issuer. The issuer receives opaque catalog and object credential references. The resolver's trusted enrollment supplies the target descriptor and explicit purpose together with a catalog bearer and S3 signing material. Return the earliest credential or grant expiry. The Rust [transport](../../../src/backend/access/lakehouse/core/pgdatalake-core/src/publication_transport.rs) requires an exact descriptor and purpose match and checks expiry at construction and when obtaining request credentials. The descriptor includes the binding generation, relation incarnation, catalog route, table UUID, ref and S3 origin and root. It carries an optional S3 session token through SigV4 signing. Provider and operator adapters own credential issuance, policy and revocation.

A send transport can prepare and produce a one-use proposal. A lookup transport can verify a retained proposal. The scheduler obtains a fresh lookup bundle even after a successful send. Refresh can change credentials for the same target and purpose. Persist the descriptor and attempt in PostgreSQL. Keep credentials in memory and out of retained bytes and receipts. If credentials expire after the local authorization but before submission, preserve the intent for lookup-only recovery. A replacement bundle can authorize lookup on the original binding. It cannot reissue a send for the reserved intent.

Provider tests check wrong target, wrong purpose and expiry before I/O, and confirm that catalog and object requests use purpose-specific credentials and that SigV4 carries the session token. The PostgreSQL/Lakekeeper/SeaweedFS fixture verifies both reply-loss barriers with a separate catalog bearer and read-only object identity for recovery. The real cloud issuer, role or prefix policy, refresh, and scheduled native integration each require their own evidence.

**Two different reply-loss barriers.** Begin at a retained parent with `[10]`, commit a bound INSERT of 20, and capture a later INSERT of 30 separately. Record the actual UUID, parent pin, operation, member IDs and intent commit record in both runs.

1. **PostgreSQL authorization reply lost.** Hold the caller after the standalone authorization commits locally and suppress its success response before dispatch. Expect one WAL-flushed intent and **zero catalog commit POSTs**. Restart, allow read-only lookup, and observe the unchanged parent and original reservation. Missing acceptance evidence leaves the claim pending. A timeout, credential refresh or generation advance cannot free its members for another append.
2. **Catalog acceptance reply lost.** Let the originally authorized POST reach the catalog, suppress its reply, and restart. Expect one WAL-flushed intent and **one catalog commit POST in total**, including recovery. With fresh original-scope evidence, finalize exactly the first member. Trino sees `[10]` at the parent and `[10,20]` at the accepted pin. The later member remains pending. A stale or unavailable lookup leaves the intent reserved.

The [joined fixture](../../../src/test/lakehouse/fixtures/bound-publication-restart.py) now covers both barriers with its test adapter. In the first case a procedure in the disposable database calls the original authorization procedure and terminates its own backend after the nested COMMIT. The caller loses the PostgreSQL connection before the outer CALL can acknowledge success. Qualification independently reads the source and intent WAL commit records, compares the retained request bytes before and after immediate restart, and checks the current catalog head and Trino rows. Preparation may already have created files. The zero count measures catalog commit POSTs.

A scheduler with a lookup-only wake calls `reconcile_publication(ledger, operation, lookup)`. Its lookup callback receives the original claim and retained request. An unsent operation returns `None` without ownership acquisition or remote I/O. A completed operation returns its local receipt. A reserved operation takes a generation, rechecks the same immutable attempt, and can finalize only on matching acceptance. Only a separately admitted send job uses `drive_publication` to prepare. Both entry points reject an observed reservation that changes or disappears during ownership transfer. Real credential resolution and the native scheduler must reproduce both fault barriers before their receipts can enable the managed overlay. Publisher wake cadence, promoted-primary recovery and external visibility deadlines keep their own gates.
