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

A credential refresh may change secret material while retaining the authorized scope. It must preserve the exact catalog authority, table, ref, object origin and root. Resolve credentials for their operation only. Persist the descriptor, operation and request evidence in the ledger. Keep bearer tokens and storage secrets out of retained bytes, errors and receipts. The current S3 options carry an access key and secret. A profile that also requires a session token needs a transport capable of retaining and using it. Keep that profile disabled until a real scoped request and expiry case pass.

**Two different reply-loss barriers.** Begin at a retained parent with `[10]`, commit a bound INSERT of 20, and capture a later INSERT of 30 separately. Record the actual UUID, parent pin, operation, member IDs and intent commit record in both runs.

1. **PostgreSQL authorization reply lost.** Hold the caller after the standalone authorization commits locally and suppress its success response before dispatch. Expect one WAL-flushed intent and **zero catalog commit POSTs**. Restart, allow read-only lookup, and observe the unchanged parent and original reservation. Missing acceptance evidence leaves the claim pending. A timeout, credential refresh or generation advance cannot free its members for another append.
2. **Catalog acceptance reply lost.** Let the originally authorized POST reach the catalog, suppress its reply, and restart. Expect one WAL-flushed intent and **one catalog commit POST in total**, including recovery. With fresh original-scope evidence, finalize exactly the first member. Trino sees `[10]` at the parent and `[10,20]` at the accepted pin. The later member remains pending. A stale or unavailable lookup leaves the intent reserved.

The [joined fixture](../../../src/test/lakehouse/fixtures/bound-publication-restart.py) now covers both barriers with its test adapter. In the first case a procedure in the disposable database calls the original authorization procedure and terminates its own backend after the nested COMMIT. The caller loses the PostgreSQL connection before the outer CALL can acknowledge success. Qualification independently reads the source and intent WAL commit records, compares the retained request bytes before and after immediate restart, and checks the current catalog head and Trino rows. Preparation may already have created files. The zero count measures catalog commit POSTs.

A scheduler with a lookup-only wake calls `reconcile_publication(ledger, operation, lookup)`. Its lookup callback receives the original claim and retained request. An unsent operation returns `None` without ownership acquisition or remote I/O. A completed operation returns its local receipt. A reserved operation takes a generation, rechecks the same immutable attempt, and can finalize only on matching acceptance. Only a separately admitted send job uses `drive_publication` to prepare. Both entry points reject an observed reservation that changes or disappears during ownership transfer. Real credential resolution and the native scheduler must reproduce both fault barriers before their receipts can enable the managed overlay. Publisher wake cadence, promoted-primary recovery and external visibility deadlines keep their own gates.
