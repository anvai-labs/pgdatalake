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
