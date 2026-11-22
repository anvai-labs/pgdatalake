# Managed staged capture enrollment

The first profile covers append-only staged
TableAM writes to one enrolled Iceberg v2 table on `main`. The
[provider crash smoke](04a-publisher-recovery.md#bound-provider-crash-smoke)
starts with fixture rows already carrying a verified authority and Iceberg
UUID. The next cut replaces the fixture's direct ledger insert with ordinary
`INSERT INTO … USING lakehouse` capture. The worker still has a separate
production integration gate. [Managed visibility](03-managed-visibility.md)

## What owns each decision

| Boundary | Required behavior |
|---|---|
| Operator enrollment | Verify the configured catalog authority and approved storage root independently of the catalog response. Resolve the expected Iceberg UUID, `main`, supported profile and schema/spec identity. Retain an immutable enrollment record with durable binding ID, local relation incarnation ID and generation. Store credential references where needed. SQL callers that can only INSERT cannot enroll, revoke or change this association. |
| Foreground capture | Resolve an active enrollment locally on first staged write and retain it through pre-commit. Serialize capture with revocation and reattachment so a commitment under an old generation precedes the reported transition or the write fails. Recheck the state before the first capture under a new transaction. Reject a revoked or changed enrolled generation with `55000`. A persistent property of the local relation records explicit bound-required opt-in. Bound-required capture without an active enrollment returns `55000`. Keep legacy capture available on relations that never opted in. |
| Local ledger | Seal the exact bytes and row count in the source transaction. Store the existing authority and table UUID plus an immutable enrollment reference sufficient to recover binding ID, incarnation, generation and the full approved target. Savepoint rollback removes the affected rows. Full rollback removes the whole capture. The capture path uses PostgreSQL's configured commit durability. Ordinary writers retain their table permissions. A trusted internal path writes only the effective relation's captured rows and immutable fields. The SQL role has no direct ledger read or write privilege. |
| Publisher selection | Use the segment's retained binding. Check the send grant and group a production claim by one eligible generation before using the 1.6 claim adapter. Hold retired generations for reconciliation and account for unresolved sends under the original identity. The current `claim_publication` adapter verifies exact pending rows against authority UUID, table UUID and local relid. Production selection and intent must add the generation constraint. |

The durable relation incarnation is assigned on enrollment. A local OID
locates a relation in the running database. Its reuse after a drop, recreation
or restore cannot authorize a retained segment. A new relation with the same
name can use ordinary legacy attachment. The bound-required property belongs
to the new relation only after opt-in. Only a trusted operator transition may
change that property. Reattachment is an explicit
operator transition. The first profile can require revocation or a drained
cutover before permitting it. The tested sequence is revoke, then explicitly
enroll a replacement. The replacement may use the same catalog authority and
a different table UUID. Old committed segments keep their original target and
enter a hold under the retired grant. Recovery evidence for an already
authorized send remains tied to that old target. New capture uses the new
generation. The legacy `attach_table` path must reject mutation of an
enrolled attachment. Legacy rows stay legacy and require reconciliation
before a cutover. The managed read merge across generations has its own
[visibility gate](03-managed-visibility.md#compatible-published-pin).

## Opt-in PostgreSQL provider

Install extension 1.6 normally and run `ALTER EXTENSION lakehouse UPDATE TO
'1.7'` to exercise bound capture. Installation still defaults to 1.6.
There is no auto-enrollment or automatic adoption of existing segments.
Use a permanent, nonpartitioned managed relation. The bound-required
entry takes precedence over the session write mode and legacy attachment.

The extension owner or administrator opts in with
`lakehouse.require_managed_capture(regclass)`. An active binding can be
installed directly with the following boundary. The operator verifies
the supplied facts before calling it.

```text
lakehouse.enroll_managed_capture(target regclass, target_identity text,
    server_name text, incarnation_id uuid, binding_id uuid, generation bigint,
    authority_id uuid, table_uuid uuid, storage_root text, schema_id integer,
    spec_id integer)
lakehouse.revoke_managed_capture(target regclass, binding_id uuid, generation bigint)
```

Enrollment and revocation run at READ COMMITTED and take an exclusive
lock on the managed relation. A concurrent writer keeps its relation lock
through PostgreSQL commit. The commit inserts the original staged rows
and binding reference in the same WAL-logged transaction. A writer whose
savepoint released the lock must recheck the binding on its next insert.
Admission returns `55000` if the same transaction already opened a legacy
copy-on-write session for the relation.
Stale or inactive bindings return `55000`. An ordinary writer needs its
usual INSERT privilege and can commit without a foreign server grant or
user mapping. The provider never reads user mapping secrets during capture.

The provider stores the foreign server reference, its configured catalog
and object endpoints, and the approved Iceberg identity and storage root.
The source schema signature records column positions, names, types, type
modifiers, collations, nullability and dropped column positions.
Generated columns are outside this profile. A local signature change
returns `55000` until the operator revokes the old generation and approves
the new schema against Iceberg. The slot layout is checked when it changes
within a transaction. The durable signature is checked on initial capture
and before commit serialization. The stored payload is UTF-8 and its
digest covers the exact bytes.

The extension upgrade enables the local capture contract. Bound scans
and COUNT return `0A000` pending the compatible-pin overlay. UPDATE,
DELETE, and remote creation are outside this profile. Production
publication and customer read-your-writes require their own gates.

## First executable red

The [foreground binding fixture](../../../src/test/lakehouse/fixtures/managed-capture-binding-check.py)
uses a disposable PostgreSQL installation with extension 1.6 and a loopback
recorder for both configured endpoints. Its baseline adapter stores the
mock operator facts beside the fixture and delegates attachment to the
existing SQL function. It never writes to the capture ledger. After
enrollment, all staged INSERT, rollback, claim and OID-lifecycle observations
come from ordinary SQL and the existing ledger.

```sh
python3 src/test/lakehouse/fixtures/managed-capture-binding-check.py --expect-baseline-red
```

At parent `1e1150ddb7b4b4f398ac5650112a1b0682c11324`, the administrator
commits the duplicate payload `[[20],[20]]` and later `[[30]]`. The rows
carry NULL authority, UUID and digest. Claiming the first segment returns
`55000`. A role granted ordinary INSERT fails with `42501` from the
private ledger. The legacy attachment path accepts an unapproved replacement,
and the test-side revocation still lets legacy capture commit `[[40]]`.
The test observes zero foreground HTTP requests. It confirms the positive
legacy and rollback controls, including a refreshed legacy attachment at the
old relation name, then checks the exact eight red markers. Its expected
red mode can run before the new provider exists.

## Candidate fixture adapter and acceptance

Run the same fixture with `--provider` to use the included PostgreSQL
adapter, or supply `--adapter PATH.sql` to select another adapter. The adapter defines
`capture_fixture.enroll`, `capture_fixture.revoke`,
`capture_fixture.require_binding` and `capture_fixture.provenance` in the
disposable database. The signatures in
the fixture are an acceptance interface. The implementation may map them to
approved product or test APIs. The `require_binding(target)` helper opts a
relation into persistent bound-required mode before enrollment. In baseline
mode it only records the intended opt-in. The baseline still reports `0A000`
for a relation with no attachment. The provenance helper returns the array
`[binding_id, incarnation_id, generation]` from the captured segment's
durable reference. An unbound segment returns three NULLs. The helper must
read the captured reference without consulting the current attachment or
inventing missing values. Enrollment and revocation may seed or verify
operator facts. All capture rows must come from the TableAM path without a
fixture trigger or direct INSERT into the ledger.

| Step | Candidate observation |
|---|---|
| Legacy control | Existing unbound relation commits `[[10]]` with NULL authority, table UUID and digest. Bound capture does not relabel it. |
| First ordinary capture | Writer role with INSERT and without ledger write access commits `[[20],[20]]` with count 2 and a digest matching the exact payload. The stamp identifies generation 1 of the enrolled incarnation. Savepoint value 98 and full-rollback value 99 never appear. Direct low-privilege ledger SELECT, INSERT, UPDATE and DELETE each return `42501`. |
| Claim and late capture | The existing claim adapter returns the expected digest and stores exactly the first segment as a member. Later `[[30]]` has a different segment ID and remains outside that claim. A second claim on the same unresolved target returns `40001`. |
| Revoke and replace | A legacy reattachment attempt returns `55000`. After explicit revoke, staged value 40 is refused with `55000`. Approve generation 2 with a new table UUID on the same authority. Value 50 is stamped with generation 2. The old segments and hashes remain byte-for-byte identical. A claim using the replacement UUID for the old late segment returns `55000`. |
| Drop and recreate | Recreate the name as a legacy relation, attach it and commit value 61 with a NULL binding. Recreate again with explicit bound-required opt-in. Before enrollment it refuses value 60 with `55000`. Fresh enrollment can point at the same remote UUID and root as its predecessor. Value 70 carries a new binding ID and relation incarnation at generation 1. Prior ledger references remain intact. |
| Request budget | The loopback recorders observe zero foreground requests after each successful enrollment. |

The provider fixture observes two relation lock waits. It verifies a
committed writer keeps the original binding and a savepoint-rolled-back
writer cannot reuse it after a concurrent revoke and reattach. It also
checks ordinary writer privileges, caller-chosen search path, and schema
drift. Add `--restart-fixture PGDATA` against a disposable server to force
an immediate shutdown and compare the recovered binding, claim, member,
and payload rows. The restart check requires `fsync=on` and
`synchronous_commit=on`.

In the publication integration gate, replace the two explicit ledger inserts in the
[bound-provider crash smoke](../../../src/test/lakehouse/fixtures/bound-publication-restart.py)
with enrolled TableAM INSERTs, look up their generated segment IDs, and keep
the existing one-POST, WAL, late segment and exact pinned row assertions.
The first real gate uses parent `[10]`, source row `20` and the later row
`30`, preserving the accepted pin result `[10,20]`. Both the ordinary
capture result and the crash result must pass before enabling publication
from foreground-captured rows.
