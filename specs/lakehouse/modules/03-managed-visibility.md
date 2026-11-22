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

- Run the managed overlay vectors with a fake published view and ledger membership responses. Verify older base/new ledger, new base/stale ledger, future operation and unknown-membership cases. Assert exact rows, including duplicates.
- Run `mixed-visibility-claim` through pin selection and ordinary SQL scans. Two segments share one verified publication. A reader between their source commits falls back to the retained parent and adds just its visible segment. Dropping that parent gives an explicit consistency error. A reader after both commits can use the published pin directly.
- SQL regression for 65 relations, nested SAVEPOINT rollback, prepared plans, ordinary roles/RLS, mode change, relid reuse and catalog outage. Isolate a provably empty offline case to exercise staged reads during an outage.
- Two sessions plus a controlled publication barrier verify one compatible cut per statement, stable rescans and no lost or double-counted rows. Compare COUNT fast path against the ordinary scan. Gate higher isolation levels separately.
