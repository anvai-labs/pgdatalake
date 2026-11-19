# 07 — Transactions, MVCC, and Recovery

The hardest part of the design. Postgres MVCC (tuple versions, WAL, snapshot
xmin/cxmax) and Iceberg MVCC (immutable snapshots, optimistic catalog commits)
are different systems; this doc pins the exact semantics we commit to.

## 1. Visibility model

| Scope | Heap behavior | Iceberg behavior (managed & external) |
|---|---|---|
| Statement | sees a snapshot at statement start | sees the **catalog ref snapshot pinned at statement start** (or transaction start with `lakehouse.mvcc = transaction`) |
| Transaction | consistent across statements | re-pins per statement by default; `SET lakehouse.mvcc = transaction` pins at `BEGIN` (snapshot-honest REPEATABLE READ-like) |
| Own uncommitted writes | visible via self-MVCC | visible via an **in-transaction overlay**: staged batches + a local delete-set are read back before commit |
| Other backends' uncommitted writes | invisible | invisible (only committed snapshots are readable — same as heap) |
| REPEATABLE READ / SERIALIZABLE | full PG semantics | pinned-at-BEGIN snapshot; SERIALIZABLE adds **conflict detection on commit** (§3) |

The overlay (own uncommitted writes visible to own reads) is mandatory —
`INSERT … RETURNING`, `UPDATE … WHERE id IN (SELECT …)`, triggers reading the
mutating table all depend on it. Implemented as an AM-private ephemeral row store
per (rel, xid) in backend-local memory (+ spill files > `lakehouse.overlay_spill_mb`,
default 64MB, in `pg_tmp`), deleted at commit/abort.

## 2. Commit protocol (managed write, single table)

```
PRE-COMMIT (at CommitTransaction):
  1. flush staged batches → Parquet data files / DV blobs in <location>/data
  2. build Iceberg commit: AppendFiles / OverwriteFiles / (Eq)DeleteFiles
     + requirements [AssertRefSnapshotId(main, base_snapshot)]
  3. commitTable via catalog, retry ≤ lakehouse.commit_retries w/ backoff
       - on requirement conflict: re-plan the delta (re-read affected files'
         current state), rebuild commit, retry (bounded)
COMMIT:
  4. mark pg_lakehouse_table snapshot pointer, invalidate relcache for self
  5. WAL: only the pg_class/pg_lakehouse_table catalog row updates (normal WAL)
POST-COMMIT (async):
  6. reportMetrics to catalog
ABORT:
  7. staged files are orphans → orphan GC after lakehouse.orphan_grace_period
```

**Failure windows are explicit:**
- crash between (1) and (3): orphans, cleaned later. No visible effect.
- crash between (3) and (4): the Iceberg commit succeeded; on next metadata
  load Postgres reconciles (remote catalog is source of truth). A user-visible
  anomaly window exists between the remote commit and local reconciliation —
  bounded by `lakehouse.metadata_ttl`; remote-committed data becomes visible then.
  This is the same guarantee class as every other multi-system Iceberg writer.
- catalog commit fails after retries: transaction aborts; data files orphaned.
  **No partial commit ever exists remotely** — that is Iceberg's core guarantee
  and we rely on it.

## 3. Isolation & conflict semantics

- **Read Committed (default)**: statement-level snapshots; two concurrent writers
  to the same table both succeed unless their file sets overlap after re-plan —
  Iceberg commit-time requirements detect overlap and we retry; non-overlapping
  updates commit independently (row-level conflicts within the same file are
  resolved by COW rewrite re-reading the file at retry time — last writer wins
  per-row, documented as Iceberg semantics, *not* PG row-lock semantics).
- **REPEATABLE READ**: BEGIN-pinned snapshot; commit conflict →
  `iceberg_serializable_failure` (40001-class) like PG's RR.
- **SERIALIZABLE**: same as RR + we set `AssertCreateSnapshotUnchanged`-style
  requirements; SSI read-write edge tracking is *not* propagated to Iceberg
  (documented limitation: SERIALIZABLE means RR conflict detection, not full SSI,
  across mixed engine tables). A WARNING is emitted once per session when a
  SERIALIZABLE transaction mixes heap and iceberg writes.
- **Row locking**: `SELECT … FOR UPDATE` on iceberg tables → ERROR v1
  (no row locks exist in Iceberg); advisory locks + `lakehouse.lock_table()` helper
  (a dedicated Iceberg table for application-level locks) cover the need (v2).

## 4. Multi-table atomicity

- REST catalog advertising multi-table commit (`CommitTransactionRequest`): all
  tables in one atomic catalog transaction. Postgres-side ordering: all remote
  commits must succeed or none is reported committed — implemented by staging
  all `commitTable` payloads and sending them in one `transactions` request.
- Non-supporting catalogs (Glue, Nessie native): **two-stage commit with journal**:
  1. write a pending-commit journal row (WAL-protected) listing per-table commits
  2. commit tables sequentially; on first failure, run compensating
     rollback commits (reverse operations) for already-committed tables
  3. mark journal `completed` at Postgres commit
  Crash between remote commit and journal completion → recovery at startup
  reconciles: completed-journal tables stay committed; incomplete journals roll
  forward (all-or-nothing was already established remotely) or roll back per the
  compensation rule. Mixed heap+iceberg transactions: **heap commits first**, then
  iceberg remote commits; if the remote phase fails, the heap part is already
  committed → we report ERROR `iceberg_commit_failed_partial` with the journal id
  for manual/automated repair (`lakehouse.repair_transaction(journal_id)`).
  This is a documented, unavoidable boundary of two independent commit systems —
  honest and rare (only non-REST catalogs + mixed tables).

## 5. WAL, replication, recovery

- Data is WAL-free (object storage is the durability). Catalog-row changes and the
  commit journal are WAL-protected as usual.
- **Physical streaming replication**: replica reads Iceberg tables from the same
  remote catalog (read-only IO contexts); staged overlays of uncommitted
  transactions are NOT replicated (they're backend-local). Snapshot-visible state
  on replica = committed remote snapshots at replay position. Consistent with
  logical semantics; document that a replica can see a *newer* remote snapshot
  than its replay position if another engine committed concurrently (unavoidable,
  bounded by `metadata_ttl`).
- **Logical replication**: managed iceberg tables publish like normal tables
  (row-image decode from the overlay at decode time — changes are decoded from
  the journal + staged commit payload, not from WAL row images, since data is not
  in WAL). v2: logical decoding plugin reading journal. v1: publication on
  iceberg tables → ERROR "logical replication of iceberg tables: v2".
- **Crash recovery**: nothing to redo (remote commits are atomic); journal
  reconciliation only.

## 6. What we explicitly do NOT claim

- Not single-system ACID across heap+iceberg on non-multi-commit catalogs (§4).
- Not serializable cross-engine (another engine's snapshot discipline is theirs).
- Not sub-millisecond OLTP update latency (object-storage round trips per
  transaction commit: expect ≥ 1 RTT × (1 + retries) per table touched; tuned for
  analytical + micro-batch + app-server workloads, not point-update TPS).
