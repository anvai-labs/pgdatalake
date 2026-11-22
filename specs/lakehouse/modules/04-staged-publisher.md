# Anvai Lake staged publisher

**Read** [shared contracts](00-integration-contracts.md) and [publication timing](../17-anvai-lake-product.md#4-publication-by-size-or-age). **Own** `publisher.c`, worker scheduling, claim orchestration, receipt state and reconciliation. PostgreSQL ledger persistence belongs to the visibility module. Rust writer and catalog modules provide file assembly, CAS and operation lookup.

**First implementation slice.** Start with [staged append recovery](04a-publisher-recovery.md). It defines the lost-reply regression, the short PostgreSQL transactions on either side of catalog submission, and the retained evidence used for read-only reconciliation. The [native worker handoff](04b-scoped-send-admission.md#native-worker-integration) defines the next integration gate.

## Implementation

The scheduler considers size or oldest committed segment age. The initial target is 128 MiB per file with 32 MiB–4 GiB configurable bounds, and a proposed 60-second default age target (experimental 20 seconds, proposed range 5–300 seconds). A time-triggered flush can emit a smaller file. Cap each claim by bytes, rows and segment count. Wake after commit and use a bounded periodic recovery scan. Do not hold a SQL transaction across unbounded network waits.

| Persistent stage | Transition and rule |
|---|---|
| Eligible | Persist an exact claim and stable operation ID before uploading files. Record table UUID/ref and membership digest. |
| Claimed | Acquire or renew an ownership generation. Resume abandoned work with the same operation ID. Persist attempt evidence needed for recovery. |
| Submitted / unknown | In 1.8 an existing intent admits lookup only, including after a catalog conflict. Preserve its original members, request and reservation until acceptance can be proven. Another send would require a future durable rejection transition. |
| Verified | Persist the verified snapshot and complete exactly those claimed segment IDs. Check lease ownership and update cardinality. A stale owner cannot finalize. |
| Needs reconciliation | Hold pending rows and surface a receipt. Retry lookup under a bounded policy. Release rows only when an operator or verified catalog outcome resolves the state. |

Lease expiry coordinates local work. Concurrent attempts must use the same claim identity and reconcile before a stale attempt can introduce duplicate effects. Gate profiles that cannot prove safe handling after loss of a reply. Engine visibility attaches to a catalog snapshot, not worker completion time.

## Eligibility, small files and backpressure

Measure file rollover in encoded output bytes. Measure eligibility using a separately bounded pending payload byte estimate and elapsed age of the oldest committed member. Calibrate the estimate against actual encoded Parquet bytes. Use the client COMMIT acknowledgement timestamp for external measurement; an enqueue or transaction-start time cannot certify it. Persist due work and rebuild eligibility from the ledger after restart. Reject zero for a new age setting or explicitly migrate it to disabled with bounded scheduling. The current 30-second `publish_max_lag` is a poll interval.

Under the simplifying assumption of steady **encoded** flow at 0.1 MiB/s, a time-only 20-second flush produces about 2 MiB and 4,320 publishes/day per continuously active table. At 60 seconds it produces 6 MiB and 1,440/day. At 8 MiB/s a 128 MiB target fills in 16 seconds. These are models assuming one file/flush and immediate service. A 32 MiB setting controls desired rollover, never a minimum legal time-triggered file. Gate a 4 GiB setting on measured streaming memory and multipart limits. A 60-second eligibility target leaves no processing margin for a 60-second end-to-end p95 on quiet tables; evaluate the latter only with a shorter age budget and known upload/catalog/refresh conditions.

Apply per-tenant and global pending-byte, WAL/disk reserve and concurrency budgets. Reject new work before COMMIT once reserves are exhausted, retain acknowledged work, report oldest age and resume with hysteresis. Compaction must preserve row multiplicity and old pinned views. Protect unresolved objects, claims and overlay payloads from GC. Test a stalled catalog, rising small-file count, compaction and restart.

## Independent test package

- With fake time and fake adapters, test age/size triggers, bursts, fairness, reload, backpressure and stable receipt after wait timeout. Count claim sizes, pending age, file sizes and recovery duration.
- Stop before upload, after upload, after catalog success and before local finalization. Repeat with takeover and a concurrent INSERT. Verify exact claims and recover the `publish-reply-lost` vector.
- Smoke against Lakekeeper with two independent readers and a restart. Record local acknowledgment, claim, verified catalog and fresh consumer latencies separately. Prioritize higher throughput or multi-table publication based on backlog and correctness evidence.
