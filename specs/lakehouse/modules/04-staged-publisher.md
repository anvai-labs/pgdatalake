# Anvai Lake staged publisher

**Read** [shared contracts](00-integration-contracts.md) and [publication timing](../17-anvai-lake-product.md#4-publication-by-size-or-age). **Own** `publisher.c`, worker scheduling, claim orchestration, receipt state and reconciliation. PostgreSQL ledger persistence belongs to the visibility module. Rust writer and catalog modules provide file assembly, CAS and operation lookup.

**First implementation slice.** Start with [staged append recovery](04a-publisher-recovery.md). It defines the lost-reply regression, the short PostgreSQL transactions on either side of catalog submission, and the evidence needed to authorize another attempt.

## Implementation

The scheduler considers size or oldest committed segment age. The initial target is 128 MiB per file with 32 MiB–4 GiB configurable bounds, and a separate default age target of 20 seconds with the proposed 5–300 second bounds. A time-triggered flush can emit a smaller file. Cap each claim by bytes, rows and segment count. Wake after commit and use a bounded periodic recovery scan. Do not hold a SQL transaction across unbounded network waits.

| Persistent stage | Transition and rule |
|---|---|
| Eligible | Persist an exact claim and stable operation ID before uploading files. Record table UUID/ref and membership digest. |
| Claimed | Acquire or renew an ownership generation. Resume abandoned work with the same operation ID. Persist attempt evidence needed for recovery. |
| Submitted / unknown | Before any retry, verify whether this operation already published. A proven rejection permits another compatible attempt with the same operation ID. Unknown remains recoverable and cannot create a new append. |
| Verified | Persist the verified snapshot and complete exactly those claimed segment IDs. Check lease ownership and update cardinality. A stale owner cannot finalize. |
| Needs reconciliation | Hold pending rows and surface a receipt. Retry lookup under a bounded policy. Release rows only when an operator or verified catalog outcome resolves the state. |

Lease expiry coordinates local work. Concurrent attempts must use the same claim identity and reconcile before a stale attempt can introduce duplicate effects. Gate profiles that cannot prove safe handling after loss of a reply. Engine visibility attaches to a catalog snapshot, not worker completion time.

## Independent test package

- With fake time and fake adapters, test age/size triggers, bursts, fairness, reload, backpressure and stable receipt after wait timeout. Count claim sizes, pending age, file sizes and recovery duration.
- Stop before upload, after upload, after catalog success and before local finalization. Repeat with takeover and a concurrent INSERT. Verify exact claims and recover the `publish-reply-lost` vector.
- Smoke against Lakekeeper with two independent readers and a restart. Record local acknowledgment, claim, verified catalog and fresh consumer latencies separately. Prioritize higher throughput or multi-table publication based on backlog and correctness evidence.
