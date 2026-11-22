# Core release contracts and milestone receipts

**Owner** Qualification (08). Baseline `1acdf09f4f3fb90b7bfebb8bbe8b500c26ec967c`. The [fork CI](https://github.com/anvai-labs/pgdatalake/actions/runs/44987466812) passed native, Rust and live integration jobs. Direct COW and catalog reply-loss fixtures exercise running software. Modules 01/03/04 have separate tests for local WAL recovery, scoped catalog lookup and preparing real immutable Avro objects. Ordinary staged capture still inserts unbound segments, the legacy worker filters for them, and staged scans skip the published base. The following release gates are **NOT RUN** as complete customer journeys. [Current worker](../../../src/backend/access/lakehouse/publisher.c) · [Managed scan and capture](../../../src/backend/access/lakehouse/tableam.c)

| Gate, owner and dependencies | Reproduce failure first | Required smoke after integration |
|---|---|---|
| Stock FDW, 06 with 01+02 | Build/install using only released PostgreSQL headers. Detect fork-only parser/AM dependency or a missing delete in a pinned scan. | Fresh install, upgrade and dump/restore. Use fixture F at 100 with `[10,20,20,30]`. External writer position-deletes the second 20 at ordinal 2, yielding 101 with `[10,20,30]`. Compare exact typed rows in FDW and a reference engine at both pins. Reuse a prepared plan across ref movement. Cross-tenant access to the denied root makes zero requests there. |
| Managed source binding and publisher, 03/04 with 01+05 | Normal INSERT at the reviewed SHA stores no verified binding. Kill after remote acceptance; a legacy worker cannot safely recover a single send. | Separate fixture P, base 100 `[10]`. Segment 7=20, commit. Drop reply after acceptance of 101, add segment 8=30, immediately stop PG. Recover actual WAL, inspect original expected parent, bytes/digest, source and intent commit records. Count 1 POST and 1 accepted append for 7. Verify 7 complete, 8 pending, rows `[10,20]` at 101. Publish 8 once to 102 and verify `[10,20,30]`. No ambiguous legacy adoption. |
| Managed overlay/receipt, 03+06 after preceding provider seams | A staged scan misses existing rows; stale ledger status can double-count. | Replay the mixed batch with both 7 and 8 containing 20. Verify old/intermediate/new PG snapshots, rolled-back write, historical and current pins, timeout, least-privilege waits and exact COUNT. Remove the compatible parent and require failure before first row. Exercise multi-table statement and explicitly decline unsupported isolation. [Client contract](03a-client-receipts.md) |
| Catalog/storage expansion, 01+02, per operation and cloud | Scope or immutable-create oracle fails, or the adapter cannot open the cloud scheme. | Replay the stock fixture and negative scope tests using real tenant credentials and external reader. For reads collect range requests and bytes. For write qualification additionally prove create-only collision and staged recovery after real acceptance. Record catalog and storage versions separately. [Cloud profiles](09-cloud-profiles.md) |
| Size/age, 04 after durable recovery | A quiet table never fills a file, or after restart a due claim is skipped or resubmitted. | Test equality and just-below thresholds with a fake clock. Run low/steady/burst, outage, recovery and compaction with real files. Check exact rows at retained pins, request counts and lag from client COMMIT ack to first external read. |
| HA consumer, core plus operator evidence | Promoted standby lacks a locally flushed source or intent commit record. | Disable the old writer and its publisher, promote another host, record system ID/timeline, recovered source/intent commit LSNs, actual original POSTs, receipts and both reader results. A restore missing evidence holds sends. Keep this capability closed in an otherwise green single-primary release. |

Fixed IDs are fixture symbols. On live catalogs record the concrete UUID, ref, actual parent/child IDs and external query pin at every step. Compare sorted typed multisets, including duplicates. Record zero, one and rejected/ambiguous POSTs at the transport boundary. Preserve a terminal outcome for every offered query. Capture SHA, package digest, PG major, toolchain, config, versions, connection/role, bytes, sample count, raw times, p95/p99 or insufficient samples. Separate observation, hypothesis and release objective. Real provider evidence applies only to its tested combination. Retain existing [smoke receipt format](../10-testing.md#milestone-tdd-and-smoke-receipts).

Qualification also records the request/byte trade-off and publication cadence
against the [I/O and publication cost gates](08b-io-cost.md).

### Joined managed capture and recovery fixture

The native fork CI job exercises [ordinary capture enrollment](03b-managed-capture-binding.md)
and the [durable send/recovery driver](04a-publisher-recovery.md) in one workflow.
Use the opt-in extension upgrade and a low-privilege PostgreSQL writer. Source
the claim members from ordinary SQL-generated segment IDs and payloads. Observe
zero foreground catalog requests. Test both immediate termination after remote
acceptance and a returned ambiguous failure. On recovery require one original
commit POST, one WAL-flushed intent with the original bytes, exact independent
rows at both pins, and an unaffected later segment. Replaying a completed
operation preserves the receipt and issues zero catalog requests.

The fixture trusts a disposable operator's explicit enrollment facts. Production
enablement needs the native scheduler, durable scope resolved from each member's
binding and generation, active grant enforcement at the send fence, and a
zero-forbidden-request drill. The managed read overlay and promoted-primary
recovery each need their own release gate.

**Safe parallel starts.** Stock packaging, scoped cloud reads, ledger enrollment and independent exact-row fixtures may start against the shared contract. Integrate the actual writer only after verified capture and local send durability. Enable managed reads only after membership and real worker receipts. Service routing may consume pinned reads independently of managed capture. Serialize SQL upgrade numbers, shared ABI/header exports and fixture semantic changes. Freeze a digest for each set of vectors; provider and consumer must pass the same digest before claiming integration.
