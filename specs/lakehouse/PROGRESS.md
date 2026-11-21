# Lakehouse progress and release gates

**Reviewed on 2026-11-21 at `perf-release` `eb8e3602179`. Production qualification is on hold.** The original review is the 2026-11-20 baseline. The [reassessment](16-release-reassessment-and-scale.md) maps all eight findings to current code and acceptance tests. Update this tracker and the handoff whenever a slice lands. Give each milestone its own smoke and measurement receipt.

## What changed since the original review

| Assessment | Evidence | Next action |
|---|---|---|
| **Beautiful** | Immutable object naming and manifest carry-forward repair the original append-history failure. Normal PostgreSQL heap/WAL durability is a sound foundation for staged inserts. The typed batch ABI, projected reads, and decode-on-demand remove work at the right layers. | Preserve these seams. Test old snapshots with an independent reader, and prove ranged early stopping and memory bounds. |
| **Good** | The fork ported to the CF #5631 AM-options contract, added commit-base validation, improved delete/schema handling, added validators and mappings, split the FDW and publisher, and landed dependency audit checks. Fresh local Rust and assertion-enabled SQL suites passed. | Retain the unit and SQL coverage. Extend the required CI path to live success cases, role boundaries, and fault barriers. |
| **Bad** | M3 isolation and cloud parity are incomplete. The all-columns read remains eager. SF10 q1 timed out in the latest receipt, and q2/q3 baselines are unqualified. The benchmark runner lacks snapshot and result-digest qualification. | Fix the test harness, record one comparable baseline and profile the hot paths before choosing the next optimization. |
| **Ugly** | A generic prepared COUNT kept returning 2 after a third row committed. A transaction inserting into 65 staged relations committed with only 64 ledger entries. Managed COUNT also removes query relation metadata, and publication has a remote/local failure window. | Gate the affected paths. Fix COUNT at execution and use durable, exact publication claims with remote reconciliation. Require negative tests before restoring the fast paths. |

## Milestone status

| Milestone | Implementation | Release evidence still needed |
|---|---|---|
| **M0 Foundation** | Historically accepted at `milestone/m0` | Revalidate clean builds, extension install and supported upgrades at each candidate SHA. |
| **M1 External reads** | FDW, REST/S3, pruning, typed scans, time travel and supported v2/v3 delete reads landed | Independent result equivalence, qualified time-travel dump/restore, delete combinations, parallel pinning, cancellation and resource limits. |
| **M2 Managed writes** | Append history, COW DML/MERGE, staged capture and a publisher exist | Exact bulk staging and crash/concurrency visibility. Demonstrate the documented local/remote commit semantics and publish-retry outcomes. |
| **M3 Semantics and cloud parity** | In progress. Ranged projection, cached bytes, lazy projected decode, and initial COUNT paths landed | Qualify COUNT and lazy reads. Close isolation, ADLS/GCS transport and auth, branch/tag/history semantics and the logical replication decision. |
| **M4 Production hardening** | Design available for maintenance, statistics and execution tiers | Implement and qualify compaction, expiry, orphan safety, ANALYZE and bounded scan/publish resource use. |
| **M5 Engine integrations** | Backlog | Compare independently against the same pinned dataset, permission scope, resource budgets and result digests. |

## Evidence and next queue

- **Local baseline.** 118 Rust tests passed and one manual benchmark was ignored. A clean assertion-enabled native build passed 243 upstream regression subtests and all 11 lakehouse SQL groups. Fork CI passed on PRs #27 and #28. See the [review receipts](16-release-reassessment-and-scale.md#evidence).
- **First.** Regress COUNT against prepared-plan reuse, deletes and ordinary query permission semantics. **Done 2026-11-21 (gate):** both plan-time COUNT elisions are off the build (managed hook deleted, FDW upper path unwired); `count_elision.sql` now pins prepared-plan freshness, relation permission checks and the offline staged-serve fallback. Remaining in First: regress staged capture across 65 relations and publisher failure between local and remote decisions. Land correctness fixes before reporting optimization numbers.
- **Second.** Qualify row-group laziness, bounded and cancelable I/O, cache scope, and byte/memory counters. Land the remote positive smoke and result-digest runner in CI.
- **Third.** Profile SF10 and one concurrency workload. Use the measured time split to decide between fewer planning calls, batching an exact aggregate, or improving a join/rescan path. Keep page-index work independent.
- **Milestone completion.** Follow the [TDD and smoke protocol](10-testing.md#milestone-tdd-and-smoke-receipts) and record PASS, FAIL, or NOT RUN against each named gate.

Historical slice notes and receipts are in the [handoff](HANDOFF.md) and [takeover notes](HANDOFF-TAKEOVER.md). Dates, commit IDs, limitations and raw measurements belong with the receipt.
