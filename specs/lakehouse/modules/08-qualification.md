# Anvai Lake integration qualification

**Read** [shared contracts](00-integration-contracts.md) and the [milestone smoke protocol](../10-testing.md#milestone-tdd-and-smoke-receipts). **Own** shared test fixtures, stateful catalog fake, deterministic barriers, CI orchestration, cold-room receipts and `scripts/perf/`. Each provider/consumer owns its own unit and SQL regression changes.

## Test layers

| Layer | Required output |
|---|---|
| Shared fixture | Versioned inputs and exact expected outputs for both provider and consumer, based on [contract-v1.json](fixtures/contract-v1.json). Add large ID/null/error vectors before wire changes. |
| Isolated module | Failing test on the parent, passing module test, and a stub using the same shared interface as production. Fake clocks for scheduling and deterministic barriers for races. |
| End to end | Assertion build with SQL, Lakekeeper, actual Iceberg files, and an independent reader. Include a stock package test where supported. Compare ordered rows or typed multiset results and specific snapshot IDs. Gate unsupported input with a negative case. |
| Milestone | Clean build, fresh install and upgrade where relevant, repeated smoke checks and separately measured release-build performance. Archive exact SHA, dataset, toolchain, engine/catalog versions and raw test/measurement receipts. |

## First integration scenarios

1. Resolve one snapshot and independently verify exact FDW and managed published results. Use old and new pins, schema mapping, existing delete files and empty projection.
2. Verify managed current view with concurrent publish and backend failure between catalog and local finalization. Exercise both overlay directions and the 65-relation regression. Assert exact logical rows and exactly one published operation.
3. Prove COW fail-closed and then delete-safe rewriting. Verify a still-live file with changed deletes forces rejection or correctly qualified replan. Check that historical rows stay intact.
4. Start the qualification benchmark only after datasets and digests match. Collect planning/first-row and full query latency, CPU, HEAD/GET count, bytes, decoded groups, peak RSS, FFI transport, foreground commit, publication lag and external visibility. Record real timeouts. Compare with optimization disabled and include controlled cache states.

Only one change modifies the shared fixture vocabulary and CI integration while modules consume it. Treat capability receipts as PASS, FAIL or NOT RUN. A green component suite permits integration. Release qualification requires the positive path and failure cases for the advertised capability.
