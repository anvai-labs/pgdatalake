# Anvai Lake integration qualification

Start each new qualification run from the [core release gate matrix](08a-core-release-gates.md). It names the independent oracle, exact multiplicities, failure point and expected recovery evidence.

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

## Pinned performance receipt

The [canary runner](../../../scripts/perf/README.md#pinned-qualification-for-q1q3)
accepts a trusted independent-engine oracle for the shipped TPC-DS q1–q3 SQL.
The oracle records the dataset, engine version, eight table locators and
explicit Iceberg snapshot IDs, bound REST/S3 server identities, exact result
multiplicity, typed row digest and SQL hashes. Use PREPARE to hold relation locks
before inspecting the local attachments without starting an FDW scan. Run
against a fixed catalog/profile, server mapping and fixture schema. Validate
every timed result against the same oracle. Keep raw elapsed samples and the
first mismatch or SQLSTATE per query in a fresh JSON output file. Bind the surrounding
deployment receipt to the supplied source SHA and package digest.

Use the median after five verified samples and nearest-rank p95/p99 after 100.
A missed or failed sample prevents a passing canary. Keep CPU, RSS, I/O,
network request counts and independently established cache state in the
deployment receipt. The runner captures PostgreSQL settings and client wall
clock, and classifies cache state only by whether its warmup ran.
