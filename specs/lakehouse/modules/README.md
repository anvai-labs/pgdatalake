# Anvai Lake module specifications

Start with the module you are implementing and [the shared contracts](00-integration-contracts.md). The product and rollout decisions remain in [Anvai Lake product](../17-anvai-lake-product.md) and [DML](../18-anvai-lake-dml.md). Module specs own implementation decisions and tests. The shared contract owns interface semantics.

| Work package | Read next | Primary source ownership | First independent delivery |
|---|---|---|---|
| Catalog and object storage | [01-catalog-storage](01-catalog-storage.md) | Rust catalog, storage, cache and session client | Verified load/commit against a fake catalog, scoped byte reads |
| Iceberg reads and batch transport | [02-iceberg-reader](02-iceberg-reader.md) | Manifests, scan planning, Parquet, Puffin, reader-side FFI and C batch conversion | Exact typed rows and physical ordinals on pinned mock data |
| Managed PostgreSQL visibility | [03-managed-visibility](03-managed-visibility.md) | TableAM read/transaction capture, local ledger schema and managed COUNT | Managed base + overlay evaluated against a fake pinned view |
| Staged publication | [04-staged-publisher](04-staged-publisher.md) | Publisher worker, durable claims and receipt transitions | [Lost-reply recovery](04a-publisher-recovery.md) with deterministic ledger and catalog checkpoints |
| Direct Iceberg mutations | [05-iceberg-mutations](05-iceberg-mutations.md) | Rust writer and snapshot assembly, direct COW orchestration | Delete-safe rewrite or explicit decline with an independent oracle |
| SQL and stock FDW package | [06-sql-and-packaging](06-sql-and-packaging.md) | FDW planner/executor, SQL diagnostics, extension install/build packaging | Stock PostgreSQL read package and fork ABI/install smoke |
| Heap CDC ingestion | [07-heap-cdc](07-heap-cdc.md) | Candidate connector and its checkpoint store | Replay-safe append log from deterministic transaction batches |
| Qualification | [08-qualification](08-qualification.md) | Fixtures, milestone smoke, CI orchestration and performance runner | Shared fixtures and a result/measurement receipt |

Cross-cutting module specs (owned by their work package, indexed here for
discoverability): [BI reuse](02a-bi-reuse.md) (02),
[capture binding](03b-managed-capture-binding.md) (03),
[client receipts](03a-client-receipts.md) (03),
[scoped send admission](04b-scoped-send-admission.md) (04),
[publisher recovery](04a-publisher-recovery.md) (04),
[release contract](06a-release-contract.md) (06),
[scoped read handoff](06b-scoped-read-handoff.md) (06),
[governed published read](06c-governed-published-read.md) (06),
[core release gates](08a-core-release-gates.md) (08),
[I/O cost](08b-io-cost.md) (08),
[cloud profiles](09-cloud-profiles.md) (01),
[BI marketplace](09a-bi-marketplace.md) (02),
[downstream handoff](10-downstream-handoff.md) (00).

## Delivery order and dependencies

### Contracts for independent work

The [core-to-operator handoff](10-downstream-handoff.md) assigns a producer, consumer, failure owner and joint test to each cross-repository seam. Start parallel development with its stock-reader receipt and exact consumer decision cases.

| Independently owned slice | Freeze with these consumers | First integration gate |
|---|---|---|
| 01 cloud adapters, with a maintainer per AWS/Azure/GCP storage API | 02 supplies exact-pin read inputs, 05 supplies immutable object descriptors, 08 checks scope and request counts. [Cloud profiles](09-cloud-profiles.md) | Start with one Lakekeeper read profile. Qualify each real store and catalog/auth combination separately. |
| 03 capture and membership | 04 consumes bound segments and returns exact batch receipts. 06 maps a source handle to a bounded wait. [Receipt contract](03a-client-receipts.md) | Run ordinary SQL against 04 after recovery providers land. Test mixed-visibility duplicate rows, rollback and a fresh pooled connection. |
| 06 stock packaging | 01 and 02 provide scoped reads and pins. Downstream operators consume a signed digest, version ranges and capabilities. [Release manifest](06a-release-contract.md) | Clean stock install and upgrade with independent pinned results. Can ship before managed publication. |
| 08 conformance and fault fixtures | Every implementing owner runs the same versioned inputs. [Gate definitions](08a-core-release-gates.md) | Provider tests precede the live SQL/independent-engine smoke. Downstream deployment teams supply separate route, concurrency and failover receipts. |

Public contributors can complete every core slice using this repository. Hosted deployment, pricing, placement and cloud fleet orchestration use the downstream artifact contract. Teams can build against fixtures concurrently. Land one coordinated change when two modules share an extension upgrade, ABI export, fixture version or interpretation of a receipt.

Cut each delivery branch from the latest merged `origin/lakehouse-dev`. Modules are ownership boundaries. A branch can deliver one tested change across both sides of a boundary. The sequence below gives one team a serial path and identifies where another contributor can start against the same fixtures. [Branch rules](../RULES.md#1a-branch--promotion-discipline-amended-2026-11-20--the-binding-graph)

| Land in this order | Start with | Evidence that unlocks the next consumer |
|---|---|---|
| 1. Qualification harness and immediate guard | 08 provides an independent row oracle, exact pins, catalog request counters and deterministic barriers. 05 can independently land the COW guard on the existing write path. | Reproduce the lost-reply defect at remote acceptance and the pre-deleted-row rewrite on the parent. Unsupported rewrite inputs fail before a catalog commit, including after a replan. |
| 2. Catalog, local ledger and pinned reader | 01 implements pin resolution and single-attempt submission/lookup. 03 implements durable segment identity, claim membership and fenced local transitions. 02 consumes a fixed pin and resolves delete-filtered rows and physical ordinals. Each can start against the harness. | Provider tests cover a moved ref, a lost reply, two claimants, mixed PG visibility in one claim, and a file with a pre-existing delete. The ledger upgrade and catalog evidence carry the identities in contract v1. |
| 3. Publish and read staged appends | 04 integrates the 03 ledger, the 05 prepared append proposal and the 01 single-attempt adapter. 03 integrates the 02 reader and publication membership. The 05 append proposal can be developed during step 2, independently of its COW rewrite. | Kill the publisher after catalog acceptance and restart it. Assert one external append and exact managed rows before and after local completion. Exercise the SQL wait, cancellation, authorization and upgrade paths. Then qualify managed COUNT against ordinary scans. |
| 4. File corrections and performance | 05 enables direct COW only for verified 02 ordinals/delete applicability and 01 expected-base checks. 08 records a baseline after pinned result digests agree. | Concurrent delete or rewrite is either correctly applied or rejected. Historical and current rows match an independent reader. Compare before/after throughput, I/O and memory on the same verified dataset. |

The stock FDW build probe (06) can start in step 1. Its first read pilot requires 01 scoped access, 02 pinned reads and 06 role/upgrade tests. It can ship independently of staged publication. Heap CDC (07) can prototype capture and replay against a fake sink. A live CDC pilot depends on verified downstream receipts and checkpoint recovery. Additional catalog profiles each replay the same provider and end-to-end gates.

For every milestone run the prior smoke plus the new failure case. Record the exact revision, rows and snapshot IDs, raw samples, sample count, settings and a PASS/FAIL/NOT RUN per gate. Include commit-to-publication and fresh-consumer lag for writes, and first-row latency, fetched bytes and peak memory for reads. [Measurement and smoke protocol](../10-testing.md#milestone-tdd-and-smoke-receipts)

### Shared-file sequencing

| File or surface | Integration order |
|---|---|
| `session.rs` | Catalog/storage owns resolution and CAS entry points. Reader and writer develop private task/encoding logic in their own files and wire into session entry points after the provider tests land. |
| `ffi.rs`, generated header, `cbindgen.toml` | Reader and writer develop private Rust signatures independently. Integrate public handle/status definitions, export list and C callers in one coherent slice per ABI change. |
| `tableam.c`, `lakehouse_tableam.h` | Visibility owns snapshot/capture and COUNT. Mutation work first adds the fail-closed guard or a scoped direct-write helper with a tested seam. Schedule any shared callback rewiring in a single integration change. |
| Extension SQL, Meson, workflow files | Ledger and SQL owners supply one upgrade/install sequence. Qualification owns the shared CI entry point. Module PRs can test privately without bumping a version in parallel. |

For each slice record the base SHA, module, interface version, paths touched, dependent providers, fixture IDs, red/green commands and any requested seam change. Review the provider and consumer together for a seam change. Keep optimizations and renames out of correctness slices unless they enable the required boundary.

Add a worked slice when two implementations could follow the brief and still return different rows, send a second commit, or make different promises to a caller. Stop once both owners can write the same failing scenario and agree on the observable result. [Staged append recovery](04a-publisher-recovery.md) illustrates this with a crash between a catalog commit and a PostgreSQL ledger update.

### Naming

Use **Anvai Lake** in product material and module headings, **AnvaiOps** for the company, and **pgdatalake** for the repo/project. Keep `lakehouse` for SQL extension, schema, GUCs, library and table AM, and `pg_datalake_*` for the C/Rust ABI. Name internal files for the responsibility they own. `lakehouse_sql.c` owns SQL entry points, `managed_count.c` owns managed counts, and `publisher.c` owns staged publication. Stable identifiers keep existing installations and downstream builds on the same upgrade path.
