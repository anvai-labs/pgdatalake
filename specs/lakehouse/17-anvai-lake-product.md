# Anvai Lake product, adoption, and publication proposal

**Decision draft for the destination branch.** The footprint figures were checked on `work/count-and-pub-qualification` at `5b31b230680`, which contains PRs #30 and #36. This proposal supplies the product and publication contracts for the next qualification run.

## 1. Brand and positioning

Use **Anvai Lake** as the product name under **AnvaiOps** (`anvaiops.com`). Use **Iceberg access and publishing for PostgreSQL** as the descriptor. “Lakehouse” describes an architecture buyers already recognize, and the existing SQL name serves a different purpose. Keep `pgdatalake` as the repository and engineering project, and keep `CREATE EXTENSION lakehouse`, `USING lakehouse`, the FDW handler and `lakehouse.*` configuration stable. Customers should see two modes of one product, **external access** and **managed publishing**. Describe PostgreSQL-managed staging and vendor-managed Iceberg tables explicitly, since “managed” can refer to either side.

| Candidate | Use |
|---|---|
| **Anvai Lake** | Preferred public brand. Short, ties to AnvaiOps, and leaves room for future formats. |
| **Anvai Lakehouse** | Descriptive, with heavier overlap in industry vocabulary. Use “lakehouse” in explanatory copy. |
| **pgdatalake** | Keep for the repo and technical discoverability. It describes the category and carries less product identity. |
| **lakehouse** | Keep as the installed SQL identifier. Prefer namespaced packaging and a clear compatibility matrix over a rename that breaks DDL, upgrades, GUCs or dump/restore. |

The checkout's remote is `anvai-labs/pgdatalake`; use the official organization spelling in public links. Treat the brand as proposed until domain/package availability and trademark clearance are checked. `pg_lake` is an existing product in the same category, including writable Iceberg tables. Differentiate with the visibility boundary, recovery evidence, and supported catalog profiles. [pg_lake Iceberg capabilities](https://github.com/Snowflake-Labs/pg_lake/blob/main/docs/iceberg-tables.md) · [PostgreSQL trademark policy](https://www.postgresql.org/about/policies/trademarks/)

Proposed copy after qualification:

> Use PostgreSQL to query Iceberg on your existing catalog. Publish PostgreSQL application data to the same Iceberg tables for Spark and other supported engines.

The deployment checklist must name the PostgreSQL build or extension package, compatible catalog and authentication method, storage access, and the publication mode. The managed publisher runs where PostgreSQL runs. External engines connect through the catalog and storage.

Use the [DML visibility contract](18-anvai-lake-dml.md)
for claims about updates, deletes and isolation. The
[competitive review](19-anvai-lake-positioning.md) adds heap CDC as a
candidate adoption path for customers who need low-latency PostgreSQL DML
on their existing deployment.

## 2. How much of PostgreSQL is forked?

This is an **extension-first, source-patched PostgreSQL distribution** today. Against the common base `207b5293b8d`, the implementation under `src/backend/access/lakehouse/` is 69 changed files and roughly 27.4k inserted lines, including dependency, build and test-fixture material. Outside it and outside `src/test/`, 20 production source/header/build files under `src/` change by **+609/−42** after excluding patch rejects. The whole diff also includes docs, workflows and related tooling. Four tracked `.rej` files are patch debris and should be removed in the branch cleanup after convergence. Count locked dependency lines and generated/fixture artifacts separately when measuring fork maintenance.

| Surface | Current dependency | Packaging direction |
|---|---|---|
| FDW, validator, mappings, PostgreSQL slots and C/Rust boundary | Uses standard FDW mechanisms. The module currently shares build and glue with the TableAM. | Qualify a separate **read-only FDW package** on an unpatched, supported PostgreSQL major. Offer a foreign-table snapshot option or another extension-level pinning surface. CI must prove install, query, upgrades and cleanup on stock binaries. |
| Managed TableAM, delta ledger, worker and GUCs | TableAM, PostgreSQL heap/WAL, background workers and extension GUCs do most of the work. Custom relation options use the fork's table-AM options support across DDL, relcache and standard option consumers. | Ship the managed preview as a versioned PostgreSQL distribution. Prototype a stock-compatible variant with server mappings and explicit attachment. Adopt the upstream interface when it ships in a supported release. |
| Native time-travel syntax | Parser, RTE serialization, deparser and catalog-version edits add a SQL surface that persists in stored trees. | Keep native syntax as a distribution capability. For the stock package, document portable alternatives and test their dump/restore semantics. |
| Build and release | Fork Meson wiring builds the Rust static library and extension together. | Prove reproducible packaging and supported architectures. Publish separate upgrade/migration instructions for stock and fork binaries. Validate logical export before advertising a route between them. |

Track patch size, changed core files, catalog/ABI impact, merge conflicts, hours per upstream sync, and whether each feature can ship on a stock major. Test an upstream build with the extension feature disabled. Keep a required stock-FDW build/test job once packaging exists. A small line count can still impose a high upgrade cost when it crosses grammar, catalog and TableAM ABI boundaries. Hosted PostgreSQL installation depends on the host's extension allowlist or provider agreement. [TableAM reloptions upstream work](https://commitfest.postgresql.org/patch/5631/) · [RDS extension availability](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL.Concepts.General.FeatureSupport.Extensions.html)

## 3. Visibility contract and customer workflows

| Workflow | Local PostgreSQL view | What another engine reads |
|---|---|---|
| **Spark/EMR writes to an existing Iceberg table.** Attach with FDW, or attach a managed TableAM where that catalog and table profile are supported. | Both should read the same published snapshot when pinned to the same ID. A staged managed relation can additionally serve PostgreSQL-visible staged rows under its documented visibility rules. | The catalog-selected published snapshot, subject to the consumer's refresh policy. |
| **PostgreSQL stages inserts through TableAM.** | Acknowledgment is the PostgreSQL commit under its configured WAL durability policy. A supported managed read sees its own writes and, after commit, the visible local ledger plus the published base. | The new rows appear only after verified Iceberg catalog publication and consumer refresh. This includes the PostgreSQL FDW reading the same Iceberg table. “Delta segments” here are PostgreSQL heap rows, not Delta Lake tables. |
| **A PostgreSQL writer needs remote visibility on the hot path.** | The current COW TableAM path commits to Iceberg during PostgreSQL pre-commit. Treat it as a candidate for catalog-first workflows after qualification. | A fresh consumer can resolve the committed snapshot after catalog success. PostgreSQL and the remote catalog have separate commit boundaries. Retry, timeout and reconciliation semantics are required. |

The current FDW registers scan and planning hooks, with no foreign modification callbacks. It is the **published-state read path** and a useful independent cross-check. For synchronous publication, prefer qualifying the existing COW append path and offering a bounded publish-and-wait operation for staged batches. The latter should wait for a specific durable receipt and return its snapshot ID or a timeout with the receipt available for polling. Treat writable FDW or an explicit direct-append API as a separate scope with its own transaction, permissions, `COPY`, cancellation and abort tests. Keep the initial interoperability promise to append workloads. Qualify shared-writer UPDATE/DELETE, compaction and schema evolution separately.

Adopt one catalog as authority per physical table. Register aliases and consumers against the same table UUID, with storage access from each engine. Compare pinned results and metadata through the catalog. Avoid multiple independent catalogs writing the same storage prefix. For coexistence with external writers, read the published base plus only those locally visible segments absent from that base. Publication must be idempotent by durable operation ID, including after a lost response. External time travel sees historical published snapshots; pending rows are a local transaction feature. [Publication correctness and remaining gates](16-release-reassessment-and-scale.md#publishing-must-reconcile-remote-success-with-local-visibility)

## 4. Publication by size or age

Retain the **32 MiB–4 GiB file target range, default 128 MiB**. A file target controls file rollover and layout. It is approximate when row-group boundaries or compression affect the result. Propose a separate effective per-table **`publish_max_delay`** with **5–300 seconds**, **20 seconds by default**, and a **60-second batching profile**. Resolve per-table settings over administrator defaults within validated bounds. The current `lakehouse.publish_max_lag` defaults to 30 seconds and controls the worker's sleep between cycles. Its current implementation does not enforce an age deadline. Keep its compatibility semantics until a versioned upgrade establishes how old values migrate to a scheduler tick or a new target.

**Schedule when either threshold is crossed.** Make a table eligible when its pending-work estimate reaches a configurable byte target or the oldest committed, unpublished segment reaches its effective max delay. Initially derive the byte target from the configured file target and measure the mapping from staged bytes to compressed output. Keep a separate cap on bytes, rows and segments in one claim. A timeout flush can produce a file smaller than 32 MiB. This makes low-volume latency independent of volume. Use maintenance compaction to control small-file debt.

Wake the scheduler when commits become visible, with a periodic fallback scan for missed wakes and restart. Prefer the oldest eligible segment across tables, and use bounded catalog and storage concurrency. Persist the exact claim and stable operation identity before sending writes. Reuse that identity after retries and reconcile unknown catalog outcomes before another append. On backlog or outage, retain committed local data, report lag and health, and enforce explicitly configured disk/WAL backpressure. Make pause, drain and credential rotation observable. Use a commit visibility marker or conservative timestamp with documented measurement error. A timestamp assigned before commit cannot by itself measure time since fsync. Test latency with synchronous durability and observe the client commit acknowledgment.

| Measure | Start and finish |
|---|---|
| **Local acknowledgment latency** | Client starts write to receipt of PostgreSQL COMMIT. Use the configured durability profile. |
| **Queue age** | Local commit acknowledgment to claim start. Max delay controls scheduling under stated load. |
| **Catalog publication lag** | Local commit acknowledgment to independently verified visibility of the committed snapshot on the intended ref. Includes upload, conflict retries, and reconciliation. |
| **Consumer visibility lag** | Local commit acknowledgment to the first successful fresh read of the expected row in the consumer. Adds refresh, caching and polling interval. |

Expose pending bytes and segment counts, oldest committed segment age, due-to-start delay, files per commit, file-size distribution, queue throughput, retries, ambiguous commits and verified snapshot IDs. Record histograms for all four durations and report errors, timeouts and backlog separately. Keep table IDs in operator diagnostics where permissions permit, with low-cardinality aggregate metrics. For planning, evaluate the 20-second queue target and a **60-second p95 acknowledgment-to-catalog objective** on a declared healthy profile. Defer a 20-second end-to-end promise until load tests justify the budget across queueing, upload, commit verification and consumer refresh. Document observed capacity, batch sizes, network conditions and failure behavior with the figures.

## 5. Addressable market and adoption friction

Lead with **read access to existing Iceberg data from customer-operated PostgreSQL**. The first purchase or pilot can involve one read-only foreign table and persistent read-only access through the customer's existing policy. The next use case is **append-heavy application events and operational ELT**, where developers want PostgreSQL read-your-writes and analytics teams accept visible, measured publication lag. Catalog-first appends help customers requiring visibility before handing off to a downstream job. Cross-engine transaction consistency, arbitrary DML, and installation on hosted PostgreSQL increase adoption friction and need explicit delivery paths.

Size this bottom-up. Deduplicate organizations appearing on multiple platforms. Segment by customer-operated PostgreSQL or a host that permits the required binary, active Iceberg tables and catalog, urgency of the access or publication need, and permission to connect both systems. Use qualified installs, pilot retention and validated willingness to pay to estimate conversion. Model paid accounts and annual contract value per segment. Count restricted managed PostgreSQL in the serviceable segment only after a provider-supported deployment or a separately designed external ingestion path exists. Treat ecosystem totals as discovery inputs. Track time to first query, coarse installation steps, privileges and credential setup, exact snapshot agreement, p95 catalog lag, consumer refresh cost, and operator time during failures.

## 6. Catalog and marketplace sequence

| Target | Integration and distribution path |
|---|---|
| **Lakekeeper** | Reference REST and portable smoke environment. Keep it for deterministic CI and use an independent Spark or Trino reader. |
| **AWS Glue / EMR** | Prioritize a Glue REST + S3 profile with SigV4, IAM/Lake Formation and scoped storage credentials. Test with Spark on EMR or Glue. AWS Marketplace is a plausible first software channel for a customer-deployed container/Helm package, separately from the Glue integration and RDS extension availability. [Glue REST](https://docs.aws.amazon.com/glue/latest/dg/connect-glu-iceberg-rest.html) · [EMR Iceberg](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-iceberg.html) · [AWS Marketplace containers](https://docs.aws.amazon.com/marketplace/latest/userguide/container-product-getting-started.html) |
| **Databricks Unity Catalog** | Qualify OAuth, credential vending and least privileges. Distinguish Unity-managed Iceberg with writable REST access from foreign Iceberg exposed read-only through that endpoint. Onboard a reference integration with a notebook or app demo, then evaluate Databricks partner distribution for the product artifact. [Unity REST access](https://docs.databricks.com/aws/en/external-access/iceberg) · [Databricks Marketplace](https://docs.databricks.com/aws/en/marketplace/) |
| **Snowflake / Open Catalog** | Start with an external catalog through a catalog-linked database, or qualify Open Catalog directly. Test exact rows and refresh lag. A Marketplace listing has eligible product types and approval requirements. A Native or Connected App would be a separate integration and commercial deliverable. [Snowflake Iceberg access](https://docs.snowflake.com/en/collaboration/open-format-sharing-access-iceberg) · [Listing product types](https://docs.snowflake.com/en/collaboration/provider-listings-choose-product-type) |

Use a common REST client plus catalog capability profiles. Validate create/load/commit, format version, credential vending, permission scope, and feature restrictions on each supported operation. Use a separate adapter where a catalog lacks an eligible REST profile and pilot demand warrants it. Capture cost and throughput for real profiles before announcing support. A working connector qualifies interoperability; a marketplace listing also needs packaging, onboarding, entitlement, support and vendor review.

## 7. TDD and milestone gates

| Slice | Red test, then implementation and smoke evidence |
|---|---|
| **Stock read package** | On an unpatched PostgreSQL build, install and attach one Spark-written table. Compare pinned FDW and independent-reader results. Measure setup time, steps, permissions and plan/first-row time. Smoke fresh install and upgrade. |
| **Visibility and recovery** | On the candidate fork, prove own-write visibility and that external readers see the previous snapshot before publish. Inject failure after catalog success and before ledger completion. Restart, reconcile, and check exact IDs and historical snapshots in PostgreSQL and an independent reader. |
| **Age/size scheduler** | Use a controllable clock and a stateful catalog. Test idle small batches, urgent size-based flush, exact deadline, simultaneous due tables, backlog, retry, config reload and worker restart. Smoke at low, steady and burst load with both 20s and 60s profiles. Record lag distributions, requests, file sizes, backlog drain and PostgreSQL CPU/WAL/disk. |
| **Catalog pilots** | Per profile, prove external-engine append → PostgreSQL read and PostgreSQL append → external-engine read. Test auth expiry/rotation, append conflict, and explicit refresh. Run the same pinned workload and report catalog time separately from consumer refresh. |

Store a receipt after each slice and milestone with candidate SHA, PostgreSQL version, catalog and engine versions, fixtures, configuration, observed consistency, and raw metrics. Run the [milestone TDD and smoke protocol](10-testing.md#milestone-tdd-and-smoke-receipts). Start with a qualified read pilot and publication correctness. Brand and marketplace copy should describe capabilities backed by those receipts.
