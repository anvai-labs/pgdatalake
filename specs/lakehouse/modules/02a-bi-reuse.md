# Reuse across BI queries on published Iceberg

The first product is a stock PostgreSQL read-only endpoint serving explicitly
enrolled Iceberg tables. Tune repeated dashboard work before introducing a
general SQL result cache. Reuse one published data generation across a dashboard
when the connector can carry it. Record the age of that generation. A published
generation has a vector of explicitly selected table UUIDs, refs and snapshot
IDs. Independent table pins do not imply an atomic external transaction.

## Customer paths

| Mode | Reuse | Work sent to Anvai Lake |
|---|---|---|
| Tableau extract, AWS Quick Sight SPICE, or QlikView in-memory model | Materialize a BI dataset at the tool and slice imported data there. | Initial load and refresh. Set the tool's freshness and authorization policies and measure reload cost. [Tableau Hyper](https://help.tableau.com/current/server/en-us/perf_tuning_long_queries_for_extracts.htm) · [SPICE](https://docs.aws.amazon.com/quick/latest/userguide/spice.html) · [QlikView](https://help.qlik.com/en-US/qlikview/May2024/Content/QV_HelpSites/what-is.htm) |
| Live dashboard over one pinned dataset | Use the tool's own live query reuse, then combine compatible widget measures into one SQL request when the tool exposes a suitable dataset model. | Measure actual SQL fan-out per interaction. Tableau supports workbook freshness policies. Quick Sight direct-query refresh and Qlik Sense Direct Discovery caching have their own rules. [Tableau query cache](https://help.tableau.com/current/online/en-us/data_freshness_policy.htm) · [Quick Sight refresh](https://docs.aws.amazon.com/quick/latest/userguide/refreshing-data.html) · [Qlik Sense Direct Discovery](https://help.qlik.com/en-US/sense/May2026/Subsystems/Hub/Content/Sense_Hub/DirectDiscovery/differences-between-direct-discovery-in-memory-data.htm) |
| Repeated live joins over a small shared dimension | Publish an explicit logged PostgreSQL copy for an authorized cohort. Index and analyze approved local keys. Join live, pinned facts against a selected dimension generation. | Zero external dimension requests on a warm local generation. Fact reads still follow the plan and receive their own request budget. [PostgreSQL materialized data](https://www.postgresql.org/docs/current/rules-materializedviews.html) |
| Repeated high-cost fact measures | Start with measured, versioned aggregates at the required grain. Combine compatible `COUNT`, `SUM`, `MIN` and `MAX` with explicit filters and grouping sets. Carry sum and non-null count for AVG. | Use one selected tile generation when it covers all requested dimensions and filter semantics. Rebuild affected tiles for changes before advance. Fall back to the exact pinned query outside the proven scope. |

`PREPARE` saves parse work and can reuse a plan. Executor `Memoize` saves
parameterized child executions inside a plan. PostgreSQL shared buffers help
logged local copies. Use them at those boundaries. An FDW object's bytes and
cross-statement dashboard results need an explicit reuse boundary.
[PostgreSQL PREPARE](https://www.postgresql.org/docs/current/sql-prepare.html) · [Memoize](https://www.postgresql.org/docs/current/runtime-config-query.html) · [Shared buffers](https://www.postgresql.org/docs/current/runtime-config-resource.html)

## First stock PostgreSQL pilot

1. Enroll one demonstrably delegated cohort and choose one dimension plus one
   fact. Refresh under a dedicated PostgreSQL role with a restricted foreign
   mapping. Load only projected rows into a private logged heap. Preserve
   duplicates and NULLs. Use a unique index only after a proven key contract.
2. Publish the dimension rows, pin vector, refresh timestamp, dataset generation,
   and exact row digest in one transaction. Expose a BI relation for that
   generation, with explicit local grants and an independent copy-retention
   contract. One session can read a generation with normal SQL. Across widget
   connections carry the same generation through the dataset selection or use
   one query for compatible widgets. A failed refresh leaves the previous
   generation intact. An older dashboard may select it while authorized.
3. Resolve fact pins separately and show the resulting input vector. If a
   customer expects a source access revocation to withdraw previously copied
   data, require a live source authorization check per use or decline the copy
   profile. Test revoking local grants separately. A copied-data grant is a
   separate authorization decision. Refresh cannot apply per-viewer row filters
   unless the source policy has an explicitly qualified delegation contract.
4. Prove request reduction and exact answers using one dashboard with shared
   measures and one with diverse slicers. A common grouping query can return a
   bounded superset, including totals distinguished with `GROUPING()`. Keep
   arbitrary DISTINCT, percentiles, top-k, corrections, and nonadditive measures
   on exact queries until an algebra and retention proof exists. Global cube
   expansion requires evidence that its cardinality and refresh cost pay off.

Consider exact-result caching after profiling real identical repeats and
non-identical slicer patterns. Start from registered deterministic query or tile
versions and typed parameters. Include the delegation/cohort, effective role
and current authorization proof, database installation, enrolled binding
generations, input pin vector, schema/metric versions and applicable session
semantics. A changing ref, schema, mapping, policy, credential scope or pin
invalidates eligibility. Keep separate key space for timezone, collation or
other behavior when definitions depend on it. Report an expired or unavailable
pin instead of substituting latest silently. Keep arbitrary SQL, volatile
queries, managed staged state and unsupported policy classes outside this scope.

## Shared reader and cache seam

| Owner | Agreed boundary | Independent test |
|---|---|---|
| SQL/packaging 06 and catalog 01 | Accept the statement's authorized identity and enrolled binding. Return one validated pin and scoped storage handle or a typed refusal. | Owner/invoker views, same table names across owners, rotated mappings and revoked credentials. Recheck before admitting a new query. |
| Reader 02 | Consume an authorized published pin, schema/projection, filters and delete proof. Return typed batches or an explicit error. Reuse the same pruning, Parquet range I/O and batch ABI in both the FDW and TableAM published scan. | Run identical pinned objects through both package builds with a shared digest, duplicates, deletes and forbidden-root counters. Share fixture behavior before changing the ABI. |
| Visibility 03 | TableAM selects the compatible published pin from its real PostgreSQL snapshot and reconciles exact visible staged members. It can reuse the published reader after selection. | Same published pin plus distinct local transaction visibility must give the correct distinct answers. Release result reuse for the managed overlay requires its own proof. |
| Qualification 08 and operator | Measure the cache or materialized generation through authenticated SQL. Hosted routing may warm an enrolled node with a canary and must preserve its pin, deadline and drain semantics. | Simultaneous load, expired generation, auth changes during fill and scale-in/out. |

The current `BlockCache` is process-local and keyed by the open storage scope.
It already permits reuse within one scope. Preserve that authorization rule.
First investigate carrying one approved object handle through read-only rescans
of the same authorized statement. PostgreSQL serves separate connections in
separate processes. Moka within an extension backend cannot share those caches.
Moka can be evaluated for byte-weighted eviction and coalescing concurrent
fills for the same key in a shared Rust process. It supplies neither authority
nor multi-node coherence. Check each caller before admission and before return,
including waiters after an in-flight fill. Bound cached bytes, inflight fills,
result size and deadline separately. Avoid permanently persisting transient
provider errors. Measure hot-key benefits and low-reuse memory cost before
adding a process or replacing the current cache. [PostgreSQL processes](https://www.postgresql.org/docs/current/tutorial-arch.html) · [Moka coalescing](https://docs.rs/moka/latest/moka/sync/struct.Cache.html#method.try_get_with) · [Scoped byte reuse](06b-scoped-read-handoff.md#remote-byte-cache-authorization)

## Acceptance and cost

Use a dimension pin D50 of `[(1,east),(1,east),(2,NULL)]` and a fact pin F200
of `[(1,10),(1,10),(2,NULL),(3,4)]`. A left join grouped by fact key and region
returns `(1,east,4,4,40)`, `(2,NULL,1,0,NULL)`, and `(3,NULL,1,1,4)` for
`count(*)`, `count(value)` and `sum(value)`. At F201, deleting one `(1,10)`
changes the first group to `(1,east,2,2,20)`. An older compatible generation
must preserve its result. Query all widgets through fresh pooled backends and
prove identical results across a successful refresh, failed refresh, restart,
and authorized scale-in/out. Test role change, revoke, snapshot expiry and
concurrent fills separately. Observe zero dimension-provider calls for the
authorized local copy and zero forbidden-root calls everywhere.

Compare refresh and query cost against direct FDW and against the BI tool's
native imported mode where available. Include allocated memory, refresh and
cache fill, catalog/object calls, retrieval/transfer, failures and idle pools.
Compare first-row and dashboard-complete p95/p99 at the same offered load and
result digests. A warm pool must earn its idle cost on measured traffic. This
test supplies the required product evidence before a shared result cache or
replica autoscaler is enabled. [I/O and cost gates](08b-io-cost.md)

The exact #66 release-build diagnostic used one attempt per SF10 query with
serial execution, no warmup, JIT off and 64 MiB `work_mem`. q2 matched 2,513
Trino rows in 17.492 s and q3 matched 100 rows in 22.799 s. q1 timed out at
40.031 s with `57014`. This is a correctness check and one timing observation
per query. Trace q1 planning, join/rescan requests and spill as a separate
baseline so high cache hit rates do not hide an expensive cold query.

Per-viewer BI over a protected table consumes the [governed published-read
contract](06c-governed-published-read.md). It adds provider policy freshness
and output-scope checks before any persistent reuse or COUNT optimization.
