# Read-only BI pools and marketplace channels

Deploy stock PostgreSQL with the public FDW wherever the tenant or Anvai Lake
can install the required extension. Expose a standard, read-only PostgreSQL
endpoint near the enrolled Iceberg object store. Each reader serves the
approved published pins. This pool has no dependency on physical WAL replica
replay from a managed PostgreSQL writer. PostgreSQL catalogs, privileges and
optional logged BI copies still require durable lifecycle and restore contracts.
Keep published BI and PostgreSQL transaction visibility as separate service
claims.

Evaluate the catalog integration, compute deployment and commercial listing as
three independent gates. A working catalog adapter earns no listing approval.
Use the [same published scan boundary](02a-bi-reuse.md#shared-reader-and-cache-seam)
for local stock readers and the hosted reader pool. The operator owns placement,
same-region connectivity, live account qualification, provisioning, billing and
marketplace support. Public code owns the catalog adapter and interoperability
tests. [Cloud profile methodology](09-cloud-profiles.md)

| Channel | First route to test | Decision boundary |
|---|---|---|
| Databricks | Qualify managed Unity Catalog Iceberg REST against explicitly approved native Iceberg, a pinned snapshot and a least-privilege external identity. Run the stock FDW in customer or Anvai Lake cloud compute. Evaluate Partner Connect for onboarding and Marketplace for an eligible partner or data-product listing. | Databricks documents Marketplace shares and assets, separately from the curated Partner Connect trial path. Confirm partner eligibility directly. Databricks Apps exposes a web application interface. Treat hosting its UI as a separate option from serving ordinary PostgreSQL wire connections. [Unity REST](https://docs.databricks.com/aws/en/external-access/iceberg) · [Marketplace listing](https://docs.databricks.com/aws/en/marketplace/create-listing) · [Partner Connect](https://docs.databricks.com/aws/en/partner-connect/) · [Databricks Apps](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/best-practices) |
| Snowflake | Qualify a Horizon Iceberg REST external-engine read with the customer's actual role and scoped storage credentials. Evaluate a paid Connected App listing for an externally hosted service. Ask the partner team whether the proposed external-engine consumption route qualifies. | The Marketplace supports paid Connected Apps and Native Apps with separate requirements. Snowpark Container Services can run containers and supports private service-to-service TCP. Its documented public ingress uses HTTP(S), so an ordinary BI PostgreSQL driver requires an independently qualified connection path. Defer a Native App until customers specifically require in-account operation and the connectivity, review and approval gates are established. [Horizon REST](https://docs.snowflake.com/en/user-guide/tables-iceberg-access-using-external-query-engine-snowflake-horizon) · [Listing types](https://docs.snowflake.com/en/collaboration/provider-listings-creating-publishing) · [App requirements](https://docs.snowflake.com/en/collaboration/guidelines-reqs-for-listing-apps) · [Service endpoints](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/specification-reference) |

Databricks Lakebase already offers scale-to-zero and autoscaling on read
replicas of Lakebase's PostgreSQL data. Lakebase publishes a list of supported
extensions. Anvai Lake's FDW does not appear on that list, so installing its
native module there requires explicit platform qualification. Benchmark the
customer job against Lakebase when the data is PostgreSQL-resident and against
Databricks SQL when the customer already queries the lake there. The proposed
advantage is a PostgreSQL-wire endpoint over existing published Iceberg with
compatible authorization and measurable dashboard reuse. [Lakebase replicas](https://docs.databricks.com/aws/en/oltp/projects/read-replicas) · [Lakebase extensions](https://docs.databricks.com/aws/en/oltp/projects/extensions)

Use native Databricks SQL or Snowflake warehouses as customer baselines for
governed dashboard work. Qualify the actual authorized policy before routing
external scans or generating BI copies. Refuse a profile that requires
row filtering, masking, governed scan plans, or credential delegation that
this path cannot enforce. Treat native Iceberg, virtual representations of
Delta, Open Catalog/Polaris and managed Horizon as separate read profiles.
Require independently pinned result and delete equivalence, credential expiry,
zero forbidden-root requests, approved encryption/key access, and live
authority verification on each. [Unity credential vending](https://docs.databricks.com/aws/en/external-access/credential-vending) · [Horizon access](https://docs.snowflake.com/en/user-guide/tables-iceberg-access-using-external-query-engine-snowflake-horizon)

## Elasticity and commercial evidence

1. Start with a single clean stock-reader deployment, then enroll one active
   tenant on the reference catalog to prove ordinary Tableau, Quick Sight or
   Qlik connectivity. Confirm refresh, connection pooling and TLS in the
   customer's actual network. Establish native-engine and imported-BI baselines.
2. Replay the same 1 → 2 → 4 reader burst on the provider's profile. Record
   user-verifiable result digests at the same generation, admission queue and
   dashboard p95/p99, first successful new-node query and cost per successful
   dashboard. Record catalog calls, storage requests/bytes, billed compute
   time, and egress. Test an authorized warm canary before admitting a node.
   Drain new queries and preserve or explicitly cancel active sessions.
3. Compare a warm minimum, scale to zero and prewarm on known refresh schedules.
   Include repeated cache-fill, upstream catalog saturation and peak database
   memory. Offer a workload-specific plan only after its warm/idle and expiry
   costs are measured. Permission change invalidates warm routing eligibility.
4. Use actual provider and marketplace RFIs for acceptance, regions and
   procurement. Keep deployment eligibility, catalog support and completed
   commercial listing independently recorded. Marketplace publication needs
   partner acceptance and live onboarding evidence.

For the [governed exact-pin contract](06c-governed-published-read.md), qualify
Horizon External OAuth with the user's role and the IRC Scan Plan API. Snowflake
serves a temporary filtered dataset. Verify source-pin linkage and account for
server-side planning and warehouse use in the pilot. Unity Catalog's documented
cross-engine ABAC profile requires a managed table with catalog commits,
external access and machine-to-machine OAuth or a personal access token.
Databricks documents serverless enforcement and a concurrent-write caveat for
multi-scan queries. Qualify its credential mode and same-source-pin proof per
profile. A user signing into a client through SSO supplies only the first
identity boundary. [Horizon credentials](https://docs.snowflake.com/en/user-guide/tables-iceberg-access-using-external-query-engine-snowflake-horizon)
· [Horizon protected scans](https://docs.snowflake.com/en/user-guide/tables-iceberg-enforce-access-policies-scan-plan-api)
· [Unity Catalog cross-engine ABAC](https://docs.databricks.com/aws/en/external-access/cross-engine-abac)
