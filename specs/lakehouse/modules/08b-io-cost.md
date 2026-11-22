# Object request and publication cost gates

Module 02 owns projected scan I/O. Module 04 owns publication scheduling and
maintenance cadence. Module 08 measures consumed operations and bytes at the
transport boundary. Module 01 maps each call to the enrolled storage profile's
billing class. The hosted operator supplies deployment-specific prices and plan
budgets. These budgets are proposed until backed by a release-build receipt.

## Published scans

Keep PostgreSQL's exact projected attributes and local quals with the scan.
Within one authorized object version and open scan, coalesce adjacent decoder
reads into bounded windows. Clip prefetch to known projected chunk bounds and
the object end. Reuse bytes already fetched for the adjacent body. Keep the
window behind the existing scope check. A revoked grant or a reopened security
scope cannot read bytes from an earlier scope. Qualify seeks, short reads,
interrupted requests, cancellation and early LIMIT. [S3 range reads](https://docs.aws.amazon.com/AmazonS3/latest/userguide/download-objects.html)

The exact-pin `date_dim` investigation identified 4,719 range GETs for one
query over a 1,352,305-byte file. Selecting the same two columns through a
two-column attachment issued 369 range GETs, with 223,408 range bytes versus
1,352,301. Both returned `(2450992,1998)` at Iceberg snapshot
`4331476042592330844`. At the published S3 Standard example rate of $0.0004
per 1,000 GETs, 100,000 such queries would cost $188.76 versus $14.76 in range
requests alone. This extrapolates the local request trace and excludes the
catalog, HEAD, metadata, compute, retrieval and transfer charges. [AWS S3 pricing](https://aws.amazon.com/s3/pricing/)

Use the same artifact settings, exact Iceberg pin and independently verified
result for baseline, projection alone, coalescing alone, and both. Begin with
a proposed gate of at most 64 data range requests for this projected canary
and at most 1.25 times the projected baseline's transferred data bytes. Keep
the existing zero-forbidden-request gate. Report outcomes separately when a
change meets the call budget and increases the byte budget. Add sparse lookup,
wide scan, early LIMIT, multi-row-group and remote retry controls before
choosing a default for all workloads. Bound per-file retained bytes and total
session memory. A released default requires profile-specific evidence.

For each attempted query, record full, ranged and metadata GETs, HEAD and LIST,
catalog calls, fetched/requested/useful bytes, retry calls, repeated or
overlapping intervals, peak buffer memory and final outcome. Group by security
scope and cache state without storing credentials. Count unsuccessful requests
and bytes for the cost estimate as well. Preserve the raw trace and samples.
Tail statistics require a comparable release-run distribution, including
timeouts. Cost improvement requires equivalent rows, fewer billable operations
and an acceptable byte and memory trade-off. [Qualification](08-qualification.md)

## Publication by size or age

Wake workers from durable local pending state. A table with no committed
pending work makes zero catalog or object calls. Rank due bindings locally and
batch eligible segments within one authorized binding and claim budget. Trigger
at the size or active age threshold and keep an explicit budget on catalog
commits, object writes, retry lookups and bytes. Leave unsettled sends lookup
only and use bounded retry/backoff during provider outages. Full queues retain
acknowledged input and expose backlog age and `deadline_elapsed` through the
receipt contract. The freshness deadline remains visible when admission is
delayed. [Publication contract](04-staged-publisher.md)

In a 30-day model with enough continuous activity for one nonempty age-driven
publish per interval, a minute means 43,200 publishes per binding and 20 seconds
means 129,600. With one data file and two writer metadata PUTs per append, that
is 129,600 or 388,800 PUTs per binding before catalog-generated metadata,
retries, compaction and housekeeping. Validate those counts independently for
each profile. Keep the proposed one-minute age as the initial experiment and
qualify 20 seconds only for a measured freshness/usage requirement. Measure
application COMMIT to first successful external pinned read separately from
eligibility and compute wake. A quiet table that receives no new writes costs
zero publication requests after its backlog clears.

Use the same validated published data for sparse/active/burst/outage and
recovery runs. Record number of publishes, files and file-size histogram,
object counts per commit, metadata growth, retained bytes, and compaction
GETs/PUTs/rewritten bytes. Verify exact retained-snapshot rows before and after
compaction. Renegotiate the freshness budget if small files and catalog work
dominate. Base file sizing on observed encoded output.

## Billing profile

Estimate costs by multiplying the measured count in each billed operation
class by its region/class rate. Add byte retrieval, network, storage,
maintenance and compute as separate terms. Billable operations can differ
from HTTP requests. For example, Google Cloud Storage separates Class A/B and
flat/hierarchical namespaces. Azure publishes transaction units for operations
that move data, so measure the actual Azure endpoint and operation mapping.
Keep calculator inputs, currency and price date in each qualification receipt.
Promote rate assumptions only after checking provider invoices or equivalent
SKU counters on the advertised deployment profile. A local S3-compatible
fixture qualifies request behavior and supplies model inputs. [Google Cloud storage pricing](https://cloud.google.com/storage/pricing) · [Azure Data Lake pricing](https://azure.microsoft.com/en-in/pricing/details/storage/data-lake/)

For a proposed larger read window, compare saved request charges against added
retrieval and transfer charges at the same pin. The request savings are
`avoided_billed_requests × rate_per_request`. The added byte charges are
`extra_billed_bytes × (retrieval_rate_per_byte + transfer_rate_per_byte)`.
Keep the conservative window if a larger one misses the byte, memory or
first-row latency gate, even when request charges would fall.
