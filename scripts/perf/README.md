# perf-lakehouse harness (spec 10 §5)

Drivers for the performance gates and the §5.1/§5.2 recorded comparisons.

## What runs where

| Piece | Engine | Role |
|---|---|---|
| `lakehouse_perf.py` | psycopg2 (plain pgwire) | exploratory timings or a pinned TPC-DS qualification receipt |
| fixture generation | pyiceberg inside the fixture docker network | bulk data writing; rides the container network because the `smoke` warehouse's storage profile carries the docker-internal `s3:9000` |

**Why pyiceberg still generates the data:** the managed/FDW write path is
row-transport-bound (write staging measured 2.3M rows/s, but bulk-load
throughput is the M4 gate — §5's ≥50 MB/s/backend). When that gate
lands, the generator flips to pgdatalake INSERTs; the driver above
never changes (it only ever speaks SQL to the server under test).

## TPC-DS suite (the §5.1/§5.2 comparison set)

Same fixture as the M1 gate: TPC-DS SF=10 (28.8M-row store_sales, 73
bin-packed files) as Iceberg tables on the fixture stack. q1/q2/q3 are
the recorded comparison set; q3 is the row-transport canary that perf
fix #1 (columnar batch reads) targeted.

```bash
# 1. a RELEASE build (debug-optimized timings are not comparable):
CC=/usr/bin/gcc meson setup build-release -Ddatalake=enabled -Dbuildtype=release
ninja -C build-release && meson install -C build-release --destdir build-release/tmp_install

# 2. a fresh server on a free port (initdb once, then start):
build-release/tmp_install/usr/local/pgsql/bin/initdb -D /tmp/vg-perf -U postgres
# postgresql.conf: shared_preload_libraries='lakehouse', port=52021
build-release/tmp_install/usr/local/pgsql/bin/pg_ctl -D /tmp/vg-perf start

# 3. exploratory timing (warmup + 3 runs, median):
python3 scripts/perf/lakehouse_perf.py --suite tpcds \
    --dsn postgres://postgres@localhost:52021/postgres
```

`tpcds/ddl.sql` attaches the 22 fixture tables with CURRENT option
placement — endpoint + s3_endpoint on the SERVER, credentials ONLY on
the user mapping (RULES §6). `tpcds/queries/` holds q1–q10 verbatim.

### Pinned qualification for q1–q3

1. On a reference engine, select and retain the exact Iceberg snapshot for
   `catalog_sales`, `customer`, `date_dim`, `item`, `store`, `store_returns`,
   `store_sales` and `web_sales`. Run the equivalent q1–q3 SQL with explicit
   version clauses. Record the engine version, dataset/scale and chosen
   snapshots. Use an isolated benchmark schema and keep the catalog authority,
   foreign servers, mappings and fixture DDL fixed during the run.
2. Attach those tables in that schema with the exact `table` and `snapshot_id`
   options from the reference run. Prepare an oracle JSON object with
   `version: 1`, `reference_engine` and `dataset` strings, a `tables` map from
   PostgreSQL table name to `{table, snapshot_id}`, and a `queries` map from
   `query1`, `query2` and `query3` to `{sql_sha256, row_count, sha256}`. Snapshot
   IDs are canonical decimal strings. `sql_sha256` hashes the shipped query
   file bytes. Compute `row_count` and `sha256` from the reference result using
   `pinned_receipt.result_digest`. Pass row tuples with database integers,
   strings, `Decimal` for SQL numeric, and `None` for SQL null. The digest
   sorts compact UTF-8 JSON row arrays, retaining duplicate lines, and hashes
   all lines with newline terminators. Numeric values use a `{"numeric":"…"}`
   tag and normalize scale and signed zero.
3. Run the driver with the pinned attachments and independently captured
   oracle. Supply the source revision and package digest from the artifact
   under test. Use a non-secret configuration label with build type, fixture,
   catalog/store versions and host settings.

```bash
python3 scripts/perf/lakehouse_perf.py --suite tpcds \
    --fixture-schema pinned_bench --oracle oracle.json --receipt receipt.json \
    --source-sha "$PGDATALAKE_BUILD_SHA" --package-sha256 "$PGDATALAKE_PACKAGE_SHA256" \
    --configuration "$PGDATALAKE_BENCHMARK_CONFIG" --samples 5
```

The driver checks each attachment and the byte hash of each query before
collecting samples. Each timed attempt includes SQL execution and client
row materialization. Digest calculation is outside the timer. A mismatch,
timeout or other failed attempt stays in the receipt and prevents a passing
timing summary for that canary. The runner reports median after five matched
samples and nearest-rank p95/p99 after 100 matched samples. With `--no-warmup`,
the cache state is uncontrolled. Measure a genuine cold run with a separate
reset of the relevant caches. Provider request counts, CPU/RSS and proof of
the cited artifact identity come from the surrounding test deployment.

## Scan microbench (fast, no full TPC-DS fixture needed)

```bash
python3 scripts/perf/lakehouse_perf.py --suite scan \
    --dsn postgres://postgres@localhost:52021/postgres
```

An existing fixture that VERIFIES (exact row count + `sum(id)`
checksum — the ids are deterministic 0..N-1) is reused with a NOTICE;
a mismatching one is regenerated with a WARNING; `--force-regen`
overrides reuse. The TPC-DS suite verifies the 28,800,991-row
store_sales sanity count the same way.

## Gate rule (§5)

Regressions > 10% on two consecutive runs block merge. Wall-clock is
runner-noisy; the deterministic P0b metrics (bytes fetched, GET counts,
cold/warm ratio via the `fetched_bytes` cache counter) are the primary
regression signal for read-path changes.
