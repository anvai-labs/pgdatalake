#!/usr/bin/env python3
"""lakehouse_perf.py — perf-lakehouse harness driver (spec 10 §5).

Talks to a RUNNING pgdatalake server over plain pgwire (psycopg2 — the
server is stock Postgres wire protocol; no third-party engine involved).
The fixture data itself is generated out-of-band (pyiceberg via the
docker fixture network — see scripts/perf/README), because the managed
write path is not yet at the M4 bulk-load gate; when it is, the
generator flips to pgdatalake INSERTs and this driver does not change.

Protocol (matches the M1 gate + §5.1/§5.2 records): per query — 1
warmup run (fills the BlockCache: cold timing measures fetch+compute,
warm timing is cache-served), then 3 measured runs, median reported.
Runs against a RELEASE build; wall-clock on a debug build is not
comparable to any recorded number.

Suites:
  tpcds  TPC-DS q1–q3 full-scale (SF=10 fixture, 28.8M-row store_sales):
         the §5.1/§5.2 comparison set — q3 is the row-transport canary
         that perf fix #1 (columnar batch reads, #5/#17) targeted.
         Scale variants attach into a dedicated schema via
         --tpcds-schema/--tpcds-ns/--expected-store-sales.
  tpch   TPC-H q1–q10 at SF=1 (duckdb dbgen fixture, 6M-row lineitem):
         joins, subplans and aggregates against an external Iceberg
         fixture; the DDL attaches on first use.
  write  managed write path through pgwire: staged INSERT throughput
         (the §5 bulk-load baseline), publisher drain lag, direct-mode
         hot-path INSERT commit and COW UPDATE throughput.
  scan   single-file projection/aggregate microbench (small, fast).

Usage:
  python3 scripts/perf/lakehouse_perf.py --suite tpcds \
      --dsn postgres://postgres@localhost:52021/postgres
"""
import argparse
import re
import statistics
import sys
import time
from pathlib import Path

import psycopg2

REPO = Path(__file__).resolve().parents[2]
DEFAULT_DDL = REPO / "scripts" / "perf" / "tpcds" / "ddl.sql"
DEFAULT_QUERIES = REPO / "scripts" / "perf" / "tpcds" / "queries"
DEFAULT_TPCH_DDL = REPO / "scripts" / "perf" / "tpch" / "ddl.sql"
DEFAULT_TPCH_QUERIES = REPO / "scripts" / "perf" / "tpch" / "queries"

# q1/q2/q3 = the recorded §5.1 comparison set
TPCDS_QUERIES = ["query1", "query2", "query3"]

# TPC-H SF=1 (duckdb dbgen): the full 22-branch lineitem fact
TPCH_QUERIES = [f"query{i}" for i in range(1, 11)]
TPCH_SF1_LINEITEM_ROWS = 6_001_215

SCAN_DDL = """
DROP TABLE IF EXISTS perf_scan CASCADE;
CREATE FOREIGN DATA WRAPPER IF NOT EXISTS perf_scan_wrapper
    HANDLER lakehouse.lakehouse_fdw_handler;
DO $$ BEGIN CREATE SERVER perf_scan_srv FOREIGN DATA WRAPPER perf_scan_wrapper
    OPTIONS (endpoint %s, warehouse 'smoke', s3_endpoint %s);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER perf_scan_srv
    OPTIONS (s3_access_key %s, s3_secret_key %s);
CREATE FOREIGN TABLE perf_scan (
    id bigint, name text
) SERVER perf_scan_srv OPTIONS (table %s);
"""

SCAN_SETUP_PY = """
TABLE = "perfcn.scan_bench_n%d"
import random
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

random.seed(7)
cat = load_catalog("smoke", type="rest", uri=%r, warehouse="smoke",
                   **{"s3.endpoint": %r,
                      "s3.addressing-style": "path",
                      "s3.access-key-id": "lakehouse-test",
                      "s3.secret-access-key": "lakehouse-test-password"})
try:
    cat.create_namespace("perfcn")
except Exception:
    pass
try:
    cat.drop_table(TABLE)
except NoSuchTableError:
    pass
schema = Schema(NestedField(1, "id", LongType(), required=False),
                NestedField(2, "name", StringType(), required=False))
t = cat.create_table(TABLE, schema=schema,
                     properties={"format-version": "2"})
N = %d
CHUNK = 200_000
for base in range(0, N, CHUNK):
    k = min(CHUNK, N - base)
    t.append(pa.table({
        "id": pa.array([base + i for i in range(k)], pa.int64()),
        "name": pa.array([f"row-{base + i}-" + "y" * 40 for i in range(k)],
                         pa.string()),
    }))
print(f"generated {TABLE} rows={N} files={len(list(t.scan().plan_files()))}")
"""


def connect(dsn: str, autocommit: bool = True):
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    return conn


def timed(cur, sql: str, timeout_s: int) -> float:
    cur.execute(f"SET statement_timeout = {timeout_s * 1000}")
    t0 = time.perf_counter()
    try:
        cur.execute(sql)  # single-statement files; psycopg2 has no nextset on pgwire
    except psycopg2.errors.QueryCanceled:
        raise SystemExit(f"TIMEOUT after {timeout_s}s: {sql[:60]}...")
    return time.perf_counter() - t0


def measure(cur, sql: str, timeout_s: int, warmup: bool = True):
    if warmup:
        timed(cur, sql, timeout_s)
    runs = [timed(cur, sql, timeout_s) for _ in range(3)]
    return runs, statistics.median(runs)


def remote_table_exists(args, ns_table: str) -> bool:
    """Probe the Iceberg catalog WITHOUT reading data (pyiceberg in the
    fixture docker network — the host cannot reach s3:9000's internal
    name). One REST lookup, no scan."""
    import subprocess
    code = f"""
from pyiceberg.catalog import load_catalog
cat = load_catalog("smoke", type="rest", uri={args.catalog_uri!r},
                   warehouse="smoke",
                   **{{"s3.endpoint": {args.s3_endpoint!r},
                       "s3.addressing-style": "path",
                      "s3.access-key-id": "lakehouse-test",
                      "s3.secret-access-key": "lakehouse-test-password"}})
try:
    cat.load_table({ns_table!r})
    print("EXISTS")
except Exception:
    print("MISSING")
"""
    out = subprocess.run(
        ["docker", "run", "--rm", "-i", "--network", args.docker_net,
         "-v", f"{args.pdi_venv}:/venv",
         "-e", f"PYTHONPATH=/venv/lib/{args.pdi_py_minor}/site-packages",
         "python:3.12-slim", "python3", "-"],
        input=code, capture_output=True, check=True, text=True)
    return "EXISTS" in out.stdout


def generate_scan_fixture(args):
    # bulk generation rides pyiceberg in the fixture docker network
    # (host cannot reach s3:9000's docker-internal name); python-
    # formatted into a standalone script — see SCAN_SETUP_PY above.
    import subprocess
    code = SCAN_SETUP_PY % (args.scan_rows, args.catalog_uri,
                            args.s3_endpoint, args.scan_rows)
    subprocess.run(
        ["docker", "run", "--rm", "-i", "--network", args.docker_net,
         "-v", f"{args.pdi_venv}:/venv",
         "-e", f"PYTHONPATH=/venv/lib/{args.pdi_py_minor}/site-packages",
         "python:3.12-slim", "python3", "-"],
        input=code, check=True, text=True)


def ensure_scan_fixture(conn, args):
    """Reuse a verified fixture if one exists; generate only when missing
    or when verification FAILS (accuracy is checked, not assumed — a
    stale/mismatched fixture would silently poison every measurement)."""
    cur = conn.cursor()
    remote_name = f"perfcn.scan_bench_n{args.scan_rows}"
    exists = remote_table_exists(args, remote_name) \
        if not args.force_regen else False
    if exists:
        # attach first (the fixture may predate this foreign table), then
        # VERIFY: ids are exactly 0..N-1, so count and sum have exact
        # expected values — one cheap scan proves content accuracy
        cur.execute(SCAN_DDL, [args.catalog_uri, args.s3_endpoint,
                               "lakehouse-test", "lakehouse-test",
                               remote_name])
        cur.execute("SELECT count(*), sum(id) FROM perf_scan")
        n, total = cur.fetchone()
        want_n, want_total = args.scan_rows, \
            args.scan_rows * (args.scan_rows - 1) // 2
        if n == want_n and total == want_total:
            print(f"NOTICE: scan fixture exists and VERIFIED "
                  f"(rows={n}, sum(id)={total}) — reusing, no generation")
            return n
        print(f"WARNING: scan fixture MISMATCH (rows={n}/{want_n}, "
              f"sum(id)={total}/{want_total}) — regenerating")
    elif args.force_regen:
        print("NOTICE: --force-regen — regenerating scan fixture")
    else:
        print("NOTICE: no scan fixture in the catalog — generating")
    generate_scan_fixture(args)
    cur.execute(SCAN_DDL, [args.catalog_uri, args.s3_endpoint,
                           "lakehouse-test", "lakehouse-test",
                           remote_name])
    cur.execute("SELECT count(*), sum(id) FROM perf_scan")
    n, total = cur.fetchone()
    want_total = args.scan_rows * (args.scan_rows - 1) // 2
    if n != args.scan_rows or total != want_total:
        raise SystemExit(f"generated fixture FAILED verification "
                         f"(rows={n}, sum(id)={total})")
    print(f"scan fixture generated and VERIFIED: rows={n}")
    return n


def run_scan_suite(conn, args):
    ensure_scan_fixture(conn, args)
    cur = conn.cursor()
    queries = {
        "scan_count": "SELECT count(*) FROM perf_scan;",
        "scan_agg": ("SELECT count(*), sum(id) FROM perf_scan "
                     "WHERE id %% 97 = 0;"),
        "scan_group": ("SELECT name, count(*) FROM perf_scan "
                       "GROUP BY 1 ORDER BY 2 DESC LIMIT 5;"),
    }
    rows = []
    for name, sql in queries.items():
        runs, med = measure(cur, sql, args.timeout, warmup=args.warmup)
        print(f"{name}: runs={['%.2f' % r for r in runs]} median={med:.2f}s")
        rows.append((name, med))
    return rows


def run_tpcds_suite(conn, args):
    cur = conn.cursor()
    # foreign tables are attached UNPREFIXED (that is how the shipped
    # query files reference them); args.tpcds_ns names the ICEBERG
    # namespace only. A non-public --tpcds-schema hosts scale variants
    # (convention: tpcds_sf1) beside the unprefixed SF=10 fixture.
    if args.tpcds_schema != "public":
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {args.tpcds_schema}")
        cur.execute(f"SET search_path = {args.tpcds_schema}, public")
    cur.execute("SELECT to_regclass('store_sales') IS NULL")
    if cur.fetchone()[0]:
        ddl_text = Path(args.ddl).read_text()
        if args.tpcds_ns != "tpcds":
            ddl_text = ddl_text.replace("table 'tpcds.",
                                        f"table '{args.tpcds_ns}.")
        cur.execute(ddl_text)
        conn.commit()
        if args.tpcds_schema != "public":
            cur.execute(f"SET search_path = {args.tpcds_schema}, public")
    cur.execute("SELECT count(*) FROM store_sales")
    n = cur.fetchone()[0]
    if n == args.expected_store_sales:
        print(f"NOTICE: TPC-DS fixture exists and VERIFIED "
              f"(store_sales rows={n}) — reusing, no generation")
    else:
        print(f"WARNING: store_sales rows={n} != {args.expected_store_sales} "
              "— the fixture is stale or wrong; regenerate with "
              "scripts/perf/gen_tpcds.py + the iceberg loader before "
              "trusting these numbers")
    rows = []
    for q in TPCDS_QUERIES:
        sql = (Path(args.queries) / f"{q}.sql").read_text()
        runs, med = measure(cur, sql, args.timeout, warmup=args.warmup)
        print(f"{q}: runs={['%.1f' % r for r in runs]} median={med:.1f}s")
        rows.append((q, med))
    return rows


def run_tpch_suite(conn, args):
    cur = conn.cursor()
    # the TPC-H tables live in schema tpch (customer collides with the
    # TPC-DS fixture's customer); the shipped query files stay unprefixed
    cur.execute("SET search_path = tpch, public")
    # attach on first use; the iceberg namespace names the fixture
    cur.execute("SELECT to_regclass('tpch.lineitem') IS NULL")
    if cur.fetchone()[0]:
        cur.execute(Path(args.tpch_ddl).read_text())
        conn.commit()
        cur.execute("SET search_path = tpch, public")
    cur.execute("SELECT count(*) FROM lineitem")
    n = cur.fetchone()[0]
    if n == TPCH_SF1_LINEITEM_ROWS:
        print(f"NOTICE: TPC-H fixture exists and VERIFIED "
              f"(lineitem rows={n}) — reusing, no generation")
    else:
        print(f"WARNING: lineitem rows={n} != {TPCH_SF1_LINEITEM_ROWS} — "
              "the SF=1 fixture is stale or wrong; regenerate with "
              "scripts/perf/gen_tpch.py + scripts/perf/write_tpch_iceberg.py "
              "before trusting these numbers")
    rows = []
    for q in TPCH_QUERIES:
        sql = (Path(args.tpch_queries) / f"{q}.sql").read_text()
        runs, med = measure(cur, sql, args.timeout, warmup=args.warmup)
        print(f"{q}: runs={['%.1f' % r for r in runs]} median={med:.1f}s")
        rows.append((q, med))
    return rows


WRITE_REMOTE_SETUP = """
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType
cat = load_catalog("smoke", type="rest", uri=%r, warehouse="smoke",
                   **{"s3.endpoint": %r,
                      "s3.addressing-style": "path",
                      "s3.access-key-id": "lakehouse-test",
                      "s3.secret-access-key": "lakehouse-test-password"})
try:
    cat.create_namespace("perfwr")
except Exception:
    pass
for TABLE in ("perfwr.write_bench", "perfwr.write_cow"):
    try:
        cat.drop_table(TABLE)
    except NoSuchTableError:
        pass
    schema = Schema(NestedField(1, "id", LongType(), required=False),
                    NestedField(2, "payload", StringType(), required=False))
    cat.create_table(TABLE, schema=schema, properties={"format-version": "2"})
print("CREATED")
"""


def run_write_suite(conn, args):
    """Measure the managed WRITE path through pgwire (§5 bulk-load
    baseline) on TWO tables, because the modes answer different
    questions: the staged-append table measures local INSERT throughput
    and publisher drain lag (its delta-only scan deliberately serves no
    published rows, spec 18 §1); the direct-mode table measures a hot-
    path INSERT commit and the COW UPDATE of published rows."""
    import subprocess
    code = WRITE_REMOTE_SETUP % (args.catalog_uri, args.s3_endpoint)
    subprocess.run(
        ["docker", "run", "--rm", "-i", "--network", args.docker_net,
         "-v", f"{args.pdi_venv}:/venv",
         "-e", f"PYTHONPATH=/venv/lib/{args.pdi_py_minor}/site-packages",
         "python:3.12-slim", "python3", "-"],
        input=code, check=True, text=True, capture_output=True)

    cur = conn.cursor()
    cur.execute("SET statement_timeout = %s", [args.timeout * 1000])
    cur.execute("DROP TABLE IF EXISTS perf_write CASCADE")
    cur.execute("DROP TABLE IF EXISTS perf_cow CASCADE")
    cur.execute("""CREATE TABLE perf_write (id bigint, payload text)
                   USING lakehouse WITH (write_mode='staged')""")
    cur.execute("CREATE TABLE perf_cow (id bigint, payload text) "
                "USING lakehouse")
    cur.execute("SELECT lakehouse.attach_table('perf_write', "
                "'perfwr.write_bench', 'smoke_server')")
    cur.execute("SELECT lakehouse.attach_table('perf_cow', "
                "'perfwr.write_cow', 'smoke_server')")
    conn.commit()

    n = args.write_rows
    ins = (f"INSERT INTO perf_write SELECT g, 'payload-' || md5(g::text) "
           f"FROM generate_series(1, {n}) g;")
    t0 = time.monotonic()
    cur.execute(ins)
    conn.commit()
    ins_s = time.monotonic() - t0

    # verify through the staged (delta-only) scan BEFORE the publisher
    # drains; after publication the local scan serves nothing by design
    cur.execute("SELECT count(*) FROM perf_write")
    got = cur.fetchone()[0]
    if got != n:
        raise SystemExit(f"staged INSERT verification FAILED "
                         f"(count={got}, want={n})")

    # wait for the publisher worker to drain (a synchronous flush command
    # would replace this poll — named band, spec 17 §3 publish-and-wait)
    t0 = time.monotonic()
    while True:
        cur.execute("SELECT count(*) FROM lakehouse.lakehouse_delta_segments "
                    "WHERE relid = 'perf_write'::regclass AND NOT published")
        pending = cur.fetchone()[0]
        if pending == 0:
            break
        if time.monotonic() - t0 > args.publish_timeout:
            raise SystemExit(f"publisher did not drain {pending} staged "
                             f"segment(s) within {args.publish_timeout}s")
        time.sleep(2)
    pub_s = time.monotonic() - t0
    print(f"insert: {n} rows in {ins_s:.2f}s -> {n / ins_s / 1e3:.0f}k rows/s "
          f"(staged)")
    print(f"publish: drained in {pub_s:.2f}s")

    m = min(args.write_rows, 100_000)
    t0 = time.monotonic()
    cur.execute(f"INSERT INTO perf_cow SELECT g, 'payload-' || md5(g::text) "
                f"FROM generate_series(1, {m}) g")
    conn.commit()
    cow_ins_s = time.monotonic() - t0
    cur.execute("SELECT count(*) FROM perf_cow")
    got = cur.fetchone()[0]
    if got != m:
        raise SystemExit(f"direct INSERT verification FAILED "
                         f"(count={got}, want={m})")

    t0 = time.monotonic()
    cur.execute("UPDATE perf_cow SET payload = payload || '-u' "
                "WHERE id % 10 = 0")
    conn.commit()
    upd_s = time.monotonic() - t0
    cur.execute("SELECT count(*) FROM perf_cow")
    got = cur.fetchone()[0]
    if got != m:
        raise SystemExit(f"COW UPDATE verification FAILED "
                         f"(count={got}, want={m})")
    print(f"direct insert: {m} rows in {cow_ins_s:.2f}s -> "
          f"{m / cow_ins_s / 1e3:.0f}k rows/s (hot-path commit)")
    print(f"update: {m // 10} rows in {upd_s:.2f}s -> "
          f"{m / 10 / upd_s / 1e3:.0f}k rows/s (COW)")
    return [("staged_insert_rows_per_s", n / ins_s), ("publish_lag_s", pub_s),
            ("direct_insert_rows_per_s", m / cow_ins_s),
            ("update_rows_per_s", m / 10 / upd_s)]


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--suite", choices=["tpcds", "tpch", "scan", "write"],
                    default="tpcds")
    ap.add_argument("--dsn", default="postgres://postgres@localhost:52021/postgres")
    ap.add_argument("--timeout", type=int, default=1200,
                    help="per-run statement timeout (s)")
    ap.add_argument("--no-warmup", dest="warmup", action="store_false",
                    help="skip the warmup run (cache state is uncontrolled)")
    # scan-suite knobs
    ap.add_argument("--force-regen", action="store_true",
                    help="regenerate the scan fixture even if one exists "
                         "and verifies (default: reuse verified fixtures)")
    ap.add_argument("--scan-rows", type=int, default=1_000_000)
    ap.add_argument("--catalog-uri", default="http://lakekeeper:8181/catalog")
    ap.add_argument("--s3-endpoint", default="http://s3:9000")
    ap.add_argument("--docker-net", default="pgdatalake_default")
    ap.add_argument("--pdi-venv", default="/tmp/pdi-venv")
    ap.add_argument("--pdi-py-minor", default="python3.12")
    ap.add_argument("--tpcds-ns", default="tpcds",
                    help="fixture namespace; convention for NEW fixtures: "
                         "tpcds_sf<N> so scales never collide (existing "
                         "M1 fixture lives in 'tpcds' at SF=10)")
    ap.add_argument("--tpcds-schema", default="public",
                    help="schema hosting the attached foreign tables "
                         "(use a dedicated schema for scale variants)")
    ap.add_argument("--expected-store-sales", type=int, default=28_800_991,
                    help="verify count for the fixture's store_sales table")
    ap.add_argument("--tpch-ddl", default=str(DEFAULT_TPCH_DDL))
    ap.add_argument("--tpch-queries", default=str(DEFAULT_TPCH_QUERIES))
    ap.add_argument("--write-rows", type=int, default=1_000_000,
                    help="rows for the write suite's staged INSERT")
    ap.add_argument("--publish-timeout", type=int, default=600,
                    help="max seconds to wait for the publisher to drain "
                         "staged segments before the COW UPDATE")
    ap.add_argument("--queries", default=str(DEFAULT_QUERIES))
    ap.add_argument("--oracle", help="independent pinned oracle JSON for qualified TPC-DS timings")
    ap.add_argument("--receipt", help="output JSON receipt for the qualified run")
    ap.add_argument("--fixture-schema", default="public", help="schema of pinned attachments")
    ap.add_argument("--source-sha", help="40-digit Git revision of the build under test")
    ap.add_argument("--package-sha256", help="64-digit digest of the package under test")
    ap.add_argument("--configuration", help="non-secret host, catalog and storage configuration label")
    ap.add_argument("--samples", type=int, default=5, help="measured attempts per qualified canary")
    args = ap.parse_args()

    if args.oracle or args.receipt:
        if args.suite != "tpcds" or not all((args.oracle, args.receipt, args.configuration,
                                            args.source_sha, args.package_sha256)):
            ap.error("qualification requires the TPC-DS suite, oracle, receipt, configuration and both build IDs")
        if not re.fullmatch(r"[0-9a-f]{40}", args.source_sha) or not re.fullmatch(
                r"[0-9a-f]{64}", args.package_sha256):
            ap.error("build IDs must be canonical lowercase hex")
        if args.samples < 1 or args.timeout < 1:
            ap.error("samples and timeout must be positive")
        if Path(args.oracle).resolve() == Path(args.receipt).resolve():
            ap.error("receipt must be separate from the oracle")
        from pinned_receipt import run_pinned

        args.workload = "shipped_tpcds_canaries" if Path(args.queries).resolve() == DEFAULT_QUERIES \
            else "custom_exploratory"
        try:
            passed = run_pinned(lambda: connect(args.dsn), args, TPCDS_QUERIES, psycopg2.Error)
        except FileExistsError:
            ap.error("receipt path already exists; choose a new file")
        print(f"{'PASS' if passed else 'FAIL'}: {args.receipt}")
        raise SystemExit(0 if passed else 1)

    conn = connect(args.dsn)
    cur = conn.cursor()
    cur.execute("SELECT lakehouse.version()")
    print(f"engine: {cur.fetchone()[0]}")

    if args.suite == "tpcds":
        rows = run_tpcds_suite(conn, args)
    elif args.suite == "tpch":
        rows = run_tpch_suite(conn, args)
    elif args.suite == "write":
        rows = run_write_suite(conn, args)
    else:
        rows = run_scan_suite(conn, args)

    print("\n| query | median (s) |")
    print("|---|---|")
    for name, med in rows:
        print(f"| {name} | {med:.2f} |")


if __name__ == "__main__":
    main()
