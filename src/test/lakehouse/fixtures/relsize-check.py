"""Planner-visible foreign-table relsize from Iceberg metadata (family E).

An un-ANALYZEd foreign table must not plan as core's rows=1000 default:
lakehouse.plan_rows_mode=metadata sums parquet record_counts at plan time
(cached per session), so join/CTE cardinalities are real. Controls:
mode=default keeps rows=1000; an unreachable endpoint degrades to the
default (fail-safe decline), and repeat planning is served from the cache.
"""

import importlib
import os
import re
from uuid import uuid4

from publication_fixture import sql

trino = importlib.import_module("bound-publication-restart").trino

# CI defaults (compose lakekeeper + seaweed); a profile can override —
# the user mapping must carry credentials the S3 endpoint accepts.
CATALOG = os.getenv("LH_CATALOG", "http://localhost:8181/catalog")
S3 = os.getenv("LH_S3", "http://localhost:9000")
S3_AK = os.getenv("LH_S3_ACCESS_KEY", "lakehouse-test")
S3_SK = os.getenv("LH_S3_SECRET_KEY", "lakehouse-test-password")


def plan_rows(name, query, mode):
    # SET must ride in the SAME session as the EXPLAIN: sql() opens a
    # fresh psql per call, so a session GUC would silently revert
    plan = sql(name, f"SET lakehouse.plan_rows_mode = '{mode}'; "
                     "EXPLAIN " + query)
    m = re.findall(r"rows=(\d+)", plan)
    assert m, plan
    return [int(x) for x in m]


def main():
    if not __debug__:
        raise RuntimeError("the relsize fixture requires Python assertions")
    ns = "rsz_" + uuid4().hex[:12]
    name = "rsz_" + uuid4().hex[:8]
    table = f"iceberg.{ns}.events"
    trino(f"CREATE SCHEMA iceberg.{ns}")
    trino(f"CREATE TABLE {table} (id bigint, grp varchar) "
          "WITH (format_version = 2)")
    trino(f"INSERT INTO {table} "
          "SELECT x, CAST(x % 7 AS varchar) FROM UNNEST(sequence(1, 5000)) u(x)")
    try:
        sql("postgres", f"CREATE DATABASE {name} TEMPLATE template0 "
                        f"ENCODING 'UTF8' LOCALE 'C'")
        try:
            sql(name, f"""
                CREATE EXTENSION lakehouse;
                CREATE FOREIGN DATA WRAPPER lh HANDLER lakehouse.lakehouse_fdw_handler;
                CREATE SERVER s FOREIGN DATA WRAPPER lh OPTIONS
                    (endpoint '{CATALOG}', warehouse 'smoke', s3_endpoint '{S3}');
                CREATE USER MAPPING FOR CURRENT_USER SERVER s OPTIONS
                    (s3_access_key '{S3_AK}', s3_secret_key '{S3_SK}');
                CREATE FOREIGN TABLE events (id bigint, grp varchar) SERVER s
                    OPTIONS (table '{ns}.events');
                CREATE SERVER dead FOREIGN DATA WRAPPER lh OPTIONS
                    (endpoint 'http://localhost:1/catalog', warehouse 'smoke',
                     s3_endpoint '{S3}');
                CREATE USER MAPPING FOR CURRENT_USER SERVER dead OPTIONS
                    (s3_access_key '{S3_AK}', s3_secret_key '{S3_SK}');
                CREATE FOREIGN TABLE dead_events (id bigint, grp varchar) SERVER dead
                    OPTIONS (table '{ns}.events');
            """)

            query = "SELECT id FROM events"

            # mode = default: core's rows=1000 default retained (the old
            # behavior this slice replaces — pinned as the escape hatch)
            sql(name, "SET lakehouse.plan_rows_mode = 'default'")
            assert plan_rows(name, query, "default") == [1000]

            # mode = metadata: the parquet record_count sum, no quals on top
            rows = plan_rows(name, query, "metadata")
            assert len(rows) == 1 and 4000 <= rows[0] <= 6000, rows

            # session cache: a repeat plan is served from the cache and
            # must report the same estimate
            assert plan_rows(name, query, "metadata") == rows

            # quals ride ON TOP of the metadata tuples: 5000 tuples with
            # no pg_statistic NDV applies core's DEFAULT_NUM_DISTINCT(200)
            # equality selectivity -> rows = 5000/200 = 25 (the residual
            # family-E gap the ANALYZE/NDV slice closes; pinned here so a
            # change in defaults is visible)
            r = plan_rows(name, "SELECT id FROM events WHERE grp = '1'",
                          "metadata")
            assert r == [25], r

            # fail-fast contract (lakehouse.h, pre-existing): an unreachable
            # endpoint fails the statement at BeginForeignScan — EXPLAIN runs
            # ExecutorStart(EXPLAIN_ONLY), so planning itself surfaces the
            # connection error. Pinned so a silent change is visible.
            err = sql(name, "EXPLAIN SELECT id FROM dead_events",
                      error="08000: lakehouse: could not plan scan")

            # fail-safe decline: metadata rows over the clamp are declined
            # and core's rows=1000 default is kept (clamp = 2000 < 5000);
            # the clamp SET rides in the same session as the EXPLAIN
            plan = sql(name, "SET lakehouse.plan_rows_mode = 'metadata'; "
                             "SET lakehouse.plan_rows_max = 2000; "
                             "EXPLAIN " + query)
            r = [int(x) for x in re.findall(r"rows=(\d+)", plan)]
            assert r == [1000], r

            print(f'{{"case": "relsize", "metadata_rows": {rows[0]}}}',
                  flush=True)
        finally:
            sql("postgres", f"DROP DATABASE {name} WITH (FORCE)")
    finally:
        trino(f"DROP TABLE IF EXISTS {table}; DROP SCHEMA iceberg.{ns}")


if __name__ == "__main__":
    main()
