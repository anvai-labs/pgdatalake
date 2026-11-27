"""Local FDW estimates and the opt-in operator metadata profile.

Plain EXPLAIN with the default setting keeps the catalog estimate and
succeeds with the origin offline. An opted-in operator can measure
metadata estimates and provider failures fall back to local estimates.
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

            assert sql(name, "LOAD 'lakehouse'; SHOW lakehouse.plan_rows_mode") == "default"
            assert plan_rows(name, query, "default") == [1000]

            # Opt-in uses the live metadata and applies the profile clamp.
            rows = plan_rows(name, query, "metadata")
            assert rows == [5000], rows

            role = name + "_reader"
            sql(name, f"CREATE ROLE {role}; GRANT SELECT ON events TO {role}")
            try:
                sql(name, f"LOAD 'lakehouse'; SET ROLE {role}; SET lakehouse.plan_rows_mode = 'metadata'",
                    error="42501")
                plan = sql(name, "LOAD 'lakehouse'; SET lakehouse.plan_rows_mode = 'metadata'; "
                                 f"SET ROLE {role}; EXPLAIN {query}")
                assert re.findall(r"rows=(\d+)", plan) == ["1000"], plan
            finally:
                sql(name, f"REVOKE SELECT ON events FROM {role}; DROP ROLE {role}")

            # quals ride ON TOP of the metadata tuples: 5000 tuples with
            # no pg_statistic NDV applies core's DEFAULT_NUM_DISTINCT(200)
            # equality selectivity -> rows = 5000/200 = 25 (the residual
            # family-E gap the ANALYZE/NDV slice closes; pinned here so a
            # change in defaults is visible)
            r = plan_rows(name, "SELECT id FROM events WHERE grp = '1'",
                          "metadata")
            assert r == [25], r

            dead_query = "SELECT id FROM dead_events"
            assert plan_rows(name, dead_query, "default") == [1000]
            assert plan_rows(name, dead_query, "metadata") == [1000]
            sql(name, dead_query, error="08000: lakehouse: could not plan scan")

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
