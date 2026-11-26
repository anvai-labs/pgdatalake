"""ANALYZE for lakehouse foreign tables: real NDV into pg_statistic.

The relsize slice grounded row counts in Iceberg metadata but left join
and qual selectivity on core's DEFAULT_NUM_DISTINCT(200) fallback. With
AnalyzeForeignTable/AcquireSampleRows, ANALYZE (explicit or auto-analyze)
populates real per-column statistics: 5000 rows over 7 distinct grp values
must plan grp = '1' at ~1/7 selectivity, not 5000/200.
"""

import importlib
import os
import re
from uuid import uuid4

from publication_fixture import sql

trino = importlib.import_module("bound-publication-restart").trino

CATALOG = os.getenv("LH_CATALOG", "http://localhost:8181/catalog")
S3 = os.getenv("LH_S3", "http://localhost:9000")
S3_AK = os.getenv("LH_S3_ACCESS_KEY", "lakehouse-test")
S3_SK = os.getenv("LH_S3_SECRET_KEY", "lakehouse-test-password")


def plan_rows(name, query):
    plan = sql(name, "EXPLAIN " + query)
    m = re.findall(r"rows=(\d+)", plan)
    assert m, plan
    return [int(x) for x in m]


def main():
    if not __debug__:
        raise RuntimeError("the analyze fixture requires Python assertions")
    ns = "ana_" + uuid4().hex[:12]
    name = "ana_" + uuid4().hex[:8]
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

            # pre-ANALYZE: metadata tuples with default NDV -> 5000/200 = 25
            r = plan_rows(name, "SELECT id FROM events WHERE grp = '1'")
            assert r == [25], r

            # explicit ANALYZE: sample -> real NDV (7 distinct over 5000)
            sql(name, "ANALYZE events")
            r = plan_rows(name, "SELECT id FROM events WHERE grp = '1'")
            assert len(r) == 1 and 500 <= r[0] <= 950, r

            # row estimate stays the metadata sum (no quals)
            r = plan_rows(name, "SELECT id FROM events")
            assert len(r) == 1 and 4000 <= r[0] <= 6000, r

            # repeat ANALYZE is idempotent and keeps estimates real
            sql(name, "ANALYZE events")
            r = plan_rows(name, "SELECT id FROM events WHERE grp = '1'")
            assert len(r) == 1 and 500 <= r[0] <= 950, r

            # fail-fast contract: ANALYZE over a dead endpoint errors
            # (explicit maintenance, not hot-path — no silent decline)
            sql(name, "ANALYZE dead_events",
                error="08000: lakehouse")

            print(f'{{"case": "analyze", "ndv_selectivity_rows": {r[0]}}}',
                  flush=True)
        finally:
            sql("postgres", f"DROP DATABASE {name} WITH (FORCE)")
    finally:
        trino(f"DROP TABLE IF EXISTS {table}; DROP SCHEMA iceberg.{ns}")


if __name__ == "__main__":
    main()
