"""Physical stats collector (spec 19 §4b, layer L0).

One row per completed foreign-scan executor in the INTERNAL
lakehouse.table_stats, keyed by (dbid, relid, endpoint, ns_table,
snapshot_id), with row_count/files from the already-parsed manifests —
no additional origin requests. Written by the collector as the table
owner; failures swallowed.
"""

import importlib
import json
from uuid import uuid4

from publication_fixture import sql

trino = importlib.import_module("bound-publication-restart").trino

CATALOG = "http://localhost:8181/catalog"
S3 = "http://localhost:9000"


def main():
    if not __debug__:
        raise RuntimeError("the scan-stats fixture requires Python assertions")
    ns = "scanst_" + uuid4().hex[:12]
    name = "scanst_" + uuid4().hex[:8]
    table = f"iceberg.{ns}.events"
    trino(f"CREATE SCHEMA iceberg.{ns}")
    trino(f"CREATE TABLE {table} (id bigint, amt double) WITH (format_version = 2)")
    trino(f"INSERT INTO {table} VALUES (1, 10.0), (2, 20.0), (3, 30.0), (4, 40.0)")
    try:
        sql("postgres", f"CREATE DATABASE {name} TEMPLATE template0 "
                        f"ENCODING 'UTF8' LOCALE 'C'")
        try:
            sql(name, f"""
                CREATE EXTENSION lakehouse;
                ALTER EXTENSION lakehouse UPDATE TO '1.24';
                CREATE FOREIGN DATA WRAPPER lh HANDLER lakehouse.lakehouse_fdw_handler;
                CREATE SERVER s FOREIGN DATA WRAPPER lh OPTIONS
                    (endpoint '{CATALOG}', warehouse 'smoke', s3_endpoint '{S3}');
                CREATE USER MAPPING FOR CURRENT_USER SERVER s OPTIONS
                    (s3_access_key 'lakehouse-test', s3_secret_key 'lakehouse-test-password');
                CREATE FOREIGN TABLE events (id bigint, amt double precision) SERVER s
                    OPTIONS (table '{ns}.events');
            """)

            # two scans of the same snapshot: the count(*) takes the
            # count-exec path (opts out — not a data scan); sum(amt) is
            # the recorded data scan
            sql(name, "SELECT count(*) FROM events")
            sql(name, "SELECT sum(amt) FROM events")

            r = sql(name, f"""
                SELECT row_count, files_total, files_pruned, scan_tasks,
                       snapshot_id, endpoint, ns_table
                FROM lakehouse.table_stats
                WHERE ns_table = '{ns}.events'
                ORDER BY captured_at
            """).strip()
            lines = [x.split("|") for x in r.split("\n") if x]
            assert len(lines) == 1, f"expected 1 scan-stat row: {r}"
            for row_count, files_total, pruned, tasks, snap, ep, ns_t in lines:
                assert int(row_count) == 4, r
                assert int(files_total) >= 1, r
                assert int(tasks) >= 1, r
                assert snap != "0" and snap != "", r
                assert ep == CATALOG, r
                assert ns_t == f"{ns}.events", r

            # INTERNAL: PUBLIC revoked — an unprivileged role gets 42501
            reader = "scanst_r_" + uuid4().hex[:8]
            sql(name, f"CREATE ROLE {reader} LOGIN")
            sql(name, f"SET ROLE {reader}; "
                      "SELECT 1 FROM lakehouse.table_stats LIMIT 1",
                error="42501")
            sql(name, "RESET ROLE")

            # column_stats: one row per schema field with composed
            # manifest stats + derived tier. value_count_upper is
            # conservative (trino writes non-null counts, not NDV), so
            # tiers land at T1 for these bounded columns; the T2
            # enum-like refinement needs Parquet footer distinct_count
            # (follow-up).
            sql(name, "SELECT sum(amt), sum(id) FROM events")
            r = sql(name, f"""
                SELECT field_name, datatype_class, null_count,
                       value_count_upper, has_bounds, tier
                FROM lakehouse.column_stats
                WHERE ns_table = '{ns}.events'
                ORDER BY field_name, captured_at
            """).strip()
            clines = [x.split("|") for x in r.split("\n") if x]
            assert len(clines) >= 3, f"expected >=3 column rows: {r}"
            for fname, dclass, nulls, vupper, bounds, tier in clines:
                assert dclass in ("numeric", "string", "boolean",
                                  "temporal", "other"), r
                assert tier in ("T1", "T2", "T3", "T4"), r
                assert int(nulls) >= 0 and int(vupper) >= 0, r
            by_name = {x[0]: x for x in clines}
            assert by_name["id"][1] == "numeric", r
            assert by_name["amt"][1] == "numeric", r

            # m2: plain EXPLAIN (EXPLAIN_ONLY) must not add a row
            b4 = int(sql(name, "SELECT count(*) FROM lakehouse.table_stats"))
            sql(name, "EXPLAIN (COSTS OFF) SELECT count(*) FROM events")
            a4 = int(sql(name, "SELECT count(*) FROM lakehouse.table_stats"))
            assert a4 == b4, f"EXPLAIN-only recorded: {a4} != {b4}"

            # a heap-only statement adds NO scan-stats rows
            before = int(sql(name, "SELECT count(*) FROM lakehouse.table_stats"))
            sql(name, "SELECT count(*) FROM pg_class")
            after = int(sql(name, "SELECT count(*) FROM lakehouse.table_stats"))
            assert after == before, f"heap-only scan recorded: {after} != {before}"

            print(json.dumps({"case": "scan-stats",
                              "rows": len(lines),
                              "row_count": int(lines[0][0])}), flush=True)
        finally:
            sql("postgres", f"DROP DATABASE {name} WITH (FORCE)")
    finally:
        trino(f"DROP TABLE IF EXISTS {table}; DROP SCHEMA iceberg.{ns}")


if __name__ == "__main__":
    main()
