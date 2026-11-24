"""CTE-inlining barrier: multi-referenced CTEs over foreign tables must
materialize ONCE (spec 03; TPC-DS q1 diagnosis).

A q1-shaped query — a multiply-referenced CTE with a correlated
avg-subquery — must show exactly ONE Foreign Scan (the CTE materializes
once; both references read the tuplestore), and the correlated result
must equal the uncorrelated equivalent computed directly.
"""

import importlib
import json
from uuid import uuid4

from publication_fixture import sql

trino = importlib.import_module("bound-publication-restart").trino

q1_shape = """
    WITH ctr AS (SELECT id, grp, sum(amt) AS t
                 FROM events GROUP BY id, grp)
    SELECT c1.id, c1.t FROM ctr c1
    WHERE c1.t > (SELECT avg(t) * 1.2 FROM ctr c2
                  WHERE c1.grp = c2.grp)
"""

CATALOG = "http://localhost:8181/catalog"
S3 = "http://localhost:9000"


def main():
    if not __debug__:
        raise RuntimeError("the barrier fixture requires Python assertions")
    ns = "cteb_" + uuid4().hex[:12]
    import os
    os.environ.setdefault("PGHOST", "/tmp/wt-sock")
    os.environ.setdefault("PGPORT", "5598")
    os.environ.setdefault("PGUSER", "vsingh")
    os.environ.setdefault("PGDATABASE", "postgres")
    name = "cteb_" + uuid4().hex[:8]
    table = f"iceberg.{ns}.events"
    trino(f"CREATE SCHEMA iceberg.{ns}")
    trino(f"CREATE TABLE {table} (id bigint, grp varchar, amt double) WITH (format_version = 2)")
    trino(f"INSERT INTO {table} VALUES (1, 'A', 10.0), (2, 'A', 20.0), (3, 'B', 30.0), (4, 'B', 40.0)")
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
                    (s3_access_key 'lakehouse-test', s3_secret_key 'lakehouse-test-password');
                CREATE FOREIGN TABLE events (id bigint, grp text, amt double precision) SERVER s
                    OPTIONS (table '{ns}.events');
            """)

            # warmup: load the library + install the planner hook in
            # this backend before planning the q1 shape
            sql(name, "SELECT count(*) FROM events")

            # BARRIER SHAPE: exactly one Foreign Scan (the CTE
            # materializes once); both references are CTE Scans.
            plan = sql(name, "EXPLAIN (COSTS OFF) " + q1_shape)
            assert plan.count("Foreign Scan on events") == 1, plan
            assert "CTE Scan on ctr c1" in plan, plan
            assert "CTE Scan on ctr c2" in plan, plan

            # FUNCTIONAL: correlated avg per grp over the ctr rows
            # (A: avg(10,20)=15 -> thr 18; B: avg(30,40)=35 -> thr 42):
            # only id 2 (t=20 > 18) survives — a deterministic oracle
            # for the correlated shape.
            got = [x.split("|") for x in sql(name, q1_shape).split("\n") if x]
            got = sorted((int(i), float(v)) for i, v in got)
            want = [(2, 20.0)]
            assert got == want, (got, want)
            print(json.dumps({"case": "cte-inlining-barrier",
                              "foreign_scans": 1,
                              "cte_scans": 2,
                              "rows": len(got)}), flush=True)
        finally:
            sql("postgres", f"DROP DATABASE {name} WITH (FORCE)")
    finally:
        trino(f"DROP TABLE IF EXISTS {table}; DROP SCHEMA iceberg.{ns}")


if __name__ == "__main__":
    main()
