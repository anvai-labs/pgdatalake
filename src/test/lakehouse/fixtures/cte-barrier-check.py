"""CTE-inlining barrier: multi-referenced CTEs over foreign tables must
materialize ONCE (spec 03; TPC-DS q1 diagnosis).

A q1-family shape — a multiply-referenced CTE with a subquery in the
WHERE — must show exactly ONE Foreign Scan (the CTE materializes once;
both references read the tuplestore). This fixture pins the
STOCK materialization semantics on a pullup-immune shape: the
subselect is UNCORRELATED (no WHERE), which the correlated-agg pullup
declines, so the SubPlan is retained and the barrier asserts describe
the untouched planner behavior.
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
    WHERE c1.t > (SELECT avg(t) * 1.2 FROM ctr c2)
"""

CATALOG = "http://localhost:8181/catalog"
S3 = "http://localhost:9000"


def main():
    if not __debug__:
        raise RuntimeError("the barrier fixture requires Python assertions")
    ns = "cteb_" + uuid4().hex[:12]
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

            # BARRIER SHAPE: exactly one Foreign Scan (the CTE
            # materializes once); both references are CTE Scans. The
            # subselect is uncorrelated, so the correlated-agg pullup
            # declines and the subquery survives as a stock InitPlan —
            # these asserts pin stock planner behavior.
            plan = sql(name, "EXPLAIN (COSTS OFF) " + q1_shape)
            assert plan.count("Foreign Scan on events") == 1, plan
            assert "CTE Scan on ctr c1" in plan, plan
            assert "CTE Scan on ctr c2" in plan, plan
            assert "InitPlan" in plan, plan
            assert "agg_pullup" not in plan, plan

            # FUNCTIONAL: uncorrelated avg over all 4 ctr rows = 25 ->
            # thr 30 -> only id 4 (t=40) survives.
            got = [x.split("|") for x in sql(name, q1_shape).split("\n") if x]
            got = sorted((int(i), float(v)) for i, v in got)
            want = [(4, 40.0)]
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
