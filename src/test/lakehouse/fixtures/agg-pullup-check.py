"""Correlated-aggregate pullup: the q1 shape must plan as a grouped
subquery join, not a per-row SubPlan.

q1 shape: a multiply-referenced CTE with a correlated avg-subplan —
`c1.t > (SELECT avg(t) * k FROM ctr c2 WHERE c1.grp = c2.grp)`. The
planner rewrite pulls the aggregate up into a grouped subquery joined
LEFT (one grouped pass + hash probes instead of O(outer x ctr)
tuplestore rescans).

Oracle (per-grp correlated averages): grp A rows 10,20 -> avg 15 ->
thr 18 -> only id 2 (t=20) survives; grp B rows 30,40 -> avg 35 ->
thr 42 -> none of 30/40 survives.
"""

import importlib
import json
from uuid import uuid4

from publication_fixture import sql

trino = importlib.import_module("bound-publication-restart").trino

CATALOG = "http://localhost:8181/catalog"
S3 = "http://localhost:9000"

q1_shape = """
    WITH ctr AS (SELECT id, grp, sum(amt) AS t
                 FROM events GROUP BY id, grp)
    SELECT c1.id, c1.t FROM ctr c1
    WHERE c1.t > (SELECT avg(t) * 1.2 FROM ctr c2
                  WHERE c1.grp = c2.grp)
"""

# decline-path variant: volatile inside the subselect must NOT be
# rewritten (SubPlan retained — fail-safe control)
q1_volatile = """
    WITH ctr AS (SELECT id, grp, sum(amt) AS t
                 FROM events GROUP BY id, grp)
    SELECT c1.id, c1.t FROM ctr c1
    WHERE c1.t > (SELECT avg(t) * 1.2 + random() * 0 FROM ctr c2
                  WHERE c1.grp = c2.grp)
"""


def main():
    if not __debug__:
        raise RuntimeError("the pullup fixture requires Python assertions")
    ns = "aggp_" + uuid4().hex[:12]
    name = "aggp_" + uuid4().hex[:8]
    table = f"iceberg.{ns}.events"
    trino(f"CREATE SCHEMA iceberg.{ns}")
    trino(f"CREATE TABLE {table} (id bigint, grp varchar, amt double) "
          "WITH (format_version = 2)")
    trino(f"INSERT INTO {table} VALUES (1, 'A', 10.0), (2, 'A', 20.0), "
          "(3, 'B', 30.0), (4, 'B', 40.0)")
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
                CREATE FOREIGN TABLE events (id bigint, grp varchar, amt double precision) SERVER s
                    OPTIONS (table '{ns}.events');
            """)

            def lines(stmt):
                return [x for x in sql(name, stmt).split("\n") if x]

            # ORACLE: grp A avg 15 -> thr 18 -> only (2, 20.0); grp B
            # avg 35 -> thr 42 -> none.
            want = [(2, 20.0)]

            # warmup: load the library + install the planner hook in
            # this backend before the q1 shape plans (otherwise the
            # rewrite pass cannot fire)
            sql(name, "SELECT count(*) FROM events")

            got = [x.split("|") for x in sql(name, q1_shape).split("\n") if x]
            got = sorted((int(i), float(v)) for i, v in got)
            assert got == want, (got, want)

            # EXPLAIN: the rewrite must remove the per-row SubPlan and
            # show the pulled-up grouped subquery.
            plan = sql(name, "EXPLAIN (COSTS OFF) " + q1_shape)
            assert "SubPlan" not in plan, plan
            assert "agg_pullup" in plan, plan

            # DECLINE control: volatile in the subselect keeps the
            # SubPlan (fail-safe shape).
            vplan = sql(name, "EXPLAIN (COSTS OFF) " + q1_volatile)
            assert "SubPlan" in vplan, vplan

            print(json.dumps({"case": "agg-pullup",
                              "rows": len(got),
                              "want": want}), flush=True)
        finally:
            sql("postgres", f"DROP DATABASE {name} WITH (FORCE)")
    finally:
        trino(f"DROP TABLE IF EXISTS {table}; DROP SCHEMA iceberg.{ns}")


if __name__ == "__main__":
    main()
