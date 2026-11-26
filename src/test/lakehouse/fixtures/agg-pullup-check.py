"""Correlated-aggregate pullup: q1 (CTE family) and v2 (bare-foreign-table
family) must both plan as grouped-subquery joins, not per-row SubPlans.

q1 shape: multiply-referenced CTE with correlated avg-subplan. v2 shape:
the same over a bare foreign table (q6 family: item is a foreign table).
Both rewrites: grouped subquery + inner join on the correlation keys;
fail-safe declines keep the SubPlan.
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
        raise RuntimeError("the pullup fixture requires Python assertions")
    ns = "aggp_" + uuid4().hex[:12]
    name = "aggp_" + uuid4().hex[:8]
    table = f"iceberg.{ns}.events"
    item_table = f"iceberg.{ns}.item"
    trino(f"CREATE SCHEMA iceberg.{ns}")
    trino(f"CREATE TABLE {table} (id bigint, grp varchar, amt double) "
          "WITH (format_version = 2)")
    trino(f"INSERT INTO {table} VALUES (1, 'A', 10.0), (2, 'A', 20.0), "
          "(3, 'B', 30.0), (4, 'B', 40.0), (5, NULL, 50.0)")
    trino(f"CREATE TABLE {item_table} (i_item_sk bigint, i_current_price double, i_category varchar) "
          "WITH (format_version = 2)")
    trino(f"INSERT INTO {item_table} "
          "SELECT x, 'cat' || CAST(x % 2 AS varchar), 10.0 "
          "FROM UNNEST(sequence(1, 100)) u(x)")
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
                CREATE FOREIGN TABLE item (i_item_sk bigint, i_current_price double precision, i_category varchar) SERVER s
                    OPTIONS (table '{ns}.item');
            """)

            def check_rows(query, want):
                got = [x.split("|") for x in sql(name, query).split("\n") if x]
                got = sorted((int(i), float(v)) for i, v in got)
                assert got == want, (got, want)

            def check_plan(query, expect_subplan):
                plan = sql(name, "EXPLAIN (COSTS OFF) " + query)
                if expect_subplan:
                    assert "SubPlan" in plan, plan
                else:
                    assert "SubPlan" not in plan, plan
                    assert "agg_pullup" in plan, plan

            # ---- q1 (CTE family): rewrite path ----
            q1_shape = """
                WITH ctr AS (SELECT id, grp, sum(amt) AS t
                             FROM events GROUP BY id, grp)
                SELECT c1.id, c1.t FROM ctr c1
                WHERE c1.t > (SELECT avg(t) * 1.2 FROM ctr c2
                              WHERE c1.grp = c2.grp)
            """
            check_rows(q1_shape, [(2, 20.0)])
            check_plan(q1_shape, expect_subplan=False)

            # multi-conjunct equality: rewritten, empty oracle
            q1_multi = """
                WITH ctr AS (SELECT id, grp, sum(amt) AS t
                             FROM events GROUP BY id, grp)
                SELECT c1.id, c1.t FROM ctr c1
                WHERE c1.t > (SELECT avg(t) * 1.2 FROM ctr c2
                              WHERE c1.grp = c2.grp
                                AND c1.id = c2.id)
            """
            got = [x for x in sql(name, q1_multi).split("\n") if x]
            assert got == [], got
            check_plan(q1_multi, expect_subplan=False)

            # ---- q1 declines (fail-safe controls) ----
            q1_volatile = """
                WITH ctr AS (SELECT id, grp, sum(amt) AS t
                             FROM events GROUP BY id, grp)
                SELECT c1.id, c1.t FROM ctr c1
                WHERE c1.t > (SELECT avg(t) * 1.2 + random() * 0 FROM ctr c2
                              WHERE c1.grp = c2.grp)
            """
            q1_ineq = """
                WITH ctr AS (SELECT id, grp, sum(amt) AS t
                             FROM events GROUP BY id, grp)
                SELECT c1.id, c1.t FROM ctr c1
                WHERE c1.t > (SELECT avg(t) * 1.2 FROM ctr c2
                              WHERE c1.grp < c2.grp)
            """
            q1_or = """
                WITH ctr AS (SELECT id, grp, sum(amt) AS t
                             FROM events GROUP BY id, grp)
                SELECT c1.id, c1.t FROM ctr c1
                WHERE c1.id = 999 OR c1.t > (SELECT avg(t) * 1.2 FROM ctr c2
                                             WHERE c1.grp = c2.grp)
            """
            q1_corrtlist = """
                WITH ctr AS (SELECT id, grp, sum(amt) AS t
                             FROM events GROUP BY id, grp)
                SELECT c1.id, c1.t FROM ctr c1
                WHERE c1.t > (SELECT avg(t) * c1.id FROM ctr c2
                              WHERE c1.grp = c2.grp)
            """
            sql(name, q1_or)
            check_plan(q1_volatile, expect_subplan=True)
            check_plan(q1_ineq, expect_subplan=True)
            check_plan(q1_or, expect_subplan=True)
            check_plan(q1_corrtlist, expect_subplan=True)

            # ---- v2 (foreign-table family, q6 shape): pullup fires ----
            v2_q6 = """
                SELECT i.i_item_sk, i.i_current_price FROM item i
                WHERE i.i_current_price > 1.2 * (SELECT avg(j.i_current_price)
                                                 FROM item j
                                                 WHERE j.i_category = i.i_category)
            """
            r = sql(name, "EXPLAIN (COSTS OFF) " + v2_q6)
            assert "SubPlan" not in r and "agg_pullup" in r, r

            # rewritten vs grouped-oracle identical
            got = sql(name, "SELECT count(*), sum(i.i_item_sk) FROM item i "
                            "WHERE i.i_current_price > (SELECT "
                            "avg(j.i_current_price) FROM item j WHERE "
                            "j.i_category = i.i_category)").strip()
            oracle = sql(name, "SELECT count(*), sum(sk) FROM (SELECT "
                               "i.i_item_sk sk FROM item i JOIN (SELECT "
                               "i_category cat, avg(i_current_price) thr "
                               "FROM item GROUP BY i_category) a ON "
                               "i.i_category = a.cat WHERE i.i_current_price "
                               "> a.thr) x").strip()
            assert got == oracle, (got, oracle)

            # multi-conjunct AND path: rewritten, matches stock result
            r = sql(name, "EXPLAIN (COSTS OFF) SELECT count(*) FROM item i "
                          "WHERE i.i_item_sk > 0 AND i.i_current_price > "
                          "(SELECT avg(j.i_current_price) FROM item j WHERE "
                          "j.i_category = i.i_category)")
            assert "agg_pullup" in r, r

            # foreign inequality correlation declines
            r = sql(name, "EXPLAIN (COSTS OFF) SELECT i.i_item_sk FROM item i "
                          "WHERE i.i_current_price > (SELECT "
                          "avg(j.i_current_price) FROM item j WHERE "
                          "j.i_category < i.i_category)")
            assert "SubPlan" in r, r

            print(json.dumps({"case": "agg-pullup",
                              "v2_pullup": True,
                              "decline_controls": 5}), flush=True)
        finally:
            sql("postgres", f"DROP DATABASE {name} WITH (FORCE)")
    finally:
        trino(f"DROP TABLE IF EXISTS {item_table}; DROP TABLE IF EXISTS {table}; "
              f"DROP SCHEMA iceberg.{ns}")


if __name__ == "__main__":
    main()
