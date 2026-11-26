"""Correlated-aggregate pullup: the q1 shape must plan as a grouped
subquery join, not a per-row SubPlan.

q1 shape: a multiply-referenced CTE with a correlated avg-subplan —
`c1.t > (SELECT avg(t) * k FROM ctr c2 WHERE c1.grp = c2.grp)`. The
planner rewrite pulls the aggregate up into a grouped subquery joined
INNER on the correlation keys (one grouped pass + hash probes instead
of O(outer x ctr) tuplestore rescans).

Oracle (per-grp correlated averages): grp A rows 10,20 -> avg 15 ->
thr 18 -> only id 2 (t=20) survives; grp B rows 30,40 -> avg 35 ->
thr 42 -> none of 30/40 survives; the NULL-grp row (id 5) never
matches the correlation — dropped identically by the SubPlan (NULL
comparison) and by the inner join.

Decline controls pin the fail-safe paths: volatile in the subselect,
non-equality correlation (`<`), OR-wrapped sublink, correlated
targetlist Var.
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

# multi-conjunct correlation: per-(grp,id) average = the row's own amt,
# thr = 1.2*amt > amt — nothing survives (equivalence pinned on the
# rewritten plan).
q1_multi = """
    WITH ctr AS (SELECT id, grp, sum(amt) AS t
                 FROM events GROUP BY id, grp)
    SELECT c1.id, c1.t FROM ctr c1
    WHERE c1.t > (SELECT avg(t) * 1.2 FROM ctr c2
                  WHERE c1.grp = c2.grp AND c1.id = c2.id)
"""

# decline-path variants: the SubPlan MUST be retained (fail-safe shapes)
q1_volatile = """
    WITH ctr AS (SELECT id, grp, sum(amt) AS t
                 FROM events GROUP BY id, grp)
    SELECT c1.id, c1.t FROM ctr c1
    WHERE c1.t > (SELECT avg(t) * 1.2 + random() * 0 FROM ctr c2
                  WHERE c1.grp = c2.grp)
"""

# B1/13-review: non-equality correlation must NOT be rewritten
# (group-by-partition != union-of-partitions average)
q1_ineq = """
    WITH ctr AS (SELECT id, grp, sum(amt) AS t
                 FROM events GROUP BY id, grp)
    SELECT c1.id, c1.t FROM ctr c1
    WHERE c1.t > (SELECT avg(t) * 1.2 FROM ctr c2
                  WHERE c1.grp < c2.grp)
"""

# B2/13-review: OR-wrapped sublink must NOT be rewritten
q1_or = """
    WITH ctr AS (SELECT id, grp, sum(amt) AS t
                 FROM events GROUP BY id, grp)
    SELECT c1.id, c1.t FROM ctr c1
    WHERE c1.id = 999 OR c1.t > (SELECT avg(t) * 1.2 FROM ctr c2
                                 WHERE c1.grp = c2.grp)
"""

# M1/13-review: correlated targetlist Var must be declined BEFORE
# pull_var_clause can raise on upper-level Vars
q1_corrtlist = """
    WITH ctr AS (SELECT id, grp, sum(amt) AS t
                 FROM events GROUP BY id, grp)
    SELECT c1.id, c1.t FROM ctr c1
    WHERE c1.t > (SELECT avg(t) * c1.id FROM ctr c2
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
          "(3, 'B', 30.0), (4, 'B', 40.0), (5, NULL, 50.0)")
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
                    OPTIONS (table 'aggp_v2.item');
            """)

            def lines(stmt):
                return [x for x in sql(name, stmt).split("\n") if x]

            # ORACLE: grp A avg 15 -> thr 18 -> only (2, 20.0); grp B
            # avg 35 -> thr 42 -> none; NULL grp never matches the
            # correlation (dropped by both SubPlan and inner join).
            want = [(2, 20.0)]

            def check_rows(query):
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

            # REWRITE PATH: exact rows + no SubPlan + grouped join
            check_rows(q1_shape)
            check_plan(q1_shape, expect_subplan=False)

            # multi-conjunct equality: still rewritten, empty oracle
            got = [x.split("|") for x in sql(name, q1_multi).split("\n") if x]
            assert got == [], got
            check_plan(q1_multi, expect_subplan=False)

            # DECLINE CONTROLS: SubPlan retained, results unchanged
            check_rows(q1_or)  # OR keeps NULL-comparison rows in play
            check_plan(q1_volatile, expect_subplan=True)
            check_plan(q1_ineq, expect_subplan=True)
            check_plan(q1_or, expect_subplan=True)
            check_plan(q1_corrtlist, expect_subplan=True)


    # ---- v2 (foreign-table family): q6 shape over a bare foreign table ----
    v2_q6 = """
        SELECT i.i_item_sk, i.i_current_price FROM item i
        WHERE i.i_current_price > 1.2 * (SELECT avg(j.i_current_price)
                                         FROM item j
                                         WHERE j.i_category = i.i_category)
    """
    v2_multi = """
        SELECT count(*) FROM item i
        WHERE i.i_item_sk > 0
          AND i.i_current_price > (SELECT avg(j.i_current_price)
                                   FROM item j
                                   WHERE j.i_category = i.i_category)
    """
    v2_nullkey = """
        SELECT count(*) FROM item i
        WHERE i.i_current_price > (SELECT avg(j.i_current_price)
                                   FROM item j
                                   WHERE j.i_category = i.i_category)
    """
    v2_ineq = """
        SELECT i.i_item_sk FROM item i
        WHERE i.i_current_price > (SELECT avg(j.i_current_price)
                                   FROM item j
                                   WHERE j.i_category < i.i_category)
    """
    # q6 pullup fires on the bare-foreign-table shape
    r = sql(name, "EXPLAIN (COSTS OFF) " + v2_q6)
    assert "agg_pullup" in r and "SubPlan" not in r, r
    # rewritten vs grouped-oracle identical
    got = sql(name, "SELECT count(*), sum(i.i_item_sk) FROM item i WHERE "
                    "i.i_current_price > (SELECT avg(j.i_current_price) "
                    "FROM item j WHERE j.i_category = i.i_category)").strip()
    oracle = sql(name, "SELECT count(*), sum(sk) FROM (SELECT i.i_item_sk sk "
                       "FROM item i JOIN (SELECT i_category cat, "
                       "avg(i_current_price) thr FROM item "
                       "GROUP BY i_category) a ON i.i_category = a.cat "
                       "WHERE i.i_current_price > a.thr) x").strip()
    assert got == oracle, (got, oracle)
    # multi-conjunct: outer filter + correlated equality (AND path)
    got2 = sql(name, v2_multi).strip()
    oracle2 = sql(name, "SELECT count(*) FROM item i WHERE i.i_item_sk > 0 "
                        "AND i.i_current_price > (SELECT avg(j.i_current_price) "
                        "FROM item j WHERE j.i_category = i.i_category)").strip()
    assert got2 == oracle2, (got2, oracle2)
    # inequality correlation declines (SubPlan retained)
    r = sql(name, "EXPLAIN (COSTS OFF) " + v2_ineq)
    assert "SubPlan" in r, r

    print(json.dumps({"case": "agg-pullup-v2",
                      "rewritten_rows": int(got.split("|")[0]),
                      "oracle_rows": int(oracle.split("|")[0])}), flush=True)

            print(json.dumps({"case": "agg-pullup",
                              "rows": 1,
                              "decline_controls": 4}), flush=True)
        finally:
            sql("postgres", f"DROP DATABASE {name} WITH (FORCE)")
    finally:
        trino(f"DROP TABLE IF EXISTS {table}; DROP SCHEMA iceberg.{ns}")


if __name__ == "__main__":
    main()
