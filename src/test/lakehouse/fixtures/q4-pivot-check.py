"""q4-pivot fold: multiply-referenced UNION-ALL grouped CTE (spec 19 §5
entry 3).

The fold rewrites each reference's discriminator quals into FILTERed
aggregate clones over the coarsened GROUP BY, so the references must
return exactly the rows the un-rewritten self-join returns — including
empty-subset handling: a group with no rows in a reference's year yields
a NULL clone, which only survives equivalence because every redirected
column is consumed by a strict comparison. Gate A/B: lakehouse.q4_pivot
off vs on, plus a DEBUG1 receipt assert so a silent decline cannot pass
the fixture (the B1/verification lesson).

Planner-generic by construction: the rewrite is a parse-tree transform,
so the fixture runs on plain SQL tables and needs no iceberg/trino.
"""

import os
import subprocess
import sys

from publication_fixture import sql

PSQL = [os.getenv("PSQL", "psql"), "-XqAt", "-v", "ON_ERROR_STOP=1"]


def psql_both(db, statements):
    """Run statements, returning (stdout, stderr) for DEBUG-receipt asserts."""
    result = subprocess.run(PSQL + ["-d", db], input=statements,
                            capture_output=True, text=True, timeout=35,
                            env=dict(os.environ, PGCONNECT_TIMEOUT="5"))
    if result.returncode:
        raise RuntimeError(result.stderr)
    return result.stdout, result.stderr


SETUP = """
CREATE TABLE sales_a (cust int, yr int, amt float8);
CREATE TABLE sales_b (cust int, yr int, amt float8);
-- cust 1: present in both branches and both years
-- cust 2: only 2001 in branch a  -> t2 (2002) subset empty -> NULL clone
-- cust 3: zero 2002 amount       -> strict guard must drop it
-- cust 4: only branch a          -> no t2 row at all
-- cust 5: only branch b          -> no t1 row at all
INSERT INTO sales_a VALUES (1,2001,10),(1,2002,20),(2,2001,30),
                           (3,2002,0),(4,2001,5),(4,2002,7);
INSERT INTO sales_b VALUES (1,2001,100),(1,2002,200),(3,2002,0),(5,2002,9);
"""

# q4 family: UNION ALL of two grouped SELECTs, referenced twice with
# per-reference discriminator quals (sale_type const, yr const incl. the
# `2002 + 0` non-folded form), strict guards, and a CASE ratio qual.
QUERY = """
WITH year_total AS (
    SELECT cust, yr, sum(amt) ytotal, 'a'::text sale_type
    FROM sales_a GROUP BY cust, yr
    UNION ALL
    SELECT cust, yr, sum(amt) ytotal, 'b'::text sale_type
    FROM sales_b GROUP BY cust, yr)
SELECT t1.cust, t1.ytotal, t2.ytotal
FROM year_total t1, year_total t2
WHERE t1.cust = t2.cust
  AND t1.sale_type = 'a' AND t2.sale_type = 'b'
  AND t1.yr = 2001 AND t2.yr = 2002 + 0
  AND t1.ytotal > 0 AND t2.ytotal > 0
  AND CASE WHEN t2.ytotal > 0 THEN t2.ytotal / t1.ytotal ELSE NULL END > 0
ORDER BY t1.cust;
"""

# exact oracle: only cust 1 has a (a,2001) x (b,2002) pair with both sums > 0
ORACLE = "1|10|200"


def main():
    if not __debug__:
        raise RuntimeError("the q4-pivot fixture requires Python assertions")
    name = "q4pivot_" + os.urandom(4).hex()
    sql("postgres", f"CREATE DATABASE {name} TEMPLATE template0 "
                    f"ENCODING 'UTF8' LOCALE 'C'")
    try:
        sql(name, SETUP)

        # gate OFF: the ordinary plan is the oracle
        out_off, err_off = psql_both(
            name, "SET lakehouse.q4_pivot = off;\n" + QUERY)
        assert out_off.strip() == ORACLE, f"oracle mismatch: {out_off!r}"
        assert "q4-pivot: folded" not in err_off, err_off

        # gate ON: fold must engage and results must be identical
        out_on, err_on = psql_both(
            name, "SET client_min_messages = debug1;\n"
                  "SET lakehouse.q4_pivot = on;\n" + QUERY)
        assert out_on.strip() == ORACLE, f"folded result mismatch: {out_on!r}"
        assert "q4-pivot: folded 2 references into 2 keys" in err_on, err_on

        # every reference shape that must decline leaves results intact
        declines = {
            "unconditioned reference": """
                WITH cte AS (
                    SELECT cust, yr, sum(amt) ytotal FROM sales_a GROUP BY cust, yr
                    UNION ALL
                    SELECT cust, yr, sum(amt) FROM sales_b GROUP BY cust, yr)
                SELECT count(*) FROM cte t1, cte t2
                WHERE t1.cust = t2.cust AND t1.yr = 2001;
            """,
            "OR over redirected column": """
                WITH cte AS (
                    SELECT cust, yr, sum(amt) ytotal FROM sales_a GROUP BY cust, yr)
                SELECT count(*) FROM cte t1, cte t2
                WHERE t1.cust = t2.cust AND t1.yr = 2001 AND t2.yr = 2002
                  AND (t1.ytotal > 0 OR t2.ytotal > 0);
            """,
            "COALESCE absorbs NULL": """
                WITH cte AS (
                    SELECT cust, yr, sum(amt) ytotal FROM sales_a GROUP BY cust, yr)
                SELECT count(*) FROM cte t1, cte t2
                WHERE t1.cust = t2.cust AND t1.yr = 2001 AND t2.yr = 2002
                  AND coalesce(t1.ytotal, 0) + coalesce(t2.ytotal, 0) > 0;
            """,
        }
        for label, q in declines.items():
            out, err = psql_both(
                name, "SET client_min_messages = debug1;\n"
                      "SET lakehouse.q4_pivot = on;\n" + q)
            assert "q4-pivot: folded" not in err, f"{label}: fold must decline"
            out_off2, _ = psql_both(
                name, "SET lakehouse.q4_pivot = off;\n" + q)
            assert out.strip() == out_off2.strip(), \
                f"{label}: decline path changed results ({out!r} vs {out_off2!r})"

        # ---- mixed branch types (real TPC-DS schemas mix numeric and
        # double precision money columns): the set-op machinery coerces
        # each branch's clone to the resolved CTE type
        sql(name, """
            CREATE TABLE m_a (cust int, yr int, amt numeric);
            CREATE TABLE m_b (cust int, yr int, amt double precision);
            INSERT INTO m_a VALUES (1,2001,10::numeric),(1,2002,20::numeric),
                                   (2,2001,30::numeric);
            INSERT INTO m_b VALUES (1,2001,100),(1,2002,200);
        """)
        mixed = """
            WITH t AS (
                SELECT cust, yr, sum(amt) ytotal, 'a'::text st FROM m_a GROUP BY cust, yr
                UNION ALL
                SELECT cust, yr, sum(amt) ytotal, 'b'::text st FROM m_b GROUP BY cust, yr)
            SELECT t1.cust, t1.ytotal, t2.ytotal FROM t t1, t t2
            WHERE t1.cust = t2.cust AND t1.st = 'a' AND t2.st = 'b'
              AND t1.yr = 2001 AND t2.yr = 2002 + 0
              AND t1.ytotal > 0 AND t2.ytotal > 0
            ORDER BY t1.cust;
        """
        out_offm, _ = psql_both(name, "SET lakehouse.q4_pivot = off;\n" + mixed)
        out_onm, err_onm = psql_both(
            name, "SET client_min_messages = debug1;\n"
                  "SET lakehouse.q4_pivot = on;\n" + mixed)
        assert out_onm.strip() == "1|10|200", f"mixed-type fold wrong: {out_onm!r}"
        assert out_onm.strip() == out_offm.strip(), "mixed-type A/B mismatch"
        assert "q4-pivot: folded 2 references into 2 keys" in err_onm, err_onm

        # ---- review round-8 regressions: each must either decline
        # pre-mutation (gate on == gate off) or fold without changing rows

        # blocker 1: non-trailing discriminator groupexpr (branch GROUP BY
        # orders differ) — fold must decline, never half-mutate
        blocker1 = """
            WITH v AS (
                SELECT cust, yr, sum(amt) ytotal FROM sales_a GROUP BY cust, yr
                UNION ALL
                SELECT cust, yr, sum(amt) ytotal FROM sales_b GROUP BY yr, cust)
            SELECT count(*) FROM v t1, v t2
            WHERE t1.cust = t2.cust AND t1.yr = 2001 AND t2.yr = 2002
              AND t1.ytotal > 0 AND t2.ytotal > 0;
        """
        # blocker 2: ref's agg column consumed in HAVING / join ON /
        # SubLink subselect — remap does not cover those locations
        having_case = """
            WITH cte AS (
                SELECT cust, yr, sum(amt) ytotal FROM sales_a GROUP BY cust, yr)
            SELECT count(*) FROM cte t1, cte t2
            WHERE t1.cust = t2.cust AND t1.yr = 2001 AND t2.yr = 2002
              AND t1.ytotal > 0
            GROUP BY t1.cust HAVING sum(t1.ytotal) > 10;
        """
        on_case = """
            WITH cte AS (
                SELECT cust, yr, sum(amt) ytotal FROM sales_a GROUP BY cust, yr)
            SELECT count(*) FROM cte t1 JOIN cte t2
              ON t1.cust = t2.cust AND t2.ytotal < 50
            WHERE t1.yr = 2001 AND t2.yr = 2002 AND t1.ytotal > 0;
        """
        sublink_case = """
            WITH cte AS (
                SELECT cust, yr, sum(amt) ytotal FROM sales_a GROUP BY cust, yr)
            SELECT count(*) FROM cte t1
            WHERE t1.yr = 2001 AND t1.ytotal > 0
              AND EXISTS (SELECT 1 FROM sales_b b
                          WHERE b.cust = t1.cust AND b.amt < t1.ytotal);
        """
        whole_row_case = """
            WITH cte AS (
                SELECT cust, yr, sum(amt) ytotal FROM sales_a GROUP BY cust, yr)
            SELECT count(*) FROM cte t1, cte t2
            WHERE t1.cust = t2.cust AND t1.yr = 2001 AND t2.yr = 2002
              AND t1 > t2;
        """
        subselect_on_case = """
            WITH cte AS (
                SELECT cust, yr, sum(amt) ytotal FROM sales_a GROUP BY cust, yr)
            SELECT count(*) FROM cte t1
            WHERE t1.yr = 2001 AND t1.ytotal > 0
              AND EXISTS (SELECT 1 FROM sales_a x1 JOIN sales_a x2
                            ON x1.cust = x2.cust AND x2.amt < t1.ytotal
                          WHERE x1.yr = 2002);
        """
        for label, q in {
            "non-trailing groupexpr": blocker1,
            "HAVING over agg col": having_case,
            "join ON over agg col": on_case,
            "SubLink interior agg col": sublink_case,
            "subselect ON over agg col": subselect_on_case,
            "whole-row ref": whole_row_case,
        }.items():
            out_on2, err_on2 = psql_both(
                name, "SET client_min_messages = debug1;\n"
                      "SET lakehouse.q4_pivot = on;\n" + q)
            out_off2, _ = psql_both(
                name, "SET lakehouse.q4_pivot = off;\n" + q)
            assert out_on2.strip() == out_off2.strip(), \
                f"{label}: gate A/B mismatch ({out_on2!r} vs {out_off2!r})"
            # either declined pre-mutation, or (if folded) results held —
            # the A/B equality above is the binding assertion

        print("q4-pivot-check: ok (oracle A/B equal, fold engaged, "
              "declines preserved, mixed types, r8 regressions)")
    finally:
        sql("postgres", f"DROP DATABASE {name}")


if __name__ == "__main__":
    sys.exit(main())
