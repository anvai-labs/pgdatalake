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

        print("q4-pivot-check: ok (oracle A/B equal, fold engaged, "
              "declines preserved)")
    finally:
        sql("postgres", f"DROP DATABASE {name}")


if __name__ == "__main__":
    sys.exit(main())
