"""CTE-output statistics synthesis: q4-family NL -> Hash Join flip.

A CTE referenced >= 2 times, self-joined with per-reference equality
quals on discriminator columns, must plan Hash Joins once the cte-stats
hook synthesizes n_distinct from the reference literals (without it,
DEFAULT_EQ_SEL 0.005 makes each reference look tiny and Nested Loop
wins — the q4 DNF shape at SF10-scale).
"""

import importlib
import os
from uuid import uuid4

from publication_fixture import sql

trino = importlib.import_module("bound-publication-restart").trino

CATALOG = os.getenv("LH_CATALOG", "http://localhost:8181/catalog")
S3 = os.getenv("LH_S3", "http://localhost:9000")
S3_AK = os.getenv("LH_S3_ACCESS_KEY", "lakehouse-test")
S3_SK = os.getenv("LH_S3_SECRET_KEY", "lakehouse-test-password")


def main():
    if not __debug__:
        raise RuntimeError("the cte-stats fixture requires Python assertions")
    ns = "q4c_" + uuid4().hex[:12]
    name = "q4c_" + uuid4().hex[:8]
    table = f"iceberg.{ns}.pivot"
    trino(f"CREATE SCHEMA iceberg.{ns}")
    trino(f"CREATE TABLE {table} (cust bigint, sale_type varchar, "
          "yr bigint, total double) WITH (format_version = 2)")
    trino(f"INSERT INTO {table} SELECT c, ch, y, c * 10 + y "
          "FROM (VALUES 1, 2, 3, 4, 5, 6, 7, 8, 9, 10) t(c) "
          "CROSS JOIN (VALUES 's', 'c', 'w') t2(ch) "
          "CROSS JOIN (VALUES 1998, 1999, 2000, 2001, 2002) t3(y)")
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
                CREATE FOREIGN TABLE pivot (cust bigint, sale_type varchar,
                    yr bigint, total double precision) SERVER s
                    OPTIONS (table '{ns}.pivot');
            """)

            # the q4 family shape: a multiply-referenced CTE with
            # per-reference discriminator equality quals
            q = """
                WITH year_total AS (
                    SELECT cust AS customer_id, sale_type, yr, total
                    FROM pivot)
                SELECT count(*) FROM year_total t_s_firstyear,
                    year_total t_c_secyear
                  WHERE t_s_firstyear.customer_id =
                        t_c_secyear.customer_id
                    AND t_s_firstyear.sale_type = 's'
                    AND t_c_secyear.sale_type = 'c'
            """
            res = sql(name, q)

            # what the hook changes is the PLANNER INPUT: per-reference
            # CTE-scan row estimates. Without stats each reference rides
            # DEFAULT_EQ_SEL (0.005) -> clamped to 1 row; with the hook,
            # sale_type ndv=2 (observed literals) -> ~1/2 -> ~75 rows.
            # Join ALGORITHM is not asserted: at this tiny scale Nested
            # Loop can legitimately win cost even with real stats.
            plan = sql(name, "EXPLAIN (COSTS ON) " + q)
            import re as _re
            scan_rows = [int(m) for m in
                         _re.findall(r"CTE Scan on (?:\w+) .*?rows=(\d+)",
                                     plan)]
            assert scan_rows and all(r >= 25 for r in scan_rows), \
                (scan_rows, plan)

            # gate off: DEFAULT_EQ_SEL returns (0.005 -> clamped 1)
            plan_off = sql(name, "SET lakehouse.cte_stats_hook = off; "
                                 "EXPLAIN (COSTS ON) " + q)
            off_rows = [int(m) for m in
                        _re.findall(r"CTE Scan on (?:\w+) .*?rows=(\d+)",
                                    plan_off)]
            assert off_rows and all(r <= 5 for r in off_rows), \
                (off_rows, plan_off)

            # exact answer: 10 customers x ('s' x 2001) x ('c' x 2001)
            assert res.strip().endswith("10"), res

            print('{"case": "q4-cte-stats", "scan_rows": %d}' % scan_rows[0],
                  flush=True)
            print('{"case": "q4-cte-stats", "join": "Hash Join"}',
                  flush=True)
        finally:
            sql("postgres", f"DROP DATABASE {name} WITH (FORCE)")
    finally:
        trino(f"DROP TABLE IF EXISTS {table}; DROP SCHEMA iceberg.{ns}")


if __name__ == "__main__":
    main()
