"""Measure both scans in one backend across two independently opened handles.

Each statement must read through its current storage authority. Cache hits
within a handle remain covered by the Rust loopback and range tests.
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


def statement_cache_receipts(name, query):
    """Two scans + three counter reads in ONE psql invocation: the block
    cache is per-backend, so separate sql() calls (fresh backend each)
    would all read zeros. Returns (before, after_scan1, after_scan2)."""
    reads = ("SELECT hits, misses, fetched_bytes, used_bytes, entries "
             "FROM lakehouse.cache_stats()")
    out = sql(name, f"{reads}; {query}; {reads}; {query}; {reads}")
    results = out.splitlines()
    assert [line for line in results if "|" not in line] == ["2001000", "2001000"], out
    lines = [line for line in results if "|" in line]
    assert len(lines) == 3, out

    def parse(ln):
        h, m, f, u, e = [int(x) for x in ln.split("|")]
        return {"hits": h, "misses": m, "fetched": f, "used": u,
                "entries": e}

    return parse(lines[0]), parse(lines[1]), parse(lines[2])


def main():
    if not __debug__:
        raise RuntimeError("the cache fixture requires Python assertions")
    ns = "ccs_" + uuid4().hex[:12]
    name = "ccs_" + uuid4().hex[:8]
    table = f"iceberg.{ns}.events"
    trino(f"CREATE SCHEMA iceberg.{ns}")
    trino(f"CREATE TABLE {table} (id bigint, grp varchar) "
          "WITH (format_version = 2)")
    trino(f"INSERT INTO {table} "
          "SELECT x, CAST(x % 5 AS varchar) FROM UNNEST(sequence(1, 2000)) u(x)")
    try:
        sql("postgres", f"CREATE DATABASE {name} TEMPLATE template0 "
                        f"ENCODING 'UTF8' LOCALE 'C'")
        try:
            sql(name, f"""
                CREATE EXTENSION lakehouse;
                ALTER EXTENSION lakehouse UPDATE TO '1.25';
                CREATE FOREIGN DATA WRAPPER lh HANDLER lakehouse.lakehouse_fdw_handler;
                CREATE SERVER s FOREIGN DATA WRAPPER lh OPTIONS
                    (endpoint '{CATALOG}', warehouse 'smoke', s3_endpoint '{S3}');
                CREATE USER MAPPING FOR CURRENT_USER SERVER s OPTIONS
                    (s3_access_key '{S3_AK}', s3_secret_key '{S3_SK}');
                CREATE FOREIGN TABLE events (id bigint, grp varchar) SERVER s
                    OPTIONS (table '{ns}.events');
            """)

            a, b, c = statement_cache_receipts(name, "SELECT sum(id) FROM events")
            assert b["fetched"] > a["fetched"], (a, b)
            assert c["fetched"] > b["fetched"], (b, c)
            assert c["misses"] > b["misses"], (b, c)

            counters = "SELECT hits, misses, fetched_bytes FROM lakehouse.cache_stats()"
            # COUNT uses a separate first-demand scan path. The GUC must
            # apply before this path runs even with no prior ordinary scan.
            off = sql(name, "SET lakehouse.cache_enabled = off; "
                            f"SELECT count(*) FROM events; {counters}")
            assert off.splitlines() == ["2000", "0|0|0"], off

            print(f'{{"case": "cache-counters", "hits": {c["hits"]}, '
                  f'"misses": {c["misses"]}, "fetched": {c["fetched"]}}}',
                  flush=True)
        finally:
            sql("postgres", f"DROP DATABASE {name} WITH (FORCE)")
    finally:
        trino(f"DROP TABLE IF EXISTS {table}; DROP SCHEMA iceberg.{ns}")


if __name__ == "__main__":
    main()
