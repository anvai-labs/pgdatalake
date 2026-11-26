"""Block-cache counters: the spec 15 §6.3a measurement gate.

lakehouse.cache_stats() exposes hits/misses/fetched_bytes/used_bytes/
entries from the per-backend immutable-path block cache (§6.2 keying:
entries valid forever, so a fully-warm re-scan must add ZERO fetched
bytes — the WORM immutability proof). The R6 result-cache layer (§6.4)
is gated on these measurements.
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


def warm_rescan_proof(name, query):
    """Two scans + three counter reads in ONE psql invocation: the block
    cache is per-backend, so separate sql() calls (fresh backend each)
    would all read zeros. Returns (before, after_scan1, after_scan2)."""
    reads = ("SELECT hits, misses, fetched_bytes, used_bytes, entries "
             "FROM lakehouse.cache_stats()")
    out = sql(name, f"{reads}; {query}; {reads}; {query}; {reads}")
    lines = [ln for ln in out.split("\n") if "|" in ln]
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

            # one backend: cold scan fills the cache (misses grow,
            # fetched_bytes accumulates), the warm re-scan adds ZERO
            # fetched bytes and serves from hits (§6.2 no-invalidation
            # law: immutable entries stay valid — the WORM proof)
            a, b, c = warm_rescan_proof(name, "SELECT sum(id) FROM events")
            assert b["fetched"] > a["fetched"], (a, b)
            assert c["fetched"] == b["fetched"], (b, c)
            assert c["hits"] > b["hits"], (b, c)

            print(f'{{"case": "cache-counters", "hits": {c["hits"]}, '
                  f'"misses": {c["misses"]}, "fetched": {c["fetched"]}}}',
                  flush=True)
        finally:
            sql("postgres", f"DROP DATABASE {name} WITH (FORCE)")
    finally:
        trino(f"DROP TABLE IF EXISTS {table}; DROP SCHEMA iceberg.{ns}")


if __name__ == "__main__":
    main()
