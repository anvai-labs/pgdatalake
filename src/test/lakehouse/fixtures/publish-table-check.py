"""Flush band: lakehouse.publish_table — typed foreground receipt.

A staged-append managed table with pending segments flushes
synchronously through publish_table: state 'published', the verified
snapshot id, exact segment count; an empty table returns state
'no change' with a NULL snapshot id and costs zero origin requests
beyond the flush itself. Trino reads the published rows afterwards.
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
        raise RuntimeError("the flush fixture requires Python assertions")
    ns = "pubtbl_" + uuid4().hex[:12]
    name = "pubtbl_" + uuid4().hex[:8]
    table = f"iceberg.{ns}.events"
    trino(f"CREATE SCHEMA iceberg.{ns}")
    trino(f"CREATE TABLE {table} (id bigint) WITH (format_version = 2)")
    try:
        sql("postgres", f"CREATE DATABASE {name} TEMPLATE template0 "
                        f"ENCODING 'UTF8' LOCALE 'C'")
        try:
            sql(name, f"""
                CREATE EXTENSION lakehouse;
                ALTER EXTENSION lakehouse UPDATE TO '1.21';
                CREATE FOREIGN DATA WRAPPER lh HANDLER lakehouse.lakehouse_fdw_handler;
                CREATE SERVER s FOREIGN DATA WRAPPER lh OPTIONS
                    (endpoint '{CATALOG}', warehouse 'smoke', s3_endpoint '{S3}');
                CREATE USER MAPPING FOR CURRENT_USER SERVER s OPTIONS
                    (s3_access_key 'lakehouse-test', s3_secret_key 'lakehouse-test-password');
                CREATE TABLE events (id bigint) USING lakehouse
                    WITH (write_mode='staged');
                SELECT lakehouse.attach_table('events', '{ns}.events', 's');
            """)

            # no change: flushing an empty table costs nothing and
            # reports state 'no change' with a NULL snapshot id
            r = sql(name, "SELECT published_segments, snapshot_id, state "
                          "FROM lakehouse.publish_table('events')")
            assert r == "0||no change", r
            head0 = trino(f'SELECT snapshot_id FROM iceberg.{ns}.'
                          f'"events$history" ORDER BY made_current_at DESC LIMIT 1').strip()

            # staged INSERT + flush: state 'published', verified
            # snapshot id, exact segment count; trino sees the row
            sql(name, "INSERT INTO events VALUES (10)")
            r = sql(name, "SELECT published_segments, snapshot_id, state "
                          "FROM lakehouse.publish_table('events')")
            segs, snap, state = [x.strip() for x in r.split("|")]
            assert segs == "1" and state == "published" and snap != "", r
            head1 = trino(f'SELECT snapshot_id FROM iceberg.{ns}.'
                          f'"events$history" ORDER BY made_current_at DESC LIMIT 1').strip()
            assert head1 != head0, (head0, head1)
            assert int(snap) == int(head1), (snap, head1)
            rows = trino(f"SELECT id FROM {table} ORDER BY id").strip()
            assert rows == "10", rows

            # idempotent drain: a second flush is 'no change'
            r = sql(name, "SELECT published_segments, snapshot_id, state "
                          "FROM lakehouse.publish_table('events')")
            assert r == "0||no change", r
            print(json.dumps({"case": "publish-table-flush",
                              "segments": 1, "state": "published",
                              "snapshot_id": snap}), flush=True)
        finally:
            sql("postgres", f"DROP DATABASE {name} WITH (FORCE)")
    finally:
        trino(f"DROP TABLE IF EXISTS {table}; DROP SCHEMA iceberg.{ns}")


if __name__ == "__main__":
    main()
