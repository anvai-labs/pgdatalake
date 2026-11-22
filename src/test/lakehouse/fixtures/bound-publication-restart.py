"""Join the PostgreSQL ledger and Rust publisher providers across a crash."""

import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import subprocess
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from publication_fixture import HDR, MEM, SEG, claim, sql


def trino(statement):
    return subprocess.check_output(
        ["docker", "exec", os.getenv("TRINO_CONTAINER", "pgdatalake-trino-1"),
         "trino", "--output-format", "TSV", "--execute", statement],
        text=True, timeout=90).strip()


def catalog_request(method, path, body=b"", headers=None):
    upstream = http.client.HTTPConnection("127.0.0.1", 8181, timeout=15)
    try:
        upstream.request(method, path, body, headers or {})
        reply = upstream.getresponse()
        return reply.status, reply.read()
    finally:
        upstream.close()


def enrolled_metadata(namespace):
    status, body = catalog_request("GET", "/catalog/v1/config?warehouse=smoke")
    assert status == 200, (status, body)
    config = json.loads(body)
    prefix = (config["defaults"] | config["overrides"])["prefix"]
    route = f"/catalog/v1/{prefix}/namespaces/{namespace}/tables/events"
    status, body = catalog_request("GET", route)
    assert status == 200, (status, body)
    return route, body, json.loads(body)["metadata"]


def run(db, namespace, data, worker):
    table = f"iceberg.{namespace}.events"
    trino(f"CREATE TABLE {table} (id bigint) WITH (format_version=2); "
          f"INSERT INTO {table} VALUES 10")
    route, old_reply, metadata = enrolled_metadata(namespace)
    parent = int(metadata["current-snapshot-id"])
    authority, remote, operation = str(uuid.uuid4()), metadata["table-uuid"], str(uuid.uuid4())
    snaps = f'SELECT snapshot_id FROM iceberg.{namespace}."events$snapshots"'
    initial_snapshots = set(trino(snaps).splitlines())
    assert str(parent) in initial_snapshots

    def capture(seg, value):
        sql(db, f"""
            SET synchronous_commit TO local;
            INSERT INTO {SEG} (seg_id, relid, payload, row_count, authority_id, table_uuid)
            OVERRIDING SYSTEM VALUE VALUES ({seg}, 'events'::regclass, '[[{value}]]', 1,
                                              '{authority}', '{remote}');
        """)

    sql(db, "CREATE EXTENSION lakehouse; CREATE EXTENSION pg_walinspect; "
            "CREATE TABLE events (id bigint);")
    assert Path(sql(db, "SHOW data_directory")).resolve() == data
    assert sql(db, "SHOW fsync") == "on"
    assert sql(db, "SHOW listen_addresses") == "", "fixture requires a socket-only cluster"
    capture(7, 20)
    input_digest = sql(db, claim(operation, "7", "events", authority, remote))
    checkpoint = sql(db, "SELECT pg_current_wal_insert_lsn()")
    accepted = threading.Event()
    release = threading.Event()
    stale_head = threading.Event()
    posts, lookups, errors = [], [], []

    class CatalogGateway(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == route:
                lookups.append(stale_head.is_set())
                if stale_head.is_set():
                    self.reply(200, old_reply)
                    return
            status, body = catalog_request("GET", self.path)
            self.reply(status, body)

        def do_POST(self):
            if self.path != route:
                self.send_error(404)
                return
            payload = self.rfile.read(int(self.headers["Content-Length"]))
            posts.append(payload)
            if len(posts) != 1:
                self.send_error(409, "fixture saw a second POST")
                return
            try:
                proof = sql(db, f"""
                    SELECT encode(a.proposal_digest, 'hex'), encode(a.proposal_body, 'hex'),
                           EXISTS (
                              SELECT FROM pg_get_wal_records_info(
                                  '{checkpoint}', pg_current_wal_flush_lsn())
                              WHERE resource_manager='Transaction' AND record_type='COMMIT'
                                AND xid=a.xmin AND end_lsn <= pg_current_wal_flush_lsn())
                    FROM lakehouse.publication_attempts AS a WHERE a.operation_id='{operation}'
                """)
                digest, body, durable = proof.split("|")
                assert (digest, bytes.fromhex(body), durable) == (
                    hashlib.sha256(payload).hexdigest(), payload, "t")
                status, body = catalog_request(
                    "POST", self.path, payload, {"Content-Type": "application/json"})
                assert 200 <= status < 300, (status, body)
            except Exception as error:
                errors.append(error)
                accepted.set()
                self.send_error(500, "fixture rejected the first POST")
                return
            accepted.set()
            # Close without writing a reply. The client is terminated first.
            release.wait(timeout=45)

        def reply(self, status, body):
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), CatalogGateway) as gateway:
        server = threading.Thread(target=gateway.serve_forever)
        server.start()
        try:
            request = json.dumps({
                "database": db, "endpoint": f"http://127.0.0.1:{gateway.server_port}/catalog",
                "namespace": namespace, "operation_id": operation, "authority_id": authority,
                "table_uuid": remote, "root": metadata["location"].rstrip("/"),
            })
            with subprocess.Popen([worker, "send"], stdin=subprocess.PIPE, text=True) as sender:
                try:
                    sender.stdin.write(request)
                    sender.stdin.close()
                    assert accepted.wait(timeout=30), "the catalog never accepted the first POST"
                    assert not errors, errors
                    assert sender.poll() is None, "sender exited before the lost-reply barrier"
                    capture(8, 30)
                    assert sql(db, f"SELECT count(*) FROM {HDR} WHERE verified_snapshot_id IS NOT NULL") == "0"
                finally:
                    if sender.poll() is None:
                        sender.kill()
                    sender.wait(timeout=10)
                    release.set()

            started = time.monotonic()
            subprocess.run([os.getenv("PG_CTL", "pg_ctl"), "restart", "-D", str(data),
                            "-m", "immediate", "-w", "-l", str(data.parent / "server.log")],
                           check=True, timeout=60)
            restart_ms = (time.monotonic() - started) * 1000

            def recover():
                return subprocess.check_output(
                    [worker, "recover"], input=request, text=True, timeout=25).strip()

            posted_snapshot = int(sql(db, f"""
                SELECT proposed_snapshot_id FROM lakehouse.publication_attempts
                WHERE operation_id='{operation}' AND expected_parent={parent}
            """))
            # First take-over sees stale proof. It must hold the original
            # intent and issue no second POST or local completion.
            stale_head.set()
            assert recover() == "hold"
            assert len(posts) == 1
            assert lookups[-1] is True
            assert sql(db, f"SELECT count(*) FROM {SEG} WHERE published") == "0"
            assert sql(db, f"SELECT count(*) FROM {HDR} WHERE verified_snapshot_id IS NOT NULL") == "0"
            stale_head.clear()
            started = time.monotonic()
            assert recover() == f"verified {posted_snapshot} 1"
            recovery_ms = (time.monotonic() - started) * 1000
            assert lookups[-1] is False
            row_versions = f"""
                SELECT xmin FROM {HDR} WHERE operation_id='{operation}';
                SELECT xmin FROM {SEG} WHERE seg_id=7;
            """
            completed = sql(db, row_versions)
            before = (len(posts), len(lookups))
            assert recover() == f"complete {posted_snapshot}"
            assert (len(posts), len(lookups)) == before
            assert sql(db, row_versions) == completed
            assert len(posts) == 1
            assert posted_snapshot != parent

            assert sql(db, f"SELECT seg_id, published, published_snapshot_id "
                       f"FROM {SEG} ORDER BY seg_id") == f"7|t|{posted_snapshot}\n8|f|"
            assert sql(db, f"SELECT array_agg(seg_id ORDER BY seg_id) FROM {MEM} "
                       f"WHERE operation_id='{operation}'") == "{7}"
            assert sql(db, f"SELECT verified_snapshot_id FROM {HDR} WHERE operation_id='{operation}' "
                       f"AND encode(input_digest, 'hex')='{input_digest}'") == str(posted_snapshot)
            next_operation = str(uuid.uuid4())
            assert sql(db, claim(next_operation, "8", "events", authority, remote)), "late row was not claimable"
            assert set(trino(snaps).splitlines()) == initial_snapshots | {str(posted_snapshot)}
            assert trino(f"SELECT id FROM {table} FOR VERSION AS OF {parent} ORDER BY id") == "10"
            assert trino(f"SELECT id FROM {table} FOR VERSION AS OF {posted_snapshot} ORDER BY id") == "10\n20"
            print(f"bound providers: base={parent} rows=[10], receipt={posted_snapshot} rows=[10,20], "
                  f"catalog_posts=1 wal_commits=1 completed_members=[7] late_members_pending=[8] "
                  f"restart_ms={restart_ms:.1f} verify_and_finalize_ms={recovery_ms:.1f} samples=1")
        finally:
            release.set()
            gateway.shutdown()
            server.join(timeout=5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--restart-fixture", required=True, type=Path, metavar="PGDATA")
    parser.add_argument("--worker", required=True, type=Path)
    args = parser.parse_args()
    if not __debug__:
        raise RuntimeError("the recovery smoke requires Python assertions")
    admin = os.getenv("PGDATABASE", "postgres")
    name = "bound_publish_" + uuid.uuid4().hex[:12]
    sql(admin, f"CREATE DATABASE {name}")
    try:
        trino(f"CREATE SCHEMA iceberg.{name}")
        try:
            run(name, name, args.restart_fixture.resolve(), args.worker.resolve())
        finally:
            trino(f"DROP TABLE IF EXISTS iceberg.{name}.events; DROP SCHEMA iceberg.{name}")
    finally:
        sql(admin, f"DROP DATABASE {name} WITH (FORCE)")


if __name__ == "__main__":
    main()
