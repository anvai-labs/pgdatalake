"""Join the PostgreSQL ledger and Rust publisher providers across a crash."""

import argparse
import hashlib
import http.client
import json
import os
import subprocess
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

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


def run(db, namespace, data, worker, reply_loss, capture_adapter, admitted_transport):
    table = f"iceberg.{namespace}.events"
    trino(f"CREATE TABLE {table} (id bigint) WITH (format_version=2); "
          f"INSERT INTO {table} VALUES 10")
    route, old_reply, metadata = enrolled_metadata(namespace)
    parent = int(metadata["current-snapshot-id"])
    authority, remote, operation = str(uuid.uuid4()), metadata["table-uuid"], str(uuid.uuid4())
    snaps = f'SELECT snapshot_id FROM iceberg.{namespace}."events$snapshots"'
    initial_snapshots = set(trino(snaps).splitlines())
    assert str(parent) in initial_snapshots

    binding, incarnation = str(uuid.uuid4()), str(uuid.uuid4())
    role = f"{db}_writer"
    root = metadata["location"].rstrip("/")
    sql(db, "CREATE EXTENSION lakehouse; CREATE EXTENSION pg_walinspect;")
    assert Path(sql(db, "SHOW data_directory")).resolve() == data
    assert sql(db, "SHOW fsync") == "on"
    assert sql(db, "SHOW listen_addresses") == "", "fixture requires a socket-only cluster"
    accepted = threading.Event()
    release = threading.Event()
    stale_head = threading.Event()
    unavailable = threading.Event()
    posts, lookups, errors = [], [], []
    requests = []
    revoke_after_acceptance = admitted_transport and reply_loss == "http_504"
    next_operation = str(uuid.uuid4())

    class CatalogGateway(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            if self.path == route:
                lookups.append(stale_head.is_set())
                if unavailable.is_set():
                    self.reply(503, b"")
                    return
                if stale_head.is_set():
                    self.reply(200, old_reply)
                    return
            status, body = catalog_request("GET", self.path)
            self.reply(status, body)

        def do_POST(self):
            requests.append(self.path)
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
            except Exception as error:  # noqa: BLE001 - unblock the sender on any fixture failure
                errors.append(error)
                accepted.set()
                self.send_error(500, "fixture rejected the first POST")
                return
            accepted.set()
            if release.wait(timeout=45) and reply_loss == "http_504":
                self.reply(504, b"")
            # In the crash case the client has already been terminated.

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
            endpoint = f"http://127.0.0.1:{gateway.server_port}/catalog"
            if capture_adapter is None:
                sql(db, "CREATE TABLE events (id bigint);")
            else:
                sql(db, f"""
                    CREATE SCHEMA capture_fixture;
                    CREATE SERVER capture_catalog FOREIGN DATA WRAPPER lakehouse_fdw OPTIONS
                      (endpoint '{endpoint}', warehouse 'smoke', s3_endpoint 'http://127.0.0.1:9000');
                    CREATE ROLE {role};
                    GRANT USAGE ON FOREIGN SERVER capture_catalog TO {role};
                    CREATE USER MAPPING FOR CURRENT_USER SERVER capture_catalog OPTIONS
                      (s3_access_key 'lakehouse-test', s3_secret_key 'lakehouse-test-password');
                    CREATE USER MAPPING FOR {role} SERVER capture_catalog OPTIONS
                      (s3_access_key 'lakehouse-test', s3_secret_key 'lakehouse-test-password');
                    CREATE TABLE events (id bigint) USING lakehouse WITH (write_mode='staged');
                    GRANT INSERT ON events TO {role};
                """)
                sql(db, capture_adapter)
                sql(db, f"""
                    SELECT capture_fixture.require_binding('events'::regclass);
                    SELECT capture_fixture.enroll('events'::regclass, '{namespace}.events',
                        'capture_catalog', '{incarnation}'::uuid, '{binding}'::uuid,
                        1::bigint, '{authority}'::uuid, '{remote}'::uuid, '{root}');
                """)
                if admitted_transport:
                    prefix = route.split("/namespaces/", 1)[0].removeprefix("/catalog/v1").lstrip("/")
                    sql(db, f"""
                        ALTER EXTENSION lakehouse UPDATE TO '1.8';
                        SELECT lakehouse.grant_publication_send('{binding}', '{prefix}', 'us-east-1');
                    """)

            def capture(seed_id, value):
                if capture_adapter is None:
                    sql(db, f"""
                        SET synchronous_commit TO local;
                        INSERT INTO {SEG} (seg_id, relid, payload, row_count, authority_id, table_uuid)
                        OVERRIDING SYSTEM VALUE VALUES (
                            {seed_id}, 'events'::regclass, '[[{value}]]', 1, '{authority}', '{remote}');
                    """)
                    return seed_id
                before = len(requests)
                sql(db, f"""
                    SET synchronous_commit TO local;
                    SET ROLE {role};
                    INSERT INTO events VALUES ({value});
                """)
                assert len(requests) == before, "foreground capture made a catalog request"
                row = json.loads(sql(db, f"""
                    SELECT json_build_array(seg_id, capture_fixture.provenance(seg_id))
                    FROM {SEG}
                    WHERE relid='events'::regclass AND row_count=1
                      AND convert_from(payload, 'UTF8')::jsonb = '[[{value}]]'::jsonb
                      AND authority_id='{authority}' AND table_uuid='{remote}'
                      AND payload_digest=sha256(payload) AND NOT published;
                """))
                seg_id, provenance = row
                assert provenance == [binding, incarnation, 1], row
                return seg_id

            def managed_claim(op, seg_id):
                return f"SELECT encode(lakehouse.claim_managed_publication('{op}', ARRAY[{seg_id}]::bigint[]), 'hex')"

            def claim_member(op, seg_id):
                if admitted_transport:
                    return sql(db, managed_claim(op, seg_id))
                return sql(db, claim(op, str(seg_id), "events", authority, remote))

            first = capture(7, 20)
            input_digest = claim_member(operation, first)
            checkpoint = sql(db, "SELECT pg_current_wal_insert_lsn()")
            worker_input = {"database": db, "operation_id": operation}
            if admitted_transport:
                worker_input["admitted_transport"] = True
            else:
                worker_input.update(endpoint=endpoint, namespace=namespace, authority_id=authority,
                                    table_uuid=remote, root=root)
            request = json.dumps(worker_input)
            # A transient failure before authorization leaves the claim
            # eligible for a new owner. This first pass reaches no POST.
            unavailable.set()
            failed = subprocess.run([worker], check=False, input=request, text=True, timeout=25,
                                    capture_output=True)
            assert failed.returncode and "prepare_unsent" in failed.stderr, failed
            assert not posts
            assert sql(db, "SELECT count(*) FROM lakehouse.publication_attempts") == "0"
            unavailable.clear()

            with subprocess.Popen([worker], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  text=True) as sender:
                try:
                    sender.stdin.write(request)
                    sender.stdin.close()
                    assert accepted.wait(timeout=30), "the catalog never accepted the first POST"
                    assert not errors, errors
                    assert sender.poll() is None, "sender exited before the lost-reply barrier"
                    late = capture(8, 30)
                    assert first != late
                    if revoke_after_acceptance:
                        # The unresolved operation holds the target reservation.
                        # Its lookup will still use the retained scope after revoke.
                        sql(db, managed_claim(next_operation, late), error="40001")
                        sql(db, f"SELECT lakehouse.revoke_publication_send('{binding}')")
                    assert sql(db, f"SELECT count(*) FROM {HDR} WHERE verified_snapshot_id IS NOT NULL") == "0"
                    if reply_loss == "http_504":
                        release.set()
                        assert sender.wait(timeout=10) == 0
                        assert sender.stdout.read().strip() == "hold dispatch_unknown"
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

            def drive():
                return subprocess.check_output(
                    [worker], input=request, text=True, timeout=25).strip()

            posted_snapshot = int(sql(db, f"""
                SELECT proposed_snapshot_id FROM lakehouse.publication_attempts
                WHERE operation_id='{operation}' AND expected_parent={parent}
            """))
            # Unavailable and then stale evidence each leave the intent reserved
            # without another POST or local completion.
            unavailable.set()
            assert drive() == "hold lookup_unavailable"
            assert len(posts) == 1
            unavailable.clear()
            stale_head.set()
            assert drive() == "hold acceptance_unproved"
            assert len(posts) == 1
            assert lookups[-1] is True
            assert sql(db, f"SELECT count(*) FROM {SEG} WHERE published") == "0"
            assert sql(db, f"SELECT count(*) FROM {HDR} WHERE verified_snapshot_id IS NOT NULL") == "0"
            stale_head.clear()
            started = time.monotonic()
            assert drive() == f"verified {posted_snapshot} 1"
            recovery_ms = (time.monotonic() - started) * 1000
            assert lookups[-1] is False
            row_versions = f"""
                SELECT xmin FROM {HDR} WHERE operation_id='{operation}';
                SELECT xmin FROM {SEG} WHERE seg_id={first};
            """
            completed = sql(db, row_versions)
            before = len(requests)
            assert drive() == f"complete {posted_snapshot}"
            assert len(requests) == before
            assert sql(db, row_versions) == completed
            assert len(posts) == 1
            assert posted_snapshot != parent

            assert sql(db, f"SELECT seg_id, published, published_snapshot_id "
                       f"FROM {SEG} ORDER BY seg_id") == f"{first}|t|{posted_snapshot}\n{late}|f|"
            assert sql(db, f"SELECT array_agg(seg_id ORDER BY seg_id) FROM {MEM} "
                       f"WHERE operation_id='{operation}'") == f"{{{first}}}"
            assert sql(db, f"SELECT verified_snapshot_id FROM {HDR} WHERE operation_id='{operation}' "
                       f"AND encode(input_digest, 'hex')='{input_digest}'") == str(posted_snapshot)
            if revoke_after_acceptance:
                before = len(requests)
                sql(db, managed_claim(next_operation, late), error="55000")
                assert len(requests) == before and len(posts) == 1
                assert sql(db, f"SELECT count(*) FROM {HDR} "
                               f"WHERE operation_id='{next_operation}'") == "0"
            else:
                assert claim_member(next_operation, late), "late row was not claimable"
            assert set(trino(snaps).splitlines()) == initial_snapshots | {str(posted_snapshot)}
            assert trino(f"SELECT id FROM {table} FOR VERSION AS OF {parent} ORDER BY id") == "10"
            assert trino(f"SELECT id FROM {table} FOR VERSION AS OF {posted_snapshot} ORDER BY id") == "10\n20"
            if admitted_transport:
                assert set(requests) == {route}, requests
            capture_source = "seeded" if capture_adapter is None else "ordinary_sql"
            transport = "admitted" if admitted_transport else "legacy_fixture"
            print(f"bound worker: capture={capture_source} transport={transport} reply_loss={reply_loss} "
                  f"base={parent} rows=[10], "
                  f"receipt={posted_snapshot} rows=[10,20], "
                  f"catalog_posts={len(posts)} catalog_gets={len(requests)-len(posts)} "
                  f"wal_intent_commit_flushed=true completed_members=[{first}] late_members_pending=[{late}] "
                  f"send_grant_revoked={str(revoke_after_acceptance).lower()} "
                  f"restart_ms={restart_ms:.1f} verify_and_finalize_ms={recovery_ms:.1f} samples=1")
        finally:
            release.set()
            gateway.shutdown()
            server.join(timeout=5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--restart-fixture", required=True, type=Path, metavar="PGDATA")
    parser.add_argument("--worker", required=True, type=Path)
    parser.add_argument("--capture-adapter", type=Path,
                        help="use ordinary TableAM INSERT with the managed capture fixture adapter")
    parser.add_argument("--admitted-transport", action="store_true",
                        help="use the durable 1.8 claim and enrolled publisher transport")
    args = parser.parse_args()
    if args.admitted_transport and args.capture_adapter is None:
        parser.error("--admitted-transport requires --capture-adapter")
    if not __debug__:
        raise RuntimeError("the recovery smoke requires Python assertions")
    admin = os.getenv("PGDATABASE", "postgres")
    data = args.restart_fixture.resolve()
    worker = args.worker.resolve()
    capture_adapter = args.capture_adapter.read_text(encoding="utf-8") if args.capture_adapter else None
    for reply_loss in ("crash", "http_504"):
        name = "bound_publish_" + uuid.uuid4().hex[:12]
        sql(admin, f"CREATE DATABASE {name}")
        try:
            trino(f"CREATE SCHEMA iceberg.{name}")
            try:
                run(name, name, data, worker, reply_loss, capture_adapter, args.admitted_transport)
            finally:
                trino(f"DROP TABLE IF EXISTS iceberg.{name}.events; DROP SCHEMA iceberg.{name}")
        finally:
            sql(admin, f"DROP DATABASE {name} WITH (FORCE); DROP ROLE IF EXISTS {name}_writer;")


if __name__ == "__main__":
    main()
