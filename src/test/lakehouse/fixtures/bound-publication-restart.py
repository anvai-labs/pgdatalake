"""Join the PostgreSQL ledger and Rust publisher providers across a crash."""

import argparse
from concurrent.futures import ThreadPoolExecutor
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


def approved_target(namespace, endpoint, prefix, authority, table, root, binding, incarnation):
    # Independent fixture enrollment. The worker compares it with the SQL
    # descriptor. A catalog response cannot broaden this record.
    return {
        "authority_id": authority, "binding_id": binding, "incarnation_id": incarnation,
        "binding_generation": 1, "table_uuid": table, "storage_root": root,
        "schema_id": 0, "spec_id": 0, "ref_name": "main", "catalog_endpoint": endpoint,
        "catalog_prefix": prefix, "namespace_components": [namespace], "table_name": "events",
        "object_endpoint": "http://127.0.0.1:9000", "s3_region": "us-east-1",
    }


def disconnect_after_authorization(db):
    # The nested CALL commits the real intent. The outer CALL never gets to
    # send CommandComplete to libpq. Only this disposable database is changed.
    sql(db, """
        ALTER PROCEDURE lakehouse.authorize_publication_send(uuid,bigint,bytea,bigint,bigint,bytea,bytea)
            RENAME TO fixture_authorize;
        CREATE PROCEDURE lakehouse.authorize_publication_send(
            op uuid, generation bigint, input_hash bytea, parent bigint, proposed bigint,
            proposal_hash bytea, body bytea, OUT intent_xid xid)
        LANGUAGE plpgsql AS $$
        BEGIN
            CALL lakehouse.fixture_authorize(op, generation, input_hash, parent, proposed,
                                            proposal_hash, body, intent_xid);
            PERFORM pg_terminate_backend(pg_backend_pid());
            RAISE EXCEPTION 'fixture expected backend termination';
        END $$;
        REVOKE ALL ON PROCEDURE lakehouse.authorize_publication_send(uuid,bigint,bytea,bigint,bigint,bytea,bytea)
            FROM PUBLIC;
    """)


def retained_intent(db, operation, first, start):
    # Independently prove the source and intent commit records are within the
    # flush horizon. Preserve the exact attempt across restart and takeover.
    record = json.loads(sql(db, f"""
        WITH wal AS (
            SELECT xid, end_lsn FROM pg_get_wal_records_info('{start}', pg_current_wal_flush_lsn())
            WHERE resource_manager = 'Transaction' AND record_type = 'COMMIT'
        )
        SELECT json_build_object('source_xid', s.xmin::text, 'intent_xid', a.xmin::text,
            'source_commit_lsn', (SELECT end_lsn FROM wal WHERE xid = s.xmin),
            'intent_commit_lsn', (SELECT end_lsn FROM wal WHERE xid = a.xmin),
            'owner_generation', a.owner_generation, 'binding', a.capture_binding_id,
            'expected_parent', a.expected_parent, 'proposed', a.proposed_snapshot_id,
            'body', encode(a.proposal_body, 'hex'), 'digest', encode(a.proposal_digest, 'hex'))
        FROM lakehouse.publication_attempts a JOIN {SEG} s ON s.seg_id = {first}
        WHERE a.operation_id = '{operation}'
    """))
    assert record["source_commit_lsn"] and record["intent_commit_lsn"]
    assert record["source_xid"] != record["intent_xid"]
    body = bytes.fromhex(record["body"])
    assert body and hashlib.sha256(body).hexdigest() == record["digest"]
    return record


NATIVE_LOOKUP = "lakehouse.reconcile_managed_publication_lookup"


def install_native_lookup_role(db, role):
    # A dedicated operator-owned credential mapping. The role has no server
    # USAGE or ownership. It is trusted internal code, never a tenant role.
    sql(db, f"""
        ALTER EXTENSION lakehouse UPDATE TO '1.12';
        CREATE ROLE {role} LOGIN;
        GRANT USAGE ON SCHEMA lakehouse TO {role};
        GRANT EXECUTE ON FUNCTION lakehouse.load_managed_publication_job(uuid),
            lakehouse.get_publication_lookup_binding(uuid),
            lakehouse.advance_publication_generation(uuid, bigint),
            lakehouse.finalize_publication(uuid, bigint, uuid, uuid, bytea, bigint, bytea) TO {role};
        GRANT SELECT ON lakehouse.publication_binding_descriptors,
            lakehouse.managed_capture_bindings, lakehouse.publication_attempts,
            lakehouse.publication_lookup_grants, {HDR}, {MEM}, {SEG} TO {role};
        GRANT UPDATE (owner_generation, verified_snapshot_id) ON {HDR} TO {role};
        GRANT UPDATE (published, published_snapshot_id) ON {SEG} TO {role};
        CREATE SERVER lookup_credentials FOREIGN DATA WRAPPER lakehouse_fdw
            OPTIONS (endpoint 'http://127.0.0.1:1/catalog', warehouse 'smoke');
        CREATE USER MAPPING FOR {role} SERVER lookup_credentials
            OPTIONS (token 'fixture-lookup', s3_access_key 'lakehouse-reader',
                     s3_secret_key 'lakehouse-reader-password');
    """)


def approve_native_lookup(db, role, binding, *, expired=False):
    expiry = "- interval '1 second'" if expired else "+ interval '90 seconds'"
    sql(db, f"""
        INSERT INTO lakehouse.publication_lookup_grants
        SELECT '{binding}', umuser, umid, clock_timestamp() {expiry}
        FROM pg_user_mappings WHERE usename='{role}' AND srvname='lookup_credentials';
    """)


def run(db, namespace, data, worker, reply_loss, capture_adapter, admitted_transport, native_lookup):

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
    lookup_role = f"{db}_lookup"
    root = metadata["location"].rstrip("/")
    sql(db, "CREATE EXTENSION lakehouse; CREATE EXTENSION pg_walinspect;")
    assert Path(sql(db, "SHOW data_directory")).resolve() == data
    assert sql(db, "SHOW fsync") == "on"
    assert sql(db, "SHOW listen_addresses") == "", "fixture requires a socket-only cluster"
    accepted = threading.Event()
    release = threading.Event()
    stale_head = threading.Event()
    unavailable = threading.Event()
    pause_lookup = threading.Event()
    proof_requested = threading.Event()
    release_proof = threading.Event()
    posts, lookups, errors = [], [], []
    requests, catalog_auth = [], []
    revoke_after_acceptance = admitted_transport and reply_loss == "http_504"
    next_operation = str(uuid.uuid4())

    class CatalogGateway(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            if admitted_transport:
                bearer = self.headers.get("Authorization")
                catalog_auth.append(("GET", bearer))
                if bearer not in ("Bearer fixture-send", "Bearer fixture-lookup"):
                    self.send_error(403, "fixture requires purpose-specific catalog credentials")
                    return
            if self.path == route:
                lookups.append(stale_head.is_set())
                if unavailable.is_set():
                    self.reply(503, b"")
                    return
                if stale_head.is_set():
                    self.reply(200, old_reply)
                    return
                if pause_lookup.is_set():
                    proof_requested.set()
                    if not release_proof.wait(timeout=10):
                        self.reply(504, b"")
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
            if admitted_transport:
                bearer = self.headers.get("Authorization")
                catalog_auth.append(("POST", bearer))
                if bearer != "Bearer fixture-send":
                    self.send_error(403, "fixture lookup credentials cannot commit")
                    return
            if reply_loss == "authorization_ack":
                self.send_error(409, "authorization reply was withheld; no POST is permitted")
                return
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
                        ALTER EXTENSION lakehouse UPDATE TO '1.10';
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

            checkpoint = sql(db, "SELECT pg_current_wal_insert_lsn()")
            first = capture(7, 20)
            input_digest = claim_member(operation, first)
            worker_input = {"database": db, "operation_id": operation}
            if admitted_transport:
                worker_input.update(admitted_transport=True, approved_target=approved_target(
                    namespace, endpoint, prefix, authority, remote, root, binding, incarnation))
            else:
                worker_input.update(endpoint=endpoint, namespace=namespace, authority_id=authority,
                                    table_uuid=remote, root=root)
            request = json.dumps(worker_input)

            def wake(mode):
                if not admitted_transport:
                    return request
                hint = json.loads(sql(db, """
                    SELECT row_to_json(c) FROM lakehouse.discover_managed_publication_claims(NULL, 16) c
                """))
                assert (hint["operation_id"], hint["binding_id"], hint["mode"]) == (operation, binding, mode), hint
                return json.dumps(worker_input | {"dispatch_hint": hint})

            # A transient failure before authorization leaves the claim
            # eligible for a new owner. This first pass reaches no POST.
            unavailable.set()
            lookup_request = json.dumps(worker_input | {"lookup_only": True})
            if admitted_transport:
                before = len(requests)
                probe = subprocess.run([worker], input=lookup_request, capture_output=True, text=True, timeout=25)
                assert len(requests) == before, "lookup-only wake contacted the catalog for an unsent claim"
                assert probe.returncode == 0 and probe.stdout.strip() == "no_retained_intent", probe
                assert sql(db, f"SELECT owner_generation FROM {HDR} WHERE operation_id='{operation}'") == "0"
            failed = subprocess.run([worker], check=False, input=wake("send"), text=True, timeout=25,
                                    capture_output=True)
            assert failed.returncode and "prepare_unsent" in failed.stderr, failed
            assert not posts
            assert sql(db, "SELECT count(*) FROM lakehouse.publication_attempts") == "0"
            unavailable.clear()

            lost_authorization = reply_loss == "authorization_ack"
            if lost_authorization:
                disconnect_after_authorization(db)
                failed = subprocess.run([worker], input=wake("send"), text=True, capture_output=True, timeout=30)
                assert not posts, f"expected zero commit POSTs after lost authorization reply, saw {len(posts)}"
                assert (failed.returncode and "authorization_unconfirmed" in failed.stderr
                        and "terminating connection due to administrator command" in failed.stderr), failed
                retained = retained_intent(db, operation, first, checkpoint)
                assert retained["binding"] == binding and retained["expected_parent"] == parent
                late = capture(8, 30)
            else:
                with subprocess.Popen([worker], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                      text=True) as sender:
                    try:
                        sender.stdin.write(wake("send"))
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

            function = NATIVE_LOOKUP
            if native_lookup:
                install_native_lookup_role(db, lookup_role)
                sql(db, f"GRANT EXECUTE ON FUNCTION {function}(uuid, bigint) TO {lookup_role};")
                approve_native_lookup(db, lookup_role, binding)

            def native_call(generation, *, error=None):
                return sql(db, f"SET ROLE {lookup_role}; SELECT {function}('{operation}', {generation});",
                           error=error)

            def recover(*, completed=False):
                if native_lookup and not completed:
                    hint = json.loads(wake("lookup"))["dispatch_hint"]
                    result = json.loads(native_call(hint["owner_generation"]))
                    if result["status"] == "verified":
                        return f"verified {result['snapshot_id']} {result['members']}"
                    states = {"unavailable": "lookup_unavailable", "unresolved": "acceptance_unproved"}
                    return f"hold {states[result['status']]}"
                wake_request = lookup_request if completed and admitted_transport else wake("lookup")
                return subprocess.check_output([worker], input=wake_request, text=True, timeout=25).strip()

            proposed_snapshot = int(sql(db, f"""
                SELECT proposed_snapshot_id FROM lakehouse.publication_attempts
                WHERE operation_id='{operation}' AND expected_parent={parent}
            """))
            # Unavailable and then stale evidence each leave the intent reserved
            # without another POST or local completion.
            expected_posts = 0 if lost_authorization else 1
            restart_requests = len(catalog_auth)
            unavailable.set()
            assert recover() == "hold lookup_unavailable"
            assert len(posts) == expected_posts
            unavailable.clear()
            stale_head.set()
            assert recover() == "hold acceptance_unproved"
            assert len(posts) == expected_posts
            assert lookups[-1] is True
            assert sql(db, f"SELECT count(*) FROM {SEG} WHERE published") == "0"
            assert sql(db, f"SELECT count(*) FROM {HDR} WHERE verified_snapshot_id IS NOT NULL") == "0"
            stale_head.clear()
            if lost_authorization:
                before = len(lookups)
                assert recover() == "hold acceptance_unproved"
                assert len(lookups) > before and not any(lookups[before:])
                assert retained_intent(db, operation, first, checkpoint) == retained
                assert proposed_snapshot == retained["proposed"] and proposed_snapshot != parent
                generation = int(sql(db, f"SELECT owner_generation FROM {HDR} WHERE operation_id='{operation}' "
                                         f"AND verified_snapshot_id IS NULL AND encode(input_digest,'hex')='{input_digest}'"))
                assert generation > retained["owner_generation"]
                assert sql(db, f"SELECT seg_id FROM {MEM} WHERE operation_id='{operation}'") == str(first)
                assert sql(db, f"SELECT seg_id, published, published_snapshot_id FROM {SEG} ORDER BY seg_id") == (
                    f"{first}|f|\n{late}|f|")
                sql(db, managed_claim(next_operation, late), error="40001")
                assert not posts and set(requests) == {route}
                assert catalog_auth[restart_requests:] == [("GET", "Bearer fixture-lookup")] * 3
                assert set(trino(snaps).splitlines()) == initial_snapshots
                _, _, head = enrolled_metadata(namespace)
                assert head["current-snapshot-id"] == parent
                rows = trino(f"SELECT id FROM {table} FOR VERSION AS OF {parent} ORDER BY id")
                assert rows == "10"
                print(json.dumps({
                    "case": "authorization_ack_lost", "lookup_driver": "native" if native_lookup else "rust-fixture",
                    "table_uuid": remote, "operation_id": operation,
                    "parent": str(parent), "proposed": str(proposed_snapshot), "trino_rows": [int(rows)],
                    "reserved_member": first, "later_pending": late, "catalog_posts": len(posts),
                    "catalog_gets": len(lookups), "lookup_outcome": "acceptance_unproved",
                    "recovery_gets_with_lookup_grant": len(catalog_auth) - restart_requests,
                    "source_commit_lsn": retained["source_commit_lsn"], "intent_commit_lsn": retained["intent_commit_lsn"],
                    "body_sha256": retained["digest"], "intent_generation": retained["owner_generation"],
                    "recovery_generation": generation, "samples": 1, "restart_ms": round(restart_ms, 1),
                }), flush=True)
                return
            raced = native_lookup and reply_loss == "crash"
            if raced:
                # Retire the native worker's ownership while it is outside
                # PostgreSQL, waiting for otherwise-valid provider evidence.
                generation = json.loads(wake("lookup"))["dispatch_hint"]["owner_generation"]
                pause_lookup.set()
                with ThreadPoolExecutor(max_workers=1) as executor:
                    result = executor.submit(native_call, generation, error="40001")
                    try:
                        assert proof_requested.wait(timeout=10), "native lookup did not reach the provider"
                        assert sql(db, f"SELECT lakehouse.advance_publication_generation("
                                       f"'{operation}', {generation + 1})") == str(generation + 2)
                    finally:
                        pause_lookup.clear()
                        release_proof.set()
                    result.result(timeout=15)
                assert sql(db, f"SELECT count(*) FROM {SEG} WHERE published") == "0"
                assert len(posts) == 1
            started = time.monotonic()
            assert recover() == f"verified {proposed_snapshot} 1"
            recovery_ms = (time.monotonic() - started) * 1000
            assert lookups[-1] is False
            row_versions = f"""
                SELECT xmin FROM {HDR} WHERE operation_id='{operation}';
                SELECT xmin FROM {SEG} WHERE seg_id={first};
            """
            completed = sql(db, row_versions)
            before = len(requests)
            if native_lookup:
                generation = int(sql(db, f"SELECT owner_generation FROM {HDR} WHERE operation_id='{operation}'"))
                native_call(generation, error="55000")
            assert recover(completed=True) == f"complete {proposed_snapshot}"
            assert len(requests) == before
            assert sql(db, row_versions) == completed
            assert len(posts) == 1
            assert proposed_snapshot != parent

            assert sql(db, f"SELECT seg_id, published, published_snapshot_id "
                       f"FROM {SEG} ORDER BY seg_id") == f"{first}|t|{proposed_snapshot}\n{late}|f|"
            assert sql(db, f"SELECT array_agg(seg_id ORDER BY seg_id) FROM {MEM} "
                       f"WHERE operation_id='{operation}'") == f"{{{first}}}"
            assert sql(db, f"SELECT verified_snapshot_id FROM {HDR} WHERE operation_id='{operation}' "
                       f"AND encode(input_digest, 'hex')='{input_digest}'") == str(proposed_snapshot)
            if revoke_after_acceptance:
                before = len(requests)
                sql(db, managed_claim(next_operation, late), error="55000")
                assert len(requests) == before and len(posts) == 1
                assert sql(db, f"SELECT count(*) FROM {HDR} "
                               f"WHERE operation_id='{next_operation}'") == "0"
            else:
                assert claim_member(next_operation, late), "late row was not claimable"
            assert set(trino(snaps).splitlines()) == initial_snapshots | {str(proposed_snapshot)}
            assert trino(f"SELECT id FROM {table} FOR VERSION AS OF {parent} ORDER BY id") == "10"
            assert trino(f"SELECT id FROM {table} FOR VERSION AS OF {proposed_snapshot} ORDER BY id") == "10\n20"
            if admitted_transport:
                assert set(requests) == {route}, requests
                assert catalog_auth[restart_requests:] == [("GET", "Bearer fixture-lookup")] * (3 + raced)
            capture_source = "seeded" if capture_adapter is None else "ordinary_sql"
            transport = "admitted" if admitted_transport else "legacy_fixture"
            driver = "native" if native_lookup else "rust-fixture"
            print(f"bound worker: capture={capture_source} transport={transport} reply_loss={reply_loss} "
                  f"lookup_driver={driver} fenced_racing_finalize={str(raced).lower()} "
                  f"base={parent} rows=[10], "
                  f"receipt={proposed_snapshot} rows=[10,20], "
                  f"catalog_posts={len(posts)} catalog_gets={len(requests)-len(posts)} "
                  f"wal_intent_commit_flushed=true completed_members=[{first}] late_members_pending=[{late}] "
                  f"send_grant_revoked={str(revoke_after_acceptance).lower()} "
                  f"restart_ms={restart_ms:.1f} verify_and_finalize_ms={recovery_ms:.1f} samples=1")
        finally:
            release.set()
            release_proof.set()
            gateway.shutdown()
            server.join(timeout=5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--restart-fixture", required=True, type=Path, metavar="PGDATA")
    parser.add_argument("--worker", required=True, type=Path)
    parser.add_argument("--capture-adapter", type=Path,
                        help="use ordinary TableAM INSERT with the managed capture fixture adapter")
    parser.add_argument("--admitted-transport", action="store_true",
                        help="use the 1.10 managed ledger adapter and enrolled publisher transport")
    parser.add_argument("--native-lookup", action="store_true",
                        help="use opt-in native 1.12 for recovery after the fixture send")
    args = parser.parse_args()
    if args.native_lookup and not args.admitted_transport:
        parser.error("--native-lookup requires --admitted-transport")
    if args.admitted_transport and args.capture_adapter is None:
        parser.error("--admitted-transport requires --capture-adapter")
    if not __debug__:
        raise RuntimeError("the recovery smoke requires Python assertions")
    admin = os.getenv("PGDATABASE", "postgres")
    data = args.restart_fixture.resolve()
    worker = args.worker.resolve()
    capture_adapter = args.capture_adapter.read_text(encoding="utf-8") if args.capture_adapter else None
    losses = ["crash", "http_504"]
    if args.admitted_transport:
        losses.append("authorization_ack")
    for reply_loss in losses:
        name = "bound_publish_" + uuid.uuid4().hex[:12]
        sql(admin, f"CREATE DATABASE {name}")
        try:
            trino(f"CREATE SCHEMA iceberg.{name}")
            try:
                run(name, name, data, worker, reply_loss, capture_adapter,
                    args.admitted_transport, args.native_lookup)
            finally:
                trino(f"DROP TABLE IF EXISTS iceberg.{name}.events; DROP SCHEMA iceberg.{name}")
        finally:
            sql(admin, f"DROP DATABASE {name} WITH (FORCE); DROP ROLE IF EXISTS {name}_writer, {name}_lookup;")


if __name__ == "__main__":
    main()
