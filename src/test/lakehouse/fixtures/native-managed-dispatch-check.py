"""Prove one native publication send across PostgreSQL and catalog reply loss."""

import argparse
import importlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import re
import subprocess
from threading import Event, Thread
import time

from publication_fixture import HDR, MEM, SEG, sql

admission = importlib.import_module("publication-admission-check")
hold = importlib.import_module("native-managed-send-hold-check")
prepare = importlib.import_module("native-managed-prepare-check")
restart = importlib.import_module("bound-publication-restart")
cut = importlib.import_module("managed-cut-check")

DISPATCH = "lakehouse.dispatch_managed_publication_send"
ATTEMPTS = "lakehouse.publication_attempts"
WORKER = ("EXISTS (SELECT FROM pg_stat_activity WHERE datname=current_database() "
          "AND backend_type='lakehouse managed send dispatch'")


PROFILE = "lakehouse_publication_credentials"
SEND_SESSION = "fixture-send-session"
LOOKUP_SESSION = "fixture-lookup-session"


@contextmanager
def scoped_session_store(root):
    # Require the exact purpose-specific session headers before forwarding
    # complete requests to the real S3 fixture for SigV4 and object policy.
    prefix = "/" + root.removeprefix("s3://") + "/"
    observations = []

    class Gateway(BaseHTTPRequestHandler):
        def do_GET(self):
            self.forward()

        def do_HEAD(self):
            self.forward()

        def do_PUT(self):
            self.forward()

        def forward(self):
            signature = self.headers.get("Authorization", "")
            match = re.search(r"\bCredential=([^/ ]+).*?SignedHeaders=([^, ]+)", signature)
            key = match[1] if match else ""
            covered = match[2].split(";") if match else []
            token = self.headers.get("x-amz-security-token")
            allowed = (
                (key == "lakehouse-test" and token == SEND_SESSION)
                or (key == "lakehouse-reader" and token == LOOKUP_SESSION and self.command != "PUT")
            )
            if not allowed or not self.path.startswith(prefix) or "x-amz-security-token" not in covered:
                observations.append((self.command, key, 403))
                self.send_error(403)
                return

            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            connection = HTTPConnection("127.0.0.1", 9000, timeout=5)
            try:
                connection.request(self.command, self.path, body=body, headers=dict(self.headers))
                origin = connection.getresponse()
                response = origin.read()
                observations.append((self.command, key, origin.status))
                self.send_response(origin.status)
                for name, value in origin.getheaders():
                    if name.lower() not in {"connection", "transfer-encoding", "content-length"}:
                        self.send_header(name, value)
                length = origin.getheader("Content-Length", "0") if self.command == "HEAD" else str(len(response))
                self.send_header("Content-Length", length)
                self.end_headers()
                if response:
                    self.wfile.write(response)
            finally:
                connection.close()

        def log_message(self, *_args):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Gateway) as gateway:
        thread = Thread(target=gateway.serve_forever)
        thread.start()
        try:
            yield f"http://127.0.0.1:{gateway.server_port}", observations
        finally:
            gateway.shutdown()
            thread.join()


def install_session_mappings(db, role, lookup):
    sql(db, f"""
        CREATE SERVER session_send FOREIGN DATA WRAPPER {PROFILE};
        CREATE SERVER session_lookup FOREIGN DATA WRAPPER {PROFILE};
        CREATE USER MAPPING FOR {role} SERVER session_send OPTIONS (
            purpose 'send', token 'fixture-send', s3_access_key 'lakehouse-test',
            s3_secret_key 'lakehouse-test-password', s3_session_token '{SEND_SESSION}');
        CREATE USER MAPPING FOR {lookup} SERVER session_lookup OPTIONS (
            purpose 'lookup', token 'fixture-lookup', s3_access_key 'lakehouse-reader',
            s3_secret_key 'lakehouse-reader-password', s3_session_token '{LOOKUP_SESSION}');
    """)
    assert sql(db, f"SELECT fdwhandler FROM pg_foreign_data_wrapper WHERE fdwname='{PROFILE}'") == "0"
    sql(db, f"CREATE SERVER invalid_session_options FOREIGN DATA WRAPPER {PROFILE} "
            "OPTIONS (s3_session_token 'fixture-unused')", error="HV00D")
    sql(db, f"ALTER USER MAPPING FOR {role} SERVER prepare_credentials "
            "OPTIONS (ADD s3_session_token 'fixture-unused')", error="HV00D")
    sql(db, f"ALTER USER MAPPING FOR {role} SERVER session_send "
            "OPTIONS (SET purpose 'invalid')", error="22023")


def install(db, role, lookup, *, baseline):
    restart.install_native_lookup_role(db, lookup)
    prepare.install(db, role)
    sql(db, f"SELECT {DISPATCH}(NULL::uuid, 1)", error="42883")
    print("expected red: extension 1.14 cannot dispatch a native send", flush=True)
    if baseline:
        return
    sql(db, f"""
        ALTER EXTENSION lakehouse UPDATE TO '1.16';
        ALTER ROLE {role} SET synchronous_commit TO off;
        GRANT EXECUTE ON FUNCTION {restart.NATIVE_LOOKUP}(uuid,bigint) TO {lookup};
        GRANT EXECUTE ON FUNCTION lakehouse.load_native_send_scope(uuid,bigint),
            lakehouse.get_publication_send_binding(uuid,bigint),
            lakehouse.lock_publication_send_grant(uuid,oid),
            lakehouse.lock_active_publication_binding(uuid,oid) TO {role};
        GRANT EXECUTE ON PROCEDURE lakehouse.authorize_publication_send({hold.SEND_ARGS}) TO {role};
        GRANT SELECT ON {HDR}, {MEM}, {SEG}, {ATTEMPTS}, lakehouse.publication_send_grants,
            lakehouse.managed_capture_bindings, lakehouse.managed_capture_state,
            lakehouse.publication_binding_descriptors TO {role};
        GRANT UPDATE (owner_generation) ON {HDR} TO {role};
        GRANT UPDATE (seg_id) ON {MEM} TO {role};
        GRANT UPDATE (published) ON {SEG} TO {role};
        GRANT UPDATE (revoked) ON lakehouse.publication_send_grants TO {role};
        GRANT UPDATE (active_binding_id) ON lakehouse.managed_capture_state TO {role};
        GRANT INSERT ON {ATTEMPTS} TO {role};
    """)


def cancel(db, app):
    assert sql(db, "SELECT pg_cancel_backend(pid) FROM pg_stat_activity "
                   f"WHERE datname=current_database() AND application_name='{app}'") == "t"


def pause_provider(db, call, op, forward, before_release, expected):
    """Hold the first provider GET after the initial local locks were released."""
    gate = Event()
    release = Event()

    def paused(method, path, body=b"", headers=None):
        if method == "GET" and not gate.is_set():
            gate.set()
            assert release.wait(15)
        return forward(method, path, body, headers)

    restart.catalog_request = paused
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            launch = pool.submit(sql, db, call, expected, "native_send_fault")
            try:
                if not gate.wait(10):
                    launch.result(timeout=1)
                    raise AssertionError("native preparation did not reach the catalog")
                before_release(launch)
            finally:
                release.set()
            launch.result(timeout=15)
        hold.wait_for(db, f"NOT {WORKER})")
        assert sql(db, f"SELECT count(*) FROM {ATTEMPTS} WHERE operation_id='{op}'") == "0"
    finally:
        restart.catalog_request = forward


def lost_authorization(db, role, call, read_retained):
    # Suppress the top-level CALL result after the actual synchronous WAL COMMIT.
    sql(db, f"""
        ALTER PROCEDURE lakehouse.authorize_publication_send({hold.SEND_ARGS}) RENAME TO fixture_authorize;
        CREATE PROCEDURE lakehouse.authorize_publication_send(
            op uuid, generation bigint, input_hash bytea, parent bigint, proposed bigint,
            proposal_hash bytea, body bytea, OUT intent_xid xid)
        LANGUAGE plpgsql AS $$
        BEGIN
            CALL lakehouse.fixture_authorize(op, generation, input_hash, parent, proposed,
                                             proposal_hash, body, intent_xid);
            PERFORM pg_sleep(30);
        END $$;
        REVOKE ALL ON PROCEDURE lakehouse.authorize_publication_send({hold.SEND_ARGS}) FROM PUBLIC;
        GRANT EXECUTE ON PROCEDURE lakehouse.authorize_publication_send({hold.SEND_ARGS}) TO {role};
    """)
    with ThreadPoolExecutor(max_workers=1) as pool:
        launch = pool.submit(sql, db, call, "57014", "native_send_lost_ack")
        try:
            hold.wait_for(db, WORKER + " AND wait_event='PgSleep')")
            retained = read_retained()
        finally:
            cancel(db, "native_send_lost_ack")
        launch.result(timeout=10)
    hold.wait_for(db, f"NOT {WORKER})")
    return retained


def exercise(db, directory, role, lookup, *, lose_authorization, temporary):
    table = f"iceberg.{db}.events"
    restart.trino(f"CREATE TABLE {table} (id bigint) WITH (format_version=2)")
    restart.trino(f"INSERT INTO {table} VALUES (10)")
    route, _, metadata = restart.enrolled_metadata(db)
    prefix = route.split("/namespaces/", 1)[0].removeprefix("/catalog/v1").lstrip("/")
    parent = metadata["current-snapshot-id"]
    remote = metadata["table-uuid"]
    root = metadata["location"].rstrip("/")
    op, authority, binding, incarnation = (admission.identifier() for _ in range(4))
    checkpoint = sql(db, "SELECT pg_current_wal_insert_lsn()")
    original_request = restart.catalog_request

    def read_retained():
        return restart.retained_intent(db, op, first, checkpoint)

    def audited(method, path, body=b"", headers=None):
        if method == "POST":
            observed = read_retained()
            assert observed["expected_parent"] == parent
            assert observed["binding"] == binding
            assert bytes.fromhex(observed["body"]) == body
        return original_request(method, path, body, headers)

    restart.catalog_request = audited
    store = scoped_session_store(root) if temporary else nullcontext(("http://127.0.0.1:9000", []))
    try:
        with cut.dropped_catalog_reply(route) as (endpoint, posts, reads), store as (object_endpoint, objects):
            target = prepare.approved_target(db, endpoint, prefix, authority, remote, root, binding, incarnation)
            target["object_endpoint"] = object_endpoint
            send_server = "session_send" if temporary else "prepare_credentials"
            sql(db, f"""
                CREATE SERVER source_catalog FOREIGN DATA WRAPPER lakehouse_fdw OPTIONS
                  (endpoint '{endpoint}', warehouse 'smoke', s3_endpoint '{object_endpoint}');
                CREATE TABLE events (id bigint) USING lakehouse WITH (write_mode='staged');
                SELECT lakehouse.enroll_managed_capture('events'::regclass, '{db}.events',
                    'source_catalog', '{incarnation}', '{binding}', 1, '{authority}', '{remote}', '{root}', 0, 0);
                SELECT lakehouse.grant_publication_send('{binding}', '{prefix}', 'us-east-1');
                INSERT INTO {prepare.GRANTS}
                SELECT '{binding}', umuser, umid, clock_timestamp() + interval '5 minutes',
                    '{json.dumps(target)}'::jsonb
                FROM pg_user_mappings WHERE usename='{role}' AND srvname='{send_server}';
            """)
            first = admission.capture(db, "events", 20, 20)
            sql(db, admission.managed_claim(op, first))
            admission.acquire(db, op)
            generation = 1
            call = f"SET ROLE {role}; SELECT {DISPATCH}('{op}', {generation})"
            sql(db, call, error="42501")
            assert not posts and not reads
            sql(db, f"GRANT EXECUTE ON FUNCTION {DISPATCH}(uuid,bigint) TO {role}")

            if temporary:
                sql(db, f"ALTER USER MAPPING FOR {role} SERVER session_send OPTIONS (SET purpose 'lookup')")
                sql(db, call, error="42501")
                assert not reads and not posts and not objects
                sql(db, f"ALTER USER MAPPING FOR {role} SERVER session_send OPTIONS (SET purpose 'send')")
                sql(db, f"ALTER USER MAPPING FOR {role} SERVER session_send OPTIONS (DROP s3_session_token)")
                sql(db, call, error="55000")
                assert len(reads) == 1 and not posts and not (
                    sql(db, f"SELECT * FROM {ATTEMPTS} WHERE operation_id='{op}'"))
                assert objects == [("GET", "lakehouse-test", 403)]
                sql(db, f"ALTER USER MAPPING FOR {role} SERVER session_send "
                        f"OPTIONS (ADD s3_session_token '{SEND_SESSION}')")

                def rotate_session_after_preparation(_launch):
                    sql(db, f"ALTER USER MAPPING FOR {role} SERVER session_send "
                            f"OPTIONS (SET s3_session_token '{SEND_SESSION}-next')")

                pause_provider(db, call, op, audited, rotate_session_after_preparation, "40001")
                assert len(reads) == 2 and not posts
                assert objects[1:] == [
                    ("GET", "lakehouse-test", 206),
                    ("PUT", "lakehouse-test", 200),
                    ("PUT", "lakehouse-test", 200),
                    ("PUT", "lakehouse-test", 200),
                ]
                sql(db, f"ALTER USER MAPPING FOR {role} SERVER session_send "
                        f"OPTIONS (SET s3_session_token '{SEND_SESSION}')")

            if lose_authorization:
                def cancel_before_authorization(launch):
                    cancel(db, "native_send_fault")
                    launch.result(timeout=10)

                pause_provider(db, call, op, audited, cancel_before_authorization, "57014")
                assert not posts and len(reads) == 1

                def expire_after_preparation(_launch):
                    sql(db, f"UPDATE {prepare.GRANTS} SET expires_at=clock_timestamp() - interval '1 second'")

                pause_provider(db, call, op, audited, expire_after_preparation, "42501")
                assert not posts and len(reads) == 2
                sql(db, f"UPDATE {prepare.GRANTS} SET expires_at=clock_timestamp() + interval '5 minutes'")

                def advance_after_preparation(_launch):
                    assert sql(db, f"SELECT lakehouse.advance_publication_generation('{op}', 1)") == "2"

                pause_provider(db, call, op, audited, advance_after_preparation, "40001")
                assert not posts and len(reads) == 3
                generation = 2
                call = f"SET ROLE {role}; SELECT {DISPATCH}('{op}', {generation})"

            late = admission.capture(db, "events", 30)
            if lose_authorization:
                retained = lost_authorization(db, role, call, read_retained)
                assert not posts
            else:
                receipt = json.loads(sql(db, call))
                retained = read_retained()
                assert receipt == {"status": "dispatch_unknown", "lookup_only": True,
                                   "snapshot_id": str(retained["proposed"]), "intent_xid": retained["intent_xid"]}
                assert [status for status, _ in posts] == [200], posts

            if temporary:
                sql(db, f"""
                    INSERT INTO lakehouse.publication_lookup_grants
                    SELECT '{binding}', umuser, umid, clock_timestamp() + interval '5 minutes'
                    FROM pg_user_mappings WHERE usename='{lookup}' AND srvname='session_lookup';
                """)
            else:
                restart.approve_native_lookup(db, lookup, binding)
            recovery_start = time.monotonic()
            subprocess.run([os.getenv("PG_CTL", "pg_ctl"), "restart", "-D", str(directory),
                            "-m", "immediate", "-w", "-l", str(directory.parent / "server.log")],
                           check=True, timeout=30, capture_output=True)
            assert read_retained() == retained
            recovery_ms = 1000 * (time.monotonic() - recovery_start)
            assert retained["owner_generation"] == generation
            before = len(reads)
            sql(db, call, error="55000")
            # Retire Send approval independently. Only the scoped Lookup
            # credential may now reconcile the original durable intent.
            sql(db, f"DELETE FROM {prepare.GRANTS} WHERE binding_id='{binding}'")
            sql(db, call, error="42501")
            assert len(reads) == before
            if temporary:
                sql(db, f"ALTER USER MAPPING FOR {lookup} SERVER session_lookup OPTIONS (SET purpose 'send')")
                look = f"SET ROLE {lookup}; SELECT {restart.NATIVE_LOOKUP}('{op}', {generation})"
                before_lookup = (len(reads), len(objects))
                sql(db, look, error="42501")
                assert (len(reads), len(objects)) == before_lookup
                sql(db, f"ALTER USER MAPPING FOR {lookup} SERVER session_lookup OPTIONS (SET purpose 'lookup')")
            lookup_start = time.monotonic()
            verified = json.loads(sql(db, f"SET ROLE {lookup}; SELECT {restart.NATIVE_LOOKUP}('{op}', {generation})"))
            lookup_ms = 1000 * (time.monotonic() - lookup_start)
            snapshot = str(retained["proposed"])
            assert sql(db, f"SELECT seg_id FROM {MEM} WHERE operation_id='{op}'") == str(first)
            assert sql(db, f"SELECT published,convert_from(payload,'UTF8') FROM {SEG} WHERE seg_id={late}") == "f|[[30]]"
            if lose_authorization:
                assert verified == {"status": "unresolved", "owner_generation": str(generation + 1)}, verified
                assert sql(db, f"SELECT published FROM {SEG} WHERE seg_id={first}") == "f"
                assert restart.trino(f"SELECT id FROM {table} ORDER BY id") == "10"
                assert len(posts) == 0
            else:
                assert verified == {"status": "verified", "owner_generation": str(generation + 1), "snapshot_id": snapshot,
                                    "members": 1}, verified
                assert sql(db, f"SELECT published FROM {SEG} WHERE seg_id={first}") == "t"
                assert restart.trino(f"SELECT id FROM {table} FOR VERSION AS OF {snapshot} ORDER BY id") == "10\n20\n20"
                assert len(posts) == 1
            assert restart.trino(f"SELECT id FROM {table} FOR VERSION AS OF {parent} ORDER BY id") == "10"
            assert reads[-1] == (route, "Bearer fixture-lookup")
            if temporary:
                assert objects[0] == ("GET", "lakehouse-test", 403)
                assert objects[5:] == [
                    ("GET", "lakehouse-test", 206),
                    ("PUT", "lakehouse-test", 200),
                    ("PUT", "lakehouse-test", 200),
                    ("PUT", "lakehouse-test", 200),
                    ("GET", "lakehouse-reader", 206),
                ]
            evidence = {key: value for key, value in retained.items() if key != "body"}
            print(json.dumps({"case": "lost_authorization" if lose_authorization else "lost_catalog_reply",
                              "catalog_posts": len(posts), "catalog_gets": len(reads), "parent": str(parent),
                              "proposed": snapshot, "retained": evidence, "later_pending": late,
                              "sample_count": 1, "recovery_ms": round(recovery_ms, 3),
                              "lookup_ms": round(lookup_ms, 3), "lookup_result": verified,
                              "credential_profile": "temporary_session" if temporary else "static",
                              "storage_calls": objects},
                             sort_keys=True), flush=True)
    finally:
        restart.catalog_request = original_request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--restart-fixture", required=True, type=Path)
    parser.add_argument("--baseline", action="store_true")
    parser.add_argument("--temporary-only", action="store_true")
    parser.add_argument("--credential-baseline", action="store_true")
    args = parser.parse_args()
    if not __debug__:
        raise RuntimeError("fixture requires assertions")
    directory = args.restart_fixture.resolve()
    admin = os.getenv("PGDATABASE", "postgres")
    assert Path(sql(admin, "SHOW data_directory")).resolve() == directory
    assert sql(admin, "SHOW fsync") == "on" and sql(admin, "SHOW listen_addresses") == ""
    cases = [(False, True)] if args.temporary_only or args.credential_baseline else [
        (True, False), (False, False), (False, True)
    ]
    for lose_authorization, temporary in cases:
        db = "native_dispatch_" + admission.identifier()[:8]
        role, lookup = f"{db}_send", f"{db}_lookup"
        sql(admin, f"CREATE DATABASE {db}")
        try:
            sql(db, "CREATE EXTENSION lakehouse; CREATE EXTENSION pg_walinspect;")
            install(db, role, lookup, baseline=args.baseline)
            if args.baseline:
                break
            if args.credential_baseline:
                install_session_mappings(db, role, lookup)
                return
            if temporary:
                sql(db, "ALTER EXTENSION lakehouse UPDATE TO '1.17'")
                install_session_mappings(db, role, lookup)
            restart.trino(f"CREATE SCHEMA iceberg.{db}")
            try:
                exercise(db, directory, role, lookup, lose_authorization=lose_authorization, temporary=temporary)
            finally:
                restart.trino(f"DROP TABLE IF EXISTS iceberg.{db}.events; DROP SCHEMA iceberg.{db}")
        finally:
            sql(admin, f"DROP DATABASE {db} WITH (FORCE); DROP ROLE IF EXISTS {role}, {role}_other, {lookup}")


if __name__ == "__main__":
    main()
