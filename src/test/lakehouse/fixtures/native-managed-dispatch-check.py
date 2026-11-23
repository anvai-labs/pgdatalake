"""Prove one native publication send across PostgreSQL and catalog reply loss."""

import argparse
import importlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import subprocess
from threading import Event
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


def exercise(db, directory, role, lookup, *, lose_authorization):
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
    try:
        with cut.dropped_catalog_reply(route) as (endpoint, posts, reads):
            target = prepare.approved_target(db, endpoint, prefix, authority, remote, root, binding, incarnation)
            sql(db, f"""
                CREATE SERVER source_catalog FOREIGN DATA WRAPPER lakehouse_fdw OPTIONS
                  (endpoint '{endpoint}', warehouse 'smoke', s3_endpoint 'http://127.0.0.1:9000');
                CREATE TABLE events (id bigint) USING lakehouse WITH (write_mode='staged');
                SELECT lakehouse.enroll_managed_capture('events'::regclass, '{db}.events',
                    'source_catalog', '{incarnation}', '{binding}', 1, '{authority}', '{remote}', '{root}', 0, 0);
                SELECT lakehouse.grant_publication_send('{binding}', '{prefix}', 'us-east-1');
                INSERT INTO {prepare.GRANTS}
                SELECT '{binding}', umuser, umid, clock_timestamp() + interval '5 minutes',
                    '{json.dumps(target)}'::jsonb
                FROM pg_user_mappings WHERE usename='{role}' AND srvname='prepare_credentials';
            """)
            first = admission.capture(db, "events", 20, 20)
            sql(db, admission.managed_claim(op, first))
            admission.acquire(db, op)
            generation = 1
            call = f"SET ROLE {role}; SELECT {DISPATCH}('{op}', {generation})"
            sql(db, call, error="42501")
            assert not posts and not reads
            sql(db, f"GRANT EXECUTE ON FUNCTION {DISPATCH}(uuid,bigint) TO {role}")

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
            evidence = {key: value for key, value in retained.items() if key != "body"}
            print(json.dumps({"case": "lost_authorization" if lose_authorization else "lost_catalog_reply",
                              "catalog_posts": len(posts), "catalog_gets": len(reads), "parent": str(parent),
                              "proposed": snapshot, "retained": evidence, "later_pending": late,
                              "sample_count": 1, "recovery_ms": round(recovery_ms, 3),
                              "lookup_ms": round(lookup_ms, 3), "lookup_result": verified},
                             sort_keys=True), flush=True)
    finally:
        restart.catalog_request = original_request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--restart-fixture", required=True, type=Path)
    parser.add_argument("--baseline", action="store_true")
    args = parser.parse_args()
    if not __debug__:
        raise RuntimeError("fixture requires assertions")
    directory = args.restart_fixture.resolve()
    admin = os.getenv("PGDATABASE", "postgres")
    assert Path(sql(admin, "SHOW data_directory")).resolve() == directory
    assert sql(admin, "SHOW fsync") == "on" and sql(admin, "SHOW listen_addresses") == ""
    for lose_authorization in (True, False):
        db = "native_dispatch_" + admission.identifier()[:8]
        role, lookup = f"{db}_send", f"{db}_lookup"
        sql(admin, f"CREATE DATABASE {db}")
        try:
            sql(db, "CREATE EXTENSION lakehouse; CREATE EXTENSION pg_walinspect;")
            install(db, role, lookup, baseline=args.baseline)
            if args.baseline:
                break
            restart.trino(f"CREATE SCHEMA iceberg.{db}")
            try:
                exercise(db, directory, role, lookup, lose_authorization=lose_authorization)
            finally:
                restart.trino(f"DROP TABLE IF EXISTS iceberg.{db}.events; DROP SCHEMA iceberg.{db}")
        finally:
            sql(admin, f"DROP DATABASE {db} WITH (FORCE); DROP ROLE IF EXISTS {role}, {role}_other, {lookup}")


if __name__ == "__main__":
    main()
