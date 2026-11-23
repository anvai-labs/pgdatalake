"""Qualify the one-shot native prepare-only worker against Lakekeeper and S3."""

import argparse
import importlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from threading import Event

from publication_fixture import HDR, SEG, sql

admission = importlib.import_module("publication-admission-check")
hold = importlib.import_module("native-managed-send-hold-check")
restart = importlib.import_module("bound-publication-restart")
cut = importlib.import_module("managed-cut-check")

PREPARE = "lakehouse.prepare_managed_publication_send"
GRANTS = "lakehouse.publication_native_send_grants"
ATTEMPTS = "lakehouse.publication_attempts"


def approved_target(namespace, endpoint, prefix, authority, table, root, binding, incarnation):
    scope = restart.approved_target(namespace, endpoint, prefix, authority, table, root,
                                    binding, incarnation)
    scope["binding"] = {key: scope.pop(key) for key in (
        "authority_id", "binding_id", "incarnation_id", "binding_generation")}
    scope["table_root"] = scope.pop("storage_root")
    scope["namespace"] = scope.pop("namespace_components")
    return scope


def install(db, role):
    sql(db, f"""
        ALTER EXTENSION lakehouse UPDATE TO '1.14';
        CREATE ROLE {role} LOGIN;
        CREATE ROLE {role}_other LOGIN;
        GRANT USAGE ON SCHEMA lakehouse TO {role}, {role}_other;
        GRANT EXECUTE ON FUNCTION lakehouse.load_native_send_preparation(uuid,bigint) TO {role}, {role}_other;
        CREATE SERVER prepare_credentials FOREIGN DATA WRAPPER lakehouse_fdw
            OPTIONS (endpoint 'http://127.0.0.1:1/catalog', warehouse 'smoke');
        CREATE USER MAPPING FOR {role} SERVER prepare_credentials
            OPTIONS (token 'fixture-send', s3_access_key 'lakehouse-test',
                     s3_secret_key 'lakehouse-test-password');
    """)


def exercise(db, role):
    table = f"iceberg.{db}.events"
    restart.trino(f"CREATE TABLE {table} (id bigint) WITH (format_version=2)")
    restart.trino(f"INSERT INTO {table} VALUES (10)")
    route, _, metadata = restart.enrolled_metadata(db)
    prefix = route.split("/namespaces/", 1)[0].removeprefix("/catalog/v1").lstrip("/")
    parent = metadata["current-snapshot-id"]
    remote = metadata["table-uuid"]
    root = metadata["location"].rstrip("/")
    op, authority, binding, incarnation = (admission.identifier() for _ in range(4))
    install(db, role)

    with cut.dropped_catalog_reply(route) as (endpoint, posts, gets):
        target = approved_target(db, endpoint, prefix, authority, remote, root, binding, incarnation)
        sql(db, f"""
            CREATE SERVER source_catalog FOREIGN DATA WRAPPER lakehouse_fdw OPTIONS
                (endpoint '{endpoint}', warehouse 'smoke', s3_endpoint 'http://127.0.0.1:9000');
            CREATE TABLE events (id bigint) USING lakehouse WITH (write_mode='staged');
            SELECT lakehouse.enroll_managed_capture('events'::regclass, '{db}.events',
                'source_catalog', '{incarnation}', '{binding}', 1, '{authority}', '{remote}', '{root}', 0, 0);
            SELECT lakehouse.grant_publication_send('{binding}', '{prefix}', 'us-east-1');
        """)
        segment = admission.capture(db, "events", 20, 20)
        sql(db, admission.managed_claim(op, segment))
        admission.acquire(db, op)
        call = f"SET ROLE {role}; SELECT {PREPARE}('{op}', 1)"
        sql(db, call, error="42501")
        sql(db, f"GRANT EXECUTE ON FUNCTION {PREPARE}(uuid,bigint) TO {role}, {role}_other")
        sql(db, call, error="42501")
        sql(db, f"""
            INSERT INTO {GRANTS}
            SELECT '{binding}', umuser, umid, clock_timestamp() - interval '1 second',
                '{json.dumps(target)}'::jsonb
            FROM pg_user_mappings WHERE usename='{role}' AND srvname='prepare_credentials';
        """)
        sql(db, call, error="42501")
        sql(db, f"""
            UPDATE {GRANTS} SET expires_at=clock_timestamp() + interval '5 minutes',
                approved_target=jsonb_set(approved_target,'{{table_root}}',to_jsonb('{root}-other'::text))
        """)
        sql(db, call, error="42501")
        sql(db, f"UPDATE {GRANTS} SET approved_target='{json.dumps(target)}'::jsonb")

        sql(db, f"SET ROLE {role}_other; SELECT {PREPARE}('{op}', 1)", error="42501")
        sql(db, f"SET ROLE {role}; SELECT {PREPARE}('{op}', 2)", error="40001")
        sql(db, f"SET ROLE {role}; BEGIN; SELECT {PREPARE}('{op}', 1)", error="55000")
        assert not gets and not posts
        assert sql(db, f"SELECT has_table_privilege('{role}', '{HDR}', 'UPDATE')") == "f"
        assert sql(db, f"SELECT has_table_privilege('{role}', '{SEG}', 'UPDATE')") == "f"
        assert sql(db, f"SELECT has_table_privilege('{role}', '{GRANTS}', 'SELECT')") == "f"

        waiting = Event()
        release = Event()
        original_request = restart.catalog_request

        def paused(method, path, body=b"", headers=None):
            if method == "GET":
                waiting.set()
                assert release.wait(20)
            return original_request(method, path, body, headers)

        restart.catalog_request = paused
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                launch = pool.submit(sql, db, call, "57014", "native_prepare_cancel")
                try:
                    if not waiting.wait(10):
                        launch.result(timeout=1)
                        raise AssertionError("the worker did not reach provider preparation")
                    assert sql(db, "SELECT pg_cancel_backend(pid) FROM pg_stat_activity "
                                   "WHERE datname=current_database() AND application_name='native_prepare_cancel'") == "t"
                    launch.result(timeout=10)
                finally:
                    release.set()
        finally:
            restart.catalog_request = original_request
        worker = "backend_type='lakehouse managed send prepare'"
        hold.wait_for(db, f"NOT EXISTS (SELECT FROM pg_stat_activity WHERE datname=current_database() AND {worker})")
        assert not posts and sql(db, f"SELECT count(*) FROM {ATTEMPTS}") == "0"
        assert gets == [(route, "Bearer fixture-send")]

        def unavailable(method, path, body=b"", headers=None):
            assert method == "GET" and path == route
            return 503, b""

        restart.catalog_request = unavailable
        try:
            sql(db, call, error="55000")
        finally:
            restart.catalog_request = original_request
        assert len(gets) == 2 and not posts
        assert sql(db, f"SELECT count(*) FROM {ATTEMPTS}") == "0"

        receipt = json.loads(sql(db, call))
        assert receipt["status"] == "prepared_discarded"
        assert receipt["expected_parent"] == str(parent)
        assert int(receipt["proposed_snapshot"]) > 0 and int(receipt["proposed_snapshot"]) != parent
        assert 0 < receipt["proposal_bytes"] <= 65536
        assert gets == [(route, "Bearer fixture-send")] * 3
        assert not posts and sql(db, f"SELECT count(*) FROM {ATTEMPTS}") == "0"
        assert sql(db, f"SELECT owner_generation FROM {HDR} WHERE operation_id='{op}'") == "1"
        assert sql(db, f"SELECT published,row_count FROM {SEG} WHERE seg_id={segment}") == "f|2"
        _, _, after = restart.enrolled_metadata(db)
        assert after["current-snapshot-id"] == parent
        assert restart.trino(f"SELECT id FROM {table} FOR VERSION AS OF {parent} ORDER BY id") == "10"
        print(json.dumps({"parent": str(parent), "catalog_gets": len(gets), "commit_posts": 0,
                          "staged_rows": [20, 20], "current_rows": [10], "receipt": receipt},
                         sort_keys=True), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", action="store_true")
    args = parser.parse_args()
    if not __debug__:
        raise RuntimeError("fixture requires assertions")
    admin = os.getenv("PGDATABASE", "postgres")
    db = "native_prepare_" + admission.identifier()[:8]
    role = f"{db}_worker"
    sql(admin, f"CREATE DATABASE {db}")
    try:
        sql(db, "CREATE EXTENSION lakehouse; ALTER EXTENSION lakehouse UPDATE TO '1.13';")
        sql(db, f"SELECT {PREPARE}(NULL::uuid, 1)", error="42883")
        print("expected red: extension 1.13 has no native prepare entry point", flush=True)
        if args.baseline:
            return
        restart.trino(f"CREATE SCHEMA iceberg.{db}")
        try:
            exercise(db, role)
        finally:
            restart.trino(f"DROP TABLE IF EXISTS iceberg.{db}.events; DROP SCHEMA iceberg.{db}")
    finally:
        sql(admin, f"DROP DATABASE {db} WITH (FORCE); DROP ROLE IF EXISTS {role}, {role}_other")


if __name__ == "__main__":
    main()
