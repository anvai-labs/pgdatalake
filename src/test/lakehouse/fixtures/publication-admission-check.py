"""Check the opt-in SQL admission guard on a disposable PostgreSQL cluster."""

import argparse
import hashlib
import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from importlib import import_module

from publication_fixture import AUTH, HDR, MEM, SEG, bound, claim, sql

login = import_module("scoped-read-check").login
ATTEMPTS = "lakehouse.publication_attempts"
GRANTS = "lakehouse.publication_send_grants"
CATALOG = "http://127.0.0.1:1/catalog"
OBJECTS = "http://127.0.0.1:1"
REGION = "us-east-1"


def identifier():
    return str(uuid.uuid4())


def enroll(db, source, incarnation, epoch, authority, remote):
    binding = identifier()
    sql(db, f"""
        SELECT lakehouse.enroll_managed_capture(
            '{source}', 'fixture.stage.{source}', 'publication_catalog',
            '{incarnation}', '{binding}', {epoch}, '{authority}', '{remote}',
            's3://fixture-bucket/{source}', 0, 0);
    """)
    return binding


def capture(db, source, *rows):
    values = ", ".join(f"({row})" for row in rows)
    sql(db, f"INSERT INTO {source} VALUES {values};")
    encoded = json.dumps([[row] for row in rows])
    return int(sql(db, f"""
        SELECT seg_id FROM {SEG} WHERE relid = '{source}'::regclass
          AND convert_from(payload, 'UTF8')::jsonb = '{encoded}'::jsonb
    """))


def managed_claim(op, *members):
    return f"SELECT encode(lakehouse.claim_managed_publication('{op}', ARRAY{list(members)}::bigint[]), 'hex');"


def send_binding(op, generation=1):
    return f"SELECT to_jsonb(b) FROM lakehouse.get_publication_send_binding('{op}', {generation}) b;"


def lookup_binding(op):
    return f"SELECT to_jsonb(b) FROM lakehouse.get_publication_lookup_binding('{op}') b;"


def grant(binding, prefix="fixture-prefix"):
    return f"SELECT lakehouse.grant_publication_send('{binding}', '{prefix}', '{REGION}');"


def revoke(binding):
    return f"SELECT lakehouse.revoke_publication_send('{binding}');"


def acquire(db, op):
    assert sql(db, f"SELECT lakehouse.advance_publication_generation('{op}', 0)") == "1"


def authorization(op, digest, snapshot):
    body = json.dumps({"snapshot-id": snapshot}).encode("ascii")
    proposal = hashlib.sha256(body).hexdigest()
    return (
        f"CALL lakehouse.authorize_publication_send('{op}', 1, decode('{digest}', 'hex'), "
        f"100, {snapshot}, decode('{proposal}', 'hex'), decode('{body.hex()}', 'hex'), NULL);"
    ), proposal


def finalize(op, authority, remote, digest, snapshot, proposal):
    return (
        f"SELECT lakehouse.finalize_publication('{op}', 1, '{authority}', '{remote}', "
        f"decode('{digest}', 'hex'), {snapshot}, decode('{proposal}', 'hex'));"
    )


def wait_blocked(db, app, task, blocker_app=None):
    deadline = time.monotonic() + 10
    blocker = "" if blocker_app is None else f"AND blocker.application_name='{blocker_app}'"
    while sql(db, f"""
        SELECT count(*) FROM pg_stat_activity a
        JOIN pg_stat_activity blocker ON blocker.pid = ANY(pg_blocking_pids(a.pid))
        WHERE a.datname = current_database() AND a.application_name='{app}' {blocker}
    """) != "1":
        if task.done():
            task.result()
            raise AssertionError(f"{app} finished before the lock was released")
        if time.monotonic() >= deadline:
            raise TimeoutError(f"{app} did not block on the admission fence")
        time.sleep(0.05)


def revoke_first(db, transition, authorize):
    owner = sql(db, "SELECT current_user")
    with ThreadPoolExecutor(max_workers=1) as pool, login(db, owner) as revoker:
        revoker("SET application_name='admission_revoke'")
        revoker(f"BEGIN; {transition}")
        sender = pool.submit(sql, db, authorize, "55000", "admission_send")
        wait_blocked(db, "admission_send", sender, "admission_revoke")
        revoker("COMMIT")
        sender.result(timeout=10)


def check_old_attempt(db, op, authority, remote, digest, snapshot, proposal):
    assert sql(db, lookup_binding(op)) == ""
    sql(db, send_binding(op), error="55000")
    call, _ = authorization(op, digest, snapshot)
    sql(db, call, error="55000")
    assert sql(db, f"SELECT capture_binding_id IS NULL FROM {ATTEMPTS} WHERE operation_id='{op}'") == "t"
    assert sql(db, finalize(op, authority, remote, digest, snapshot, proposal)) == "1"


def setup(db):
    sql(db, f"""
        CREATE EXTENSION lakehouse;
        CREATE TABLE legacy_src (id bigint);
        CREATE SERVER publication_catalog FOREIGN DATA WRAPPER lakehouse_fdw
            OPTIONS (endpoint '{CATALOG}', warehouse 'fixture', s3_endpoint '{OBJECTS}');
    """)
    assert sql(db, "SELECT extversion FROM pg_extension WHERE extname='lakehouse'") == "1.6"
    auth, legacy_remote, op = identifier(), identifier(), identifier()
    sql(db, bound(90000, "legacy_src", legacy_remote))
    digest = sql(db, claim(op, "90000", "legacy_src", AUTH, legacy_remote))
    acquire(db, op)
    call, proposal = authorization(op, digest, 106)
    assert int(sql(db, call)) > 0

    sql(db, "ALTER EXTENSION lakehouse UPDATE TO '1.7';")
    for source in ("events", "prior_attempt", "unstamped", "race_grant"):
        sql(db, f"CREATE TABLE {source} (id bigint) USING lakehouse WITH (write_mode='staged');")
    remote = identifier()
    old_binding = enroll(db, "prior_attempt", identifier(), 1, auth, remote)
    old_seg = capture(db, "prior_attempt", 17)
    old_op = identifier()
    old_digest = sql(db, claim(old_op, str(old_seg), "prior_attempt", auth, remote))
    acquire(db, old_op)
    old_call, old_proposal = authorization(old_op, old_digest, 107)
    assert int(sql(db, old_call)) > 0

    current_remote = identifier()
    incarnation = identifier()
    prior = enroll(db, "events", incarnation, 1, auth, current_remote)
    retired_seg = capture(db, "events", 10)
    sql(db, f"SELECT lakehouse.revoke_managed_capture('events', '{prior}', 1)")
    current = enroll(db, "events", incarnation, 2, auth, current_remote)
    current_seg = capture(db, "events", 20, 20)
    late_seg = capture(db, "events", 30)
    return (
        (op, AUTH, legacy_remote, digest, 106, proposal),
        (old_op, auth, remote, old_digest, 107, old_proposal),
        (current, incarnation, auth, current_remote, retired_seg, current_seg, late_seg),
        old_binding,
    )


def exercise(db, role, baseline):
    old16, old17, captured, old_binding = setup(db)
    binding, incarnation, auth, remote, retired, current, late = captured
    operation = identifier()
    if baseline:
        sql(db, managed_claim(operation, retired, current), error="42883")
        digest = sql(db, claim(operation, f"{retired},{current}", "events", auth, remote))
        acquire(db, operation)
        call, _ = authorization(operation, digest, 108)
        assert int(sql(db, call)) > 0
        assert sql(db, f"SELECT array_agg(seg_id ORDER BY seg_id) FROM {MEM} WHERE operation_id='{operation}'") == (
            "{" + f"{retired},{current}" + "}"
        )
        print("expected red: 1.7 authorizes a single claim spanning capture generations 1 and 2")
        return

    unstamped_remote = identifier()
    unstamped_binding = enroll(db, "unstamped", identifier(), 1, auth, unstamped_remote)
    unstamped_seg = capture(db, "unstamped", 19)
    unstamped_op = identifier()
    unstamped_digest = sql(db, claim(unstamped_op, str(unstamped_seg), "unstamped", auth, unstamped_remote))
    acquire(db, unstamped_op)

    sql(db, "ALTER EXTENSION lakehouse UPDATE TO '1.8';")
    check_old_attempt(db, *old16)
    check_old_attempt(db, *old17)
    sql(db, grant(old_binding))
    assert sql(db, lookup_binding(old17[0])) == ""

    sql(db, f"CREATE ROLE {role}; GRANT USAGE ON SCHEMA lakehouse TO {role};")
    for command in (
        grant(binding), revoke(binding), managed_claim(operation, current), send_binding(operation),
        lookup_binding(old17[0]), authorization(operation, "00" * 32, 108)[0],
        f"SELECT * FROM {GRANTS};",
    ):
        sql(db, f"SET ROLE {role}; {command}", error="42501")

    sql(db, f"SELECT lakehouse.grant_publication_send('{binding}', NULL, '{REGION}')", error="22023")
    sql(db, grant(binding, "wrong/../route"), error="22023")
    sql(db, grant(unstamped_binding))
    sql(db, managed_claim(unstamped_op, unstamped_seg), error="55000")
    sql(db, send_binding(unstamped_op), error="55000")
    sql(db, authorization(unstamped_op, unstamped_digest, 109)[0], error="55000")
    assert sql(db, f"SELECT capture_binding_id IS NULL FROM {HDR} WHERE operation_id='{unstamped_op}'") == "t"
    assert sql(db, lookup_binding(unstamped_op)) == ""

    sql(db, managed_claim(operation, current), error="55000")  # No grant yet.
    sql(db, "BEGIN; " + grant(binding, "") + managed_claim(operation, current) + "ROLLBACK;")
    assert sql(db, f"SELECT count(*) FROM {GRANTS} WHERE capture_binding_id='{binding}'") == "0"
    assert sql(db, f"SELECT count(*) FROM {HDR} WHERE operation_id='{operation}'") == "0"
    sql(db, grant(binding, ""))
    for ids in ((retired, current), (current, 999999), (current, 90000)):
        sql(db, managed_claim(operation, *ids), error="55000")
    sql(db, managed_claim(operation, current, current), error="22023")
    assert sql(db, f"SELECT count(*) FROM {HDR} WHERE operation_id='{operation}'") == "0"

    digest = sql(db, managed_claim(operation, current))
    assert digest == sql(db, managed_claim(operation, current))
    acquire(db, operation)
    sql(db, send_binding(operation, 2), error="40001")
    descriptor = json.loads(sql(db, send_binding(operation)))
    expected = {
        "binding_id": binding, "incarnation_id": incarnation, "binding_generation": 2,
        "authority_id": auth, "table_uuid": remote, "server_id": sql(db, """
            SELECT oid FROM pg_foreign_server WHERE srvname='publication_catalog'
        """),
        "catalog_endpoint": CATALOG, "catalog_prefix": "", "namespace_components": ["fixture", "stage"],
        "table_name": "events", "warehouse": "fixture", "object_endpoint": OBJECTS,
        "s3_region": REGION, "storage_root": "s3://fixture-bucket/events",
        "schema_id": 0, "spec_id": 0, "ref_name": "main",
    }
    assert descriptor == expected, descriptor
    assert sql(db, lookup_binding(operation)) == ""
    # Extra unstamped member with NULL digest must not disappear from either
    # the aggregate binding proof or its digest comparison.
    extra = int(sql(db, f"INSERT INTO {SEG} (relid,payload) VALUES ('legacy_src'::regclass,'[[40]]') "
                        "RETURNING seg_id"))
    sql(db, f"BEGIN; INSERT INTO {MEM} VALUES ({extra}, '{operation}'); "
            f"{send_binding(operation)} COMMIT;", error="55000")
    assert sql(db, f"SELECT seg_id FROM {MEM} WHERE operation_id='{operation}'") == str(current)

    call, _ = authorization(operation, digest, 110)
    sql(db, "BEGIN; " + call + "ROLLBACK;", error="2D000")
    assert sql(db, lookup_binding(operation)) == ""
    revoke_first(db, f"SELECT lakehouse.revoke_managed_capture('events', '{binding}', 2);", call)
    sql(db, send_binding(operation), error="55000")
    assert sql(db, f"SELECT count(*) FROM {ATTEMPTS} WHERE operation_id='{operation}'") == "0"

    race_remote = identifier()
    race_binding = enroll(db, "race_grant", identifier(), 1, auth, race_remote)
    race_member = capture(db, "race_grant", 25)
    sql(db, grant(race_binding))
    race_op = identifier()
    race_digest = sql(db, managed_claim(race_op, race_member))
    acquire(db, race_op)
    race_scope = sql(db, send_binding(race_op))
    race_call, race_proposal = authorization(race_op, race_digest, 111)
    assert int(sql(db, race_call)) > 0
    sql(db, revoke(race_binding))
    sql(db, grant(race_binding), error="55000")
    sql(db, send_binding(race_op), error="55000")
    sql(db, race_call, error="55000")
    assert sql(db, lookup_binding(race_op)) == race_scope
    assert sql(db, f"SELECT capture_binding_id FROM {ATTEMPTS} WHERE operation_id='{race_op}'") == race_binding
    sql(db, f"UPDATE {GRANTS} SET catalog_prefix='moved' WHERE capture_binding_id='{race_binding}'",
        error="55000")
    sql(db, f"UPDATE lakehouse.managed_capture_bindings SET storage_root='s3://other/root' "
            f"WHERE binding_id='{race_binding}'", error="55000")
    assert sql(db, finalize(race_op, auth, race_remote, race_digest, 111, race_proposal)) == "1"
    assert sql(db, lookup_binding(race_op)) == race_scope
    assert sql(db, f"SELECT row_count,published FROM {SEG} WHERE seg_id={current}") == "2|f"
    assert sql(db, f"SELECT row_count,published FROM {SEG} WHERE seg_id={late}") == "1|f"
    print("admission: standalone attempt, revoke-first refusal, grant-after-send lookup, and 1.6/1.7 holds")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expect-baseline-red", action="store_true")
    args = parser.parse_args()
    if not __debug__:
        raise RuntimeError("the admission fixture requires Python assertions")
    suffix = uuid.uuid4().hex[:12]
    db, role = f"admission_{suffix}", f"admission_writer_{suffix}"
    sql("postgres", f"CREATE DATABASE {db}")
    try:
        exercise(db, role, args.expect_baseline_red)
    finally:
        sql("postgres", f"DROP DATABASE {db} WITH (FORCE); DROP ROLE IF EXISTS {role};")


if __name__ == "__main__":
    main()
