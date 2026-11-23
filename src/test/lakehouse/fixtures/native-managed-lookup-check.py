"""Check the opt-in, one-shot native preflight on a disposable PostgreSQL cluster."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from importlib import import_module
import json
import os
from pathlib import Path
import subprocess
import time

from publication_fixture import HDR, SEG, sql

admission = import_module("publication-admission-check")
FUNCTION = "lakehouse.preflight_managed_publication_lookup"
WORKER_TYPE = "lakehouse managed lookup preflight"


def wait_for_worker(db, active):
    deadline = time.monotonic() + 10
    wait = " AND wait_event_type='Lock'" if active else ""
    while time.monotonic() < deadline:
        count = int(sql(db, f"""
            SELECT count(*) FROM pg_stat_activity
            WHERE datname=current_database() AND backend_type='{WORKER_TYPE}'{wait};
        """))
        if bool(count) == active:
            return
        time.sleep(0.05)
    raise AssertionError("native lookup worker did not reach its expected lock/exit state")


def check(db, role, baseline, restart_fixture):
    sql(db, "CREATE EXTENSION lakehouse;")
    assert sql(db, "SELECT extversion FROM pg_extension WHERE extname = 'lakehouse'") == "1.6"
    assert sql(db, f"SELECT count(*) FROM pg_stat_activity WHERE backend_type = '{WORKER_TYPE}'") == "0"
    sql(db, "ALTER EXTENSION lakehouse UPDATE TO '1.10';")
    missing = f"SELECT {FUNCTION}('00000000-0000-0000-0000-000000000000', 1);"
    sql(db, missing, error="42883")
    if baseline:
        sql(db, "ALTER EXTENSION lakehouse UPDATE TO '1.11';", error="22023")
        print("expected red: 1.10 cannot install an explicit native lookup worker", flush=True)
        return

    authority, table, incarnation, operation = (admission.identifier() for _ in range(4))
    launch = f"{FUNCTION}('{operation}', 1)"
    run = f"SET ROLE {role}; SELECT {launch};"
    sql(db, f"""
        ALTER EXTENSION lakehouse UPDATE TO '1.11';
        CREATE ROLE {role} LOGIN;
        CREATE SERVER publication_catalog FOREIGN DATA WRAPPER lakehouse_fdw
            OPTIONS (endpoint '{admission.CATALOG}', warehouse 'fixture',
                     s3_endpoint '{admission.OBJECTS}');
        CREATE USER MAPPING FOR CURRENT_USER SERVER publication_catalog;
        CREATE TABLE items(value bigint) USING lakehouse WITH (write_mode='staged');
        CREATE TABLE legacy_items(value bigint) USING lakehouse WITH (write_mode='staged');
        SELECT lakehouse.attach_table('legacy_items', 'fixture.legacy_items', 'publication_catalog');
        GRANT USAGE ON SCHEMA lakehouse TO {role};
    """)
    binding = admission.enroll(db, "items", incarnation, 1, authority, table)
    sql(db, admission.grant(binding))
    member = admission.capture(db, "items", 20)
    input_digest = sql(db, admission.managed_claim(operation, member))
    admission.acquire(db, operation)

    sql(db, f"SELECT {launch};", error="42501")  # never use a bootstrap/superuser worker
    sql(db, run, error="42501")  # ordinary role has no launch privilege
    sql(db, f"GRANT EXECUTE ON FUNCTION {FUNCTION}(uuid, bigint) TO {role};")
    sql(db, run, error="42501")  # worker must use the role's SQL ACLs
    sql(db, f"GRANT EXECUTE ON FUNCTION lakehouse.load_managed_publication_job(uuid) TO {role};")
    sql(db, run, error="42501")  # invoker SQL still needs ledger SELECT
    sql(db, f"GRANT SELECT ON {HDR}, lakehouse.managed_capture_bindings, {admission.ATTEMPTS} TO {role};")
    sql(db, run, error="55000")  # unsent operation has no lookup candidate

    call, digest = admission.authorization(operation, input_digest, 101)
    intent_xid = int(sql(db, call))
    assert intent_xid > 0
    intent_record = f"SELECT xmin::text FROM {admission.ATTEMPTS} WHERE operation_id='{operation}';"
    assert int(sql(db, intent_record)) == intent_xid
    late = admission.capture(db, "items", 30)
    legacy = admission.capture(db, "legacy_items", 99)
    sql(db, admission.revoke(binding))
    before = sql(db, f"""
        SELECT row_to_json(j) FROM lakehouse.load_managed_publication_job('{operation}') j;
    """)
    def launch_worker():
        result = json.loads(sql(db, f"""
            SET ROLE {role};
            SELECT jsonb_build_object('launcher_pid', pg_backend_pid(), 'receipt', {launch});
        """))
        receipt = result["receipt"]
        worker_pid = receipt.pop("worker_pid")
        assert worker_pid > 0 and worker_pid != result["launcher_pid"]
        return receipt

    receipt = launch_worker()
    assert receipt == {
        "operation_id": operation,
        "authority_id": authority,
        "table_uuid": table,
        "binding_id": binding,
        "incarnation_id": incarnation,
        "binding_generation": "1",
        "owner_generation": "1",
        "attempt_owner_generation": "1",
        "expected_parent": "100",
        "proposed_snapshot_id": "101",
        "input_digest": input_digest,
        "proposal_digest": digest,
        "proposal_bytes": len(json.dumps({"snapshot-id": 101}).encode("ascii")),
    }, receipt
    sql(db, f"SET ROLE {role}; SELECT {FUNCTION}('{operation}', 2);", error="40001")
    sql(db, f"SET ROLE {role}; SELECT {FUNCTION}('{operation}', 0);", error="22023")
    assert sql(db, f"SELECT row_to_json(j) FROM lakehouse.load_managed_publication_job('{operation}') j;") == before
    segments = f"SELECT seg_id, published FROM {SEG} ORDER BY seg_id;"
    assert sql(db, segments) == f"{member}|f\n{late}|f\n{legacy}|f"

    # Cancel the SQL launcher while its worker is blocked by another backend.
    # The dynamic worker must exit while the blocker still holds its lock.
    launcher = sql(db, "SELECT current_user;")
    with ThreadPoolExecutor(max_workers=1) as pool, admission.login(db, launcher) as blocker:
        blocker(f"BEGIN; LOCK TABLE {admission.ATTEMPTS} IN ACCESS EXCLUSIVE MODE;")
        waiting = pool.submit(sql, db, run, "57014", "native_lookup_launcher")
        wait_for_worker(db, True)
        assert sql(db, """
            SELECT pg_cancel_backend(pid) FROM pg_stat_activity
            WHERE datname=current_database() AND application_name='native_lookup_launcher';
        """) == "t"
        waiting.result(timeout=10)
        wait_for_worker(db, False)
        blocker("ROLLBACK")
    assert launch_worker() == receipt

    restarted = restart_fixture is not None
    if restarted:
        directory = restart_fixture.resolve()
        assert Path(sql(db, "SHOW data_directory")).resolve() == directory
        assert sql(db, "SHOW fsync") == "on"
        started = sql(db, "SELECT pg_postmaster_start_time();")
        subprocess.run([os.getenv("PG_CTL", "pg_ctl"), "restart", "-D", str(directory),
                        "-m", "immediate", "-w", "-l", str(directory.parent / "server.log")],
                       check=True, capture_output=True, text=True, timeout=30)
        assert sql(db, "SELECT pg_postmaster_start_time();") != started
        assert launch_worker() == receipt
        assert int(sql(db, intent_record)) == intent_xid
        assert sql(db, segments) == f"{member}|f\n{late}|f\n{legacy}|f"
        assert sql(db, f"SELECT row_to_json(j) FROM lakehouse.load_managed_publication_job('{operation}') j;") == before

    # A privileged corruption of the original digest must not create a lookup
    # candidate. The stored body stays bounded and no network is consulted.
    for written_digest in ("11" * 32, digest):
        sql(db, f"""
            SET session_replication_role = replica;
            UPDATE {admission.ATTEMPTS} SET proposal_digest = decode('{written_digest}', 'hex')
              WHERE operation_id = '{operation}';
        """)
        if written_digest != digest:
            sql(db, run, error="22000")
    assert sql(db, f"""
        SELECT verified_snapshot_id IS NULL, owner_generation FROM {HDR}
        WHERE operation_id = '{operation}';
    """) == "t|1"
    assert sql(db, admission.finalize(operation, authority, table, input_digest, 101, digest)) == "1"
    sql(db, run, error="55000")
    assert sql(db, segments) == f"{member}|t\n{late}|f\n{legacy}|f"
    print(json.dumps({"case": "native_lookup_preflight", "explicit_worker_role": True,
                      "unauthorized_sqlstate": "42501", "stale_sqlstate": "40001",
                      "corrupt_digest_sqlstate": "22000", "cancel_sqlstate": "57014",
                      "reloaded_after_immediate_restart": restarted, "proposal_digest": digest,
                      "proposed_snapshot_id": "101", "proposal_bytes": receipt["proposal_bytes"],
                      "retained_intent_xid": str(intent_xid),
                      "late_row": 30, "legacy_row": 99, "ledger_mutation": False}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", action="store_true")
    parser.add_argument("--restart-fixture", type=Path, help="PGDATA of the disposable server to restart immediately")
    args = parser.parse_args()
    if not __debug__:
        raise RuntimeError("fixture requires assertions")
    name = f"native_lookup_{admission.identifier().replace('-', '')}"
    role = f"{name}_worker"
    sql("postgres", f"CREATE DATABASE {name};")
    try:
        check(name, role, args.baseline, args.restart_fixture)
    finally:
        sql("postgres", f"DROP DATABASE {name} WITH (FORCE); DROP ROLE IF EXISTS {role};")


if __name__ == "__main__":
    main()
