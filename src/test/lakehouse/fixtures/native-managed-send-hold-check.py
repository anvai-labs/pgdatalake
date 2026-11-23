"""Prove native send authorization commits before its receipt and never dispatches."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import import_module
import json
import os
from pathlib import Path
import subprocess
import threading
import time

from publication_fixture import HDR, MEM, SEG, sql


admission = import_module("publication-admission-check")
retained_intent = import_module("bound-publication-restart").retained_intent
HOLD = "lakehouse.hold_managed_publication_send"
WORKER = "lakehouse managed send hold"
SEND_ARGS = "uuid,bigint,bytea,bigint,bigint,bytea,bytea"


def native_call(op, input_digest, pin, *, generation=1, parent=100):
    body = json.dumps({"snapshot-id": pin}).encode("ascii")
    proposal_digest = hashlib.sha256(body).hexdigest()
    parent_sql = "NULL" if parent is None else str(parent)
    return (f"SELECT {HOLD}('{op}', {generation}, decode('{input_digest}', 'hex'), "
            f"{parent_sql}, {pin}, decode('{proposal_digest}', 'hex'), decode('{body.hex()}', 'hex'));")


def wait_for(db, condition):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if sql(db, f"SELECT {condition};") == "t":
            return
        time.sleep(0.05)
    raise AssertionError("native send hold did not reach the expected barrier")


def seed(db, source, operation):
    authority, table, incarnation = (admission.identifier() for _ in range(3))
    binding = admission.enroll(db, source, incarnation, 1, authority, table)
    sql(db, admission.grant(binding))
    member = admission.capture(db, source, 20)
    digest = sql(db, admission.managed_claim(operation, member))
    admission.acquire(db, operation)
    return binding, member, digest


def check(db, role, endpoint, directory, baseline):
    sql(db, "CREATE EXTENSION lakehouse; CREATE EXTENSION pg_walinspect;")
    assert sql(db, "SELECT extversion FROM pg_extension WHERE extname='lakehouse'") == "1.6"
    sql(db, "ALTER EXTENSION lakehouse UPDATE TO '1.12';")
    sql(db, f"SELECT {HOLD}(NULL::uuid, 1, NULL::bytea, NULL::bigint, 101, "
            "NULL::bytea, NULL::bytea);", error="42883")
    if baseline:
        print("expected red: 1.12 cannot invoke the native hold barrier", flush=True)
        return

    sql(db, f"""
        ALTER EXTENSION lakehouse UPDATE TO '1.13';
        CREATE ROLE {role} LOGIN;
        ALTER ROLE {role} SET synchronous_commit TO off;
        GRANT USAGE ON SCHEMA lakehouse TO {role};
        CREATE SERVER publication_catalog FOREIGN DATA WRAPPER lakehouse_fdw
            OPTIONS (endpoint '{endpoint}/catalog', warehouse 'fixture', s3_endpoint '{endpoint}');
        CREATE TABLE acknowledged (id bigint) USING lakehouse WITH (write_mode='staged');
        CREATE TABLE unacknowledged (id bigint) USING lakehouse WITH (write_mode='staged');
    """)
    assert sql(db, "SHOW fsync") == "on"
    assert sql(db, "SHOW listen_addresses") == ""
    checkpoint = sql(db, "SELECT pg_current_wal_insert_lsn();")
    first, lost = admission.identifier(), admission.identifier()
    first_binding, first_member, first_digest = seed(db, "acknowledged", first)
    lost_binding, lost_member, lost_digest = seed(db, "unacknowledged", lost)
    local_call = native_call(first, first_digest, 101, parent=None)
    sql(db, local_call, error="42501")
    sql(db, f"SET ROLE {role}; {local_call}", error="42501")
    sql(db, f"GRANT EXECUTE ON FUNCTION {HOLD}({SEND_ARGS}) TO {role};")
    sql(db, f"SET ROLE {role}; {local_call}", error="42501")
    assert sql(db, f"SELECT count(*) FROM {admission.ATTEMPTS};") == "0"

    sql(db, f"""
        GRANT EXECUTE ON PROCEDURE lakehouse.authorize_publication_send({SEND_ARGS}) TO {role};
        GRANT EXECUTE ON FUNCTION lakehouse.get_publication_send_binding(uuid, bigint),
            lakehouse.lock_publication_send_grant(uuid, oid),
            lakehouse.lock_active_publication_binding(uuid, oid),
            lakehouse.load_managed_publication_job(uuid),
            lakehouse.preflight_managed_publication_lookup(uuid, bigint) TO {role};
        GRANT SELECT ON {HDR}, {MEM}, {SEG}, {admission.ATTEMPTS}, {admission.GRANTS},
            lakehouse.managed_capture_bindings, lakehouse.managed_capture_state,
            lakehouse.publication_binding_descriptors TO {role};
        GRANT UPDATE (owner_generation) ON {HDR} TO {role};
        GRANT UPDATE (seg_id) ON {MEM} TO {role};
        GRANT UPDATE (published) ON {SEG} TO {role};
        GRANT UPDATE (revoked) ON {admission.GRANTS} TO {role};
        GRANT UPDATE (active_binding_id) ON lakehouse.managed_capture_state TO {role};
        GRANT INSERT ON {admission.ATTEMPTS} TO {role};
    """)
    sql(db, f"SET ROLE {role}; BEGIN; {local_call}", error="55000")
    sql(db, f"SET ROLE {role}; SELECT {HOLD}('{first}', 1, decode('{first_digest}', 'hex'), NULL, 101, "
            "decode(repeat('11', 32), 'hex'), decode(repeat('11', 65537), 'hex'));", error="22023")
    sql(db, f"SET ROLE {role}; {native_call(first, first_digest, 101, generation=2)}", error="40001")
    assert sql(db, f"SELECT count(*) FROM {admission.ATTEMPTS};") == "0"

    # Only one top-level CALL may reserve the operation.
    def hold_once():
        try:
            return json.loads(sql(db, f"SET ROLE {role}; {local_call}"))
        except RuntimeError as error:
            if "ERROR:  55000:" not in str(error):
                raise
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: hold_once(), range(2)))
    receipts = [receipt for receipt in results if receipt is not None]
    assert len(receipts) == 1, results
    first_record = retained_intent(db, first, first_member, checkpoint)
    assert receipts[0] == {"status": "held", "lookup_only": True, "intent_xid": first_record["intent_xid"]}
    assert first_record["binding"] == first_binding and first_record["expected_parent"] is None

    # Retain the real authorization and then withhold the worker's completion.
    # The new wrapper still calls the original procedure in its nonatomic portal.
    sql(db, f"""
        ALTER PROCEDURE lakehouse.authorize_publication_send({SEND_ARGS}) RENAME TO fixture_authorize;
        CREATE PROCEDURE lakehouse.authorize_publication_send(
            op uuid, generation bigint, input_hash bytea, parent bigint, proposed bigint,
            proposal_hash bytea, body bytea, OUT intent_xid xid)
        LANGUAGE plpgsql AS $$
        BEGIN
            IF current_setting('synchronous_commit') <> 'off' THEN
                RAISE EXCEPTION 'fixture expected the worker default to be asynchronous';
            END IF;
            CALL lakehouse.fixture_authorize(op, generation, input_hash, parent, proposed,
                                             proposal_hash, body, intent_xid);
            PERFORM pg_sleep(30);
        END $$;
        REVOKE ALL ON PROCEDURE lakehouse.authorize_publication_send({SEND_ARGS}) FROM PUBLIC;
        GRANT EXECUTE ON PROCEDURE lakehouse.authorize_publication_send({SEND_ARGS}) TO {role};
    """)
    lost_call = native_call(lost, lost_digest, 102)
    worker_running = f"EXISTS (SELECT FROM pg_stat_activity WHERE datname=current_database() AND backend_type='{WORKER}'"
    with ThreadPoolExecutor(max_workers=1) as pool:
        launcher = pool.submit(sql, db, f"SET ROLE {role}; {lost_call}", "57014", "native_send_lost_ack")
        try:
            wait_for(db, worker_running + " AND wait_event='PgSleep')")
            lost_record = retained_intent(db, lost, lost_member, checkpoint)
            assert lost_record["binding"] == lost_binding and lost_record["expected_parent"] == 100
        finally:
            assert sql(db, """
                SELECT pg_cancel_backend(pid) FROM pg_stat_activity
                WHERE datname=current_database() AND application_name='native_send_lost_ack';
            """) == "t"
        launcher.result(timeout=10)
    wait_for(db, "NOT " + worker_running + ")")
    late = admission.capture(db, "unacknowledged", 30)
    sql(db, admission.revoke(lost_binding))

    if directory is not None:
        directory = directory.resolve()
        assert Path(sql(db, "SHOW data_directory")).resolve() == directory
        prior = sql(db, "SELECT pg_postmaster_start_time();")
        subprocess.run([os.getenv("PG_CTL", "pg_ctl"), "restart", "-D", str(directory),
                        "-m", "immediate", "-w", "-l", str(directory.parent / "server.log")],
                       check=True, timeout=30, capture_output=True)
        assert sql(db, "SELECT pg_postmaster_start_time();") != prior

    assert retained_intent(db, lost, lost_member, checkpoint) == lost_record
    assert retained_intent(db, first, first_member, checkpoint) == first_record
    lookup = json.loads(sql(db, f"SET ROLE {role}; SELECT lakehouse.preflight_managed_publication_lookup('{lost}', 1);"))
    assert lookup["proposal_digest"] == lost_record["digest"] and lookup["proposed_snapshot_id"] == "102"
    sql(db, f"SET ROLE {role}; {lost_call}", error="55000")
    assert sql(db, f"SELECT seg_id, published FROM {SEG} ORDER BY seg_id;") == (
        f"{first_member}|f\n{lost_member}|f\n{late}|f")
    assert sql(db, f"SELECT seg_id FROM {MEM} WHERE operation_id='{lost}';") == str(lost_member)
    assert sql(db, "SELECT count(*) FROM lakehouse.discover_managed_publication_claims(NULL,16) "
                   "WHERE mode='lookup';") == "2"
    return {"case": "native_send_hold", "one_concurrent_authorization": True,
            "ordinary_role_denied": True, "lost_ack_sqlstate": "57014", "restart": directory is not None,
            "receipt": receipts[0], "lost_ack_intent": lost_record, "later_pending": late}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--restart-fixture", type=Path, help="PGDATA of the disposable server to restart immediately")
    parser.add_argument("--baseline", action="store_true")
    args = parser.parse_args()
    if not __debug__:
        raise RuntimeError("fixture requires assertions")
    db = "send_hold_" + admission.identifier().replace("-", "")
    role = db + "_worker"
    requests = []

    class Recorder(BaseHTTPRequestHandler):
        def request(self):
            requests.append(self.command)
            self.send_error(503)

        do_GET = do_HEAD = do_POST = do_PUT = request

        def log_message(self, *_):
            pass

    sql("postgres", f"CREATE DATABASE {db};")
    try:
        with ThreadingHTTPServer(("127.0.0.1", 0), Recorder) as recorder:
            worker = threading.Thread(target=recorder.serve_forever)
            worker.start()
            try:
                outcome = check(db, role, f"http://127.0.0.1:{recorder.server_port}", args.restart_fixture,
                                args.baseline)
                assert requests == []
                if outcome:
                    print(json.dumps(outcome | {"provider_requests": 0}), flush=True)
            finally:
                recorder.shutdown()
                worker.join()
    finally:
        sql("postgres", f"DROP DATABASE {db} WITH (FORCE); DROP ROLE IF EXISTS {role};")


if __name__ == "__main__":
    main()
