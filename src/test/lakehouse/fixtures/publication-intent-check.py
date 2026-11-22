"""Verify the local send fence and receipt transitions on an isolated database."""

import argparse
import hashlib
import json
import os
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from publication_fixture import AUTH, HDR, OP, PSQL, SEG, TABLE, bound, claim, sql


ATTEMPTS = "lakehouse.publication_attempts"


def proposal_body(op, table, digest, parent, snapshot):
    return json.dumps({
        "requirements": [
            {"type": "assert-table-uuid", "uuid": table},
            {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": parent},
            {"type": "assert-current-schema-id", "current-schema-id": 0},
            {"type": "assert-default-spec-id", "default-spec-id": 0},
        ],
        "updates": [
            {"action": "add-snapshot", "snapshot": {
                "snapshot-id": snapshot, "parent-snapshot-id": parent,
                "manifest-list": f"s3://fixture/events/metadata/{snapshot}.avro",
                "schema-id": 0, "timestamp-ms": 1700000000000,
                "summary": {"operation": "append", "pgdatalake.operation-id": op,
                            "pgdatalake.input-sha256": digest},
            }},
            {"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": snapshot},
        ],
    }, indent=2).encode() + b"\n"


def run(db, data):
    sql(db, "CREATE EXTENSION lakehouse VERSION '1.4'; CREATE TABLE events (id bigint);")
    digest = sql(db, bound(7) + bound(9) + claim(ids="7,9") + bound(8))
    sql(db, "ALTER EXTENSION lakehouse UPDATE TO '1.5'; CREATE EXTENSION pg_walinspect;")
    old_op, old_table = str(uuid.uuid4()), str(uuid.uuid4())
    old_digest = sql(db, bound(6, table=old_table) + claim(op=old_op, ids="6", table=old_table))
    old_body = proposal_body(old_op, old_table, old_digest, None, 95)
    old_hash = hashlib.sha256(old_body).hexdigest()
    sql(db, f"SELECT lakehouse.advance_publication_generation('{old_op}', 0);"
        f"CALL lakehouse.authorize_publication_send('{old_op}', 1, decode('{old_digest}', 'hex'),"
        f"NULL, 95, decode('{old_hash}', 'hex'), NULL);")
    sql(db, "ALTER EXTENSION lakehouse UPDATE;")
    assert sql(db, f"SELECT proposal_body IS NULL FROM {ATTEMPTS} WHERE operation_id='{old_op}'") == "t"
    assert sql(db, f"SELECT encode(proposal_digest, 'hex') FROM {ATTEMPTS} "
               f"WHERE operation_id='{old_op}'") == old_hash
    body = proposal_body(OP, TABLE, digest, 100, 101)
    proposal = hashlib.sha256(body).hexdigest()

    def advance(generation):
        return f"SELECT lakehouse.advance_publication_generation('{OP}', {generation});"

    def send(generation, parent="100", proposed=101, proof=digest, payload=None):
        payload = payload if payload is not None else f"decode('{body.hex()}', 'hex')"
        return (f"CALL lakehouse.authorize_publication_send('{OP}', {generation}, "
                f"decode('{proof}', 'hex'), {parent}, {proposed}, decode('{proposal}', 'hex'), "
                f"{payload}, NULL);")

    def verify(generation, snapshot=101, proposal_hash=proposal, table=TABLE):
        return (f"SELECT lakehouse.finalize_publication('{OP}', {generation}, '{AUTH}', "
                f"'{table}', decode('{digest}', 'hex'), {snapshot}, decode('{proposal_hash}', 'hex'));")

    # A parked owner resumes only after another session transferred its generation.
    assert sql(db, advance(0)) == "1"
    assert sql(db, advance(1)) == "2"
    sql(db, advance(1), error="40001")
    sql(db, send(1), error="40001")
    sql(db, verify(2), error="55000")
    sql(db, send(2, proof="ff" * 32), error="22023")
    sql(db, send(2, payload="NULL"), error="22023")
    sql(db, send(2, payload="'\\x'::bytea"), error="22023")
    sql(db, send(2, payload="decode(repeat('ab', 65537), 'hex')"), error="22023")
    sql(db, send(2, payload=f"decode('{(body.rstrip()).hex()}', 'hex')"), error="22023")
    sql(db, "BEGIN;" + send(2), error="2D000")
    assert sql(db, f"SELECT count(*) FROM {ATTEMPTS} WHERE operation_id='{OP}'") == "0"

    # Two sessions with the same token can race. Only one receives a send authorization.
    before = sql(db, "SELECT pg_current_wal_insert_lsn();")
    with ThreadPoolExecutor(max_workers=2) as pool:
        def contender():
            result = subprocess.run(PSQL + ["-d", db],
                                    input="SET synchronous_commit=off;" + send(2),
                                    capture_output=True, text=True, timeout=15)
            if result.returncode:
                assert "ERROR:  55000:" in result.stderr, result
                return None
            return int(result.stdout.strip())
        results = [result for result in pool.map(lambda _: contender(), range(2)) if result]
    assert len(results) == 1, results
    xid = results[0]
    flush = sql(db, "SELECT pg_current_wal_flush_lsn();")
    assert sql(db, f"""SELECT count(*) FROM pg_get_wal_records_info('{before}', '{flush}')
        WHERE resource_manager='Transaction' AND record_type='COMMIT' AND xid='{xid}'::xid
          AND end_lsn <= '{flush}'""") == "1"
    assert sql(db, f"SELECT owner_generation, expected_parent, proposed_snapshot_id FROM {ATTEMPTS} "
               f"WHERE operation_id='{OP}'") == "2|100|101"

    if data:
        directory = Path(data).resolve()
        assert Path(sql(db, "SHOW data_directory")).resolve() == directory
        assert sql(db, "SHOW fsync") == "on"
        started = time.monotonic()
        subprocess.run([os.getenv("PG_CTL", "pg_ctl"), "restart", "-D", str(directory),
                        "-m", "immediate", "-w", "-l", str(directory.parent / "server.log")],
                       check=True, timeout=60)
        print(f"intent crash restart took {(time.monotonic() - started):.3f}s")

    assert sql(db, f"SELECT encode(proposal_body, 'hex') FROM {ATTEMPTS} "
               f"WHERE operation_id='{OP}'") == body.hex()
    assert sql(db, f"SELECT lakehouse.advance_publication_generation('{old_op}', 1)") == "2"
    sql(db, f"CALL lakehouse.authorize_publication_send('{old_op}', 2, decode('{old_digest}', 'hex'), "
        f"NULL, 95, decode('{old_hash}', 'hex'), decode('{old_body.hex()}', 'hex'), NULL);", error="55000")
    sql(db, f"UPDATE {ATTEMPTS} SET proposal_body=NULL WHERE operation_id='{OP}'", error="55000")
    sql(db, f"DELETE FROM {ATTEMPTS} WHERE operation_id='{OP}'", error="55000")
    sql(db, f"TRUNCATE {ATTEMPTS}", error="55000")
    assert sql(db, advance(2)) == "3"
    sql(db, send(2), error="40001")
    sql(db, send(3, parent="101", proposed=102), error="55000")
    sql(db, verify(2), error="40001")
    sql(db, verify(3, snapshot=102), error="22023")
    sql(db, verify(3, proposal_hash="cd" * 32), error="22023")
    sql(db, verify(3, table=str(uuid.uuid4())), error="22023")
    sql(db, f"UPDATE {SEG} SET published=true WHERE seg_id=9;")
    sql(db, verify(3), error="55000")
    assert sql(db, f"SELECT count(*) FROM {SEG} WHERE seg_id=7 AND published") == "0"
    assert sql(db, f"SELECT count(*) FROM {HDR} WHERE verified_snapshot_id IS NOT NULL") == "0"
    sql(db, f"UPDATE {SEG} SET published=false WHERE seg_id=9;")

    sql(db, "BEGIN;" + verify(3) + "ROLLBACK;")
    assert sql(db, f"SELECT count(*) FROM {SEG} WHERE published") == "0"
    assert sql(db, f"SELECT count(*) FROM {HDR} WHERE verified_snapshot_id IS NOT NULL") == "0"

    assert sql(db, verify(3)) == "2"
    assert sql(db, verify(3)) == "2"
    assert sql(db, f"SELECT seg_id, published, published_snapshot_id FROM {SEG} WHERE seg_id>=7 ORDER BY seg_id") == "7|t|101\n8|f|\n9|t|101"
    sql(db, advance(3), error="40001")
    op = str(uuid.uuid4())
    next_digest = sql(db, claim(op=op, ids="8"))
    assert sql(db, f"SELECT lakehouse.advance_publication_generation('{op}', 0)") == "1"
    next_body = proposal_body(op, TABLE, next_digest, None, 103).ljust(65536, b" ")
    sql(db, f"CALL lakehouse.authorize_publication_send('{op}', 1, decode('{next_digest}', 'hex'), "
        f"NULL, 103, decode('{hashlib.sha256(next_body).hexdigest()}', 'hex'), "
        f"decode('{next_body.hex()}', 'hex'), NULL);")
    assert sql(db, f"SELECT expected_parent IS NULL, octet_length(proposal_body) "
               f"FROM {ATTEMPTS} WHERE operation_id='{op}'") == "t|65536"

    for signature in ("advance_publication_generation(uuid,bigint)",
                      "authorize_publication_send(uuid,bigint,bytea,bigint,bigint,bytea,bytea)",
                      "finalize_publication(uuid,bigint,uuid,uuid,bytea,bigint,bytea)"):
        assert sql(db, f"SELECT has_function_privilege('public', 'lakehouse.{signature}', 'execute')") == "f"
    assert sql(db, f"SELECT count(*) FROM {ATTEMPTS} WHERE operation_id='{OP}'") == "1"
    print("intent: concurrent authorizations=1 durable_commit_records=1 lookup_only_after_takeover=1 "
          f"receipts=1 completed_members=2 late_members_pending=1 retained_bytes={len(body)} "
          "legacy_intents_preserved=1")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--restart-fixture", metavar="PGDATA")
    args = parser.parse_args()
    admin = os.getenv("PGDATABASE", "postgres")
    db = "intent_smoke_" + uuid.uuid4().hex
    sql(admin, f"CREATE DATABASE {db}")
    try:
        run(db, args.restart_fixture)
    finally:
        sql(admin, f"DROP DATABASE {db} WITH (FORCE)")
