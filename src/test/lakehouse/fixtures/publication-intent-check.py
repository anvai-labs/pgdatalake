"""Verify the local send fence and receipt transitions on an isolated database."""

import argparse
import os
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from publication_fixture import AUTH, HDR, OP, PSQL, SEG, TABLE, bound, claim, sql


ATTEMPTS = "lakehouse.publication_attempts"


def run(db, data):
    sql(db, "CREATE EXTENSION lakehouse VERSION '1.4'; CREATE TABLE events (id bigint);")
    digest = sql(db, bound(7) + bound(9) + claim(ids="7,9") + bound(8))
    sql(db, "ALTER EXTENSION lakehouse UPDATE; CREATE EXTENSION pg_walinspect;")
    proposal = "ab" * 32

    def advance(generation):
        return f"SELECT lakehouse.advance_publication_generation('{OP}', {generation});"

    def send(generation, parent="100", proposed=101, proof=digest):
        return (f"CALL lakehouse.authorize_publication_send('{OP}', {generation}, "
                f"decode('{proof}', 'hex'), {parent}, {proposed}, decode('{proposal}', 'hex'), NULL);")

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
    sql(db, "BEGIN;" + send(2), error="2D000")
    assert sql(db, f"SELECT count(*) FROM {ATTEMPTS}") == "0"

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
    assert sql(db, f"SELECT owner_generation, expected_parent, proposed_snapshot_id FROM {ATTEMPTS}") == "2|100|101"

    if data:
        directory = Path(data).resolve()
        assert Path(sql(db, "SHOW data_directory")).resolve() == directory
        assert sql(db, "SHOW fsync") == "on"
        started = time.monotonic()
        subprocess.run([os.getenv("PG_CTL", "pg_ctl"), "restart", "-D", str(directory),
                        "-m", "immediate", "-w", "-l", str(directory.parent / "server.log")],
                       check=True, timeout=60)
        print(f"intent crash restart took {(time.monotonic() - started):.3f}s")

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
    assert sql(db, f"SELECT seg_id, published, published_snapshot_id FROM {SEG} ORDER BY seg_id") == "7|t|101\n8|f|\n9|t|101"
    sql(db, advance(3), error="40001")
    op = str(uuid.uuid4())
    next_digest = sql(db, claim(op=op, ids="8"))
    assert sql(db, f"SELECT lakehouse.advance_publication_generation('{op}', 0)") == "1"
    sql(db, f"CALL lakehouse.authorize_publication_send('{op}', 1, decode('{next_digest}', 'hex'), "
        f"NULL, 103, decode('{proposal}', 'hex'), NULL);")
    assert sql(db, f"SELECT expected_parent IS NULL FROM {ATTEMPTS} WHERE operation_id='{op}'") == "t"

    for signature in ("advance_publication_generation(uuid,bigint)",
                      "authorize_publication_send(uuid,bigint,bytea,bigint,bigint,bytea)",
                      "finalize_publication(uuid,bigint,uuid,uuid,bytea,bigint,bytea)"):
        assert sql(db, f"SELECT has_function_privilege('public', 'lakehouse.{signature}', 'execute')") == "f"
    assert sql(db, f"SELECT count(*) FROM {ATTEMPTS} WHERE operation_id='{OP}'") == "1"
    print("intent: concurrent authorizations=1 durable_commit_records=1 lookup_only_after_takeover=1 "
          "receipts=1 completed_members=2 late_members_pending=1")


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
