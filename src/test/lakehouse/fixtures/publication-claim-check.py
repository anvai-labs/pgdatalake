"""Check the private PostgreSQL claim provider on clean install and upgrade."""

import argparse
import hashlib
import os
import struct
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from publication_fixture import AUTH, HDR, MEM, OP, OTHER, PSQL, SEG, TABLE, bound, claim, sql


def run(db, upgrade, restart_dir=None):
    sql(db, "CREATE TABLE events (id bigint); CREATE TABLE alias (id bigint);")
    legacy = (f"INSERT INTO {SEG} (relid, payload) "
              "VALUES ('events'::regclass, '[[10]]');")
    legacy += (f"INSERT INTO {SEG} (relid, payload, published, published_snapshot_id) "
               "VALUES ('events'::regclass, '[[5]]', true, 123);")
    if upgrade:
        sql(db, "CREATE EXTENSION lakehouse VERSION '1.3';" + legacy +
            "ALTER EXTENSION lakehouse UPDATE;")
    else:
        sql(db, "CREATE EXTENSION lakehouse;" + legacy)
    assert sql(db, f"SELECT count(*) FROM {SEG} WHERE seg_id=1 AND NOT published "
               "AND row_count IS NULL AND authority_id IS NULL AND payload_digest IS NULL") == "1"
    assert sql(db, f"SELECT published_snapshot_id FROM {SEG} WHERE seg_id=2 AND published "
               "AND authority_id IS NULL") == "123"
    sql(db, bound(7) + bound(8) + bound(9, relation="alias"))
    sql(db, claim(ids="7,1"), error="55000")
    sql(db, claim(ids="7,99"), error="55000")
    sql(db, claim(ids="7,7"), error="22023")
    sql(db, claim(ids="7,NULL"), error="22023")
    sql(db, claim(ids=""), error="22023")
    sql(db, claim(ids="7,9"), error="55000")
    sql(db, claim(authority=OTHER), error="55000")
    sql(db, f"UPDATE {SEG} SET authority_id='{AUTH}', table_uuid='{TABLE}' WHERE seg_id=1;",
        error="55000")
    assert sql(db, f"SELECT count(*) FROM {HDR}") == "0"
    assert sql(db, "SELECT has_function_privilege('public', "
               "'lakehouse.claim_publication(uuid,uuid,uuid,oid,bigint[])', 'execute')") == "f"

    # A rolled-back acquisition releases both target and member reservations.
    sql(db, "BEGIN;" + claim(ids="8,7") + "ROLLBACK;")
    assert sql(db, f"SELECT count(*) FROM {MEM}") == "0"
    with subprocess.Popen(PSQL + ["-d", db], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                          text=True) as holder, ThreadPoolExecutor(max_workers=2) as pool:
        def send(cmd):
            holder.stdin.write(cmd + "\n")
            holder.stdin.flush()

        send("BEGIN;" + claim(ids="8,7"))
        digest = holder.stdout.readline().strip()
        expected = hashlib.sha256(b"".join(
            struct.pack(">qi", i, 1) + hashlib.sha256(b"[[20]]").digest()
            for i in (7, 8))).hexdigest()
        assert digest == expected, (digest, expected)
        # A separate PostgreSQL connection competes on the same target through
        # another local relation. Observe a real lock wait before releasing it.
        challenger = pool.submit(sql, db, claim(OTHER, "9", "alias"), "40001", "ledger_racer")
        same = pool.submit(sql, db, claim(str(uuid.uuid4()), "7"), "40001", "ledger_same")
        deadline = time.monotonic() + 10
        try:
            while sql(db, "SELECT count(*) FROM pg_stat_activity WHERE datname = "
                       "current_database() AND application_name IN ('ledger_racer','ledger_same') AND "
                       "cardinality(pg_blocking_pids(pid)) > 0") != "2":
                if challenger.done() or same.done():
                    (challenger if challenger.done() else same).result()
                    raise AssertionError("claimant completed before first claim committed")
                if time.monotonic() >= deadline:
                    raise TimeoutError("claimant did not block on the uncommitted claim")
                time.sleep(0.05)
            sql(db, bound(10) + bound(11, table=OTHER) + claim(str(uuid.uuid4()), "11", table=OTHER))
        finally:
            send("COMMIT;")
            holder.stdin.close()
        challenger.result()
        same.result()
        assert holder.wait(timeout=5) == 0

    if restart_dir:
        data = Path(restart_dir).resolve()
        assert Path(sql(db, "SHOW data_directory")).resolve() == data
        assert sql(db, "SHOW fsync") == "on"
        started = time.monotonic()
        subprocess.run([os.getenv("PG_CTL", "pg_ctl"), "restart", "-D", str(data),
                        "-m", "immediate", "-w", "-l", str(data.parent / "server.log")],
                       check=True, timeout=60)
        print(f"PostgreSQL crash restart completed in {time.monotonic() - started:.3f}s")
    assert sql(db, claim()) == expected
    sql(db, claim(ids="7,10"), error="22023")
    sql(db, claim(str(uuid.uuid4()), "8"), error="40001")
    sql(db, f"UPDATE {SEG} SET payload='[]' WHERE seg_id=7", error="55000")
    sql(db, f"DELETE FROM {SEG} WHERE seg_id=7", error="23503")
    assert sql(db, f"SELECT array_agg(seg_id ORDER BY seg_id) FROM {MEM} "
               f"WHERE operation_id='{OP}'") == "{7,8}"
    assert sql(db, f"SELECT array_agg(seg_id ORDER BY seg_id) FROM {SEG} s WHERE NOT published "
               f"AND authority_id IS NOT NULL AND NOT EXISTS (SELECT FROM {MEM} m "
               "WHERE m.seg_id=s.seg_id)") == "{9,10}"
    print(f"{'upgrade' if upgrade else 'fresh'}: claimed=2 pending=2 unrelated_claims=1 "
          "conflicting_claims=0 digest=" + expected)


def check_capacity(db):
    target, op = str(uuid.uuid4()), str(uuid.uuid4())
    sql(db, f"""INSERT INTO {SEG} (seg_id, relid, payload, row_count, authority_id, table_uuid)
        OVERRIDING SYSTEM VALUE
        SELECT id, 'events'::regclass, convert_to('[["' || repeat('x',8186) || '"]]', 'UTF8'),
               1, '{AUTH}', '{target}' FROM generate_series(1000,2023) id""")
    request = (f"SELECT lakehouse.claim_publication('{op}', '{AUTH}', '{target}', "
               "'events'::regclass, ARRAY(SELECT generate_series(1000,2023)::bigint))")
    sql(db, request.replace("2023", "2024"), error="22023")
    started = time.monotonic()
    sql(db, request)
    elapsed_ms = (time.monotonic() - started) * 1000
    assert sql(db, f"SELECT count(*) FROM {MEM} WHERE operation_id='{op}'") == "1024"
    print(f"capacity: segments=1024 payload_bytes=8388608 claim_client_ms={elapsed_ms:.1f} samples=1")


def live_legacy(db):
    ns = "claim_" + uuid.uuid4().hex[:12]
    table = f"iceberg.{ns}.events"

    def trino(query):
        return subprocess.check_output(
            ["docker", "exec", os.getenv("TRINO_CONTAINER", "pgdatalake-trino-1"),
             "trino", "--output-format", "TSV", "--execute", query],
            text=True, timeout=90).strip()

    trino(f"CREATE SCHEMA iceberg.{ns}")
    try:
        trino(f"CREATE TABLE {table} (id bigint) WITH (format_version=2)")
        snapshot_query = f'SELECT snapshot_id FROM iceberg.{ns}."events$snapshots"'
        initial = set(trino(snapshot_query).splitlines())
        sql(db, f"""
            CREATE SERVER pub_live FOREIGN DATA WRAPPER lakehouse_fdw
              OPTIONS (endpoint 'http://localhost:8181/catalog', warehouse 'smoke',
                       s3_endpoint 'http://localhost:9000');
            CREATE USER MAPPING FOR CURRENT_USER SERVER pub_live
              OPTIONS (s3_access_key 'lakehouse-test', s3_secret_key 'lakehouse-test-password');
            CREATE TABLE mixed (id bigint) USING lakehouse;
            SELECT lakehouse.attach_table('mixed'::regclass, '{ns}.events', 'pub_live');
            SET lakehouse.write_mode = 'staged';
            INSERT INTO mixed VALUES (42);
        """)
        # The publisher shares the local relation with a bound candidate. Its
        # successful legacy append must leave that candidate in PostgreSQL.
        sql(db, bound(27, "mixed", table=OTHER))
        with subprocess.Popen(PSQL + ["-d", db], stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as stale:
            stale.stdin.write(f"BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT count(*) FROM {SEG} "
                              "WHERE relid='mixed'::regclass AND NOT published;\n")
            stale.stdin.flush()
            assert stale.stdout.readline().strip() == "2"
            assert sql(db, "SELECT lakehouse.publish_staged('mixed')") == "1"
            _, error = stale.communicate("SELECT lakehouse.publish_staged('mixed');", timeout=30)
            assert stale.returncode and "ERROR:  40001:" in error, error
        assert sql(db, "SELECT lakehouse.publish_staged('mixed')") == "0"
        assert sql(db, f"SELECT count(*) FROM {SEG} WHERE seg_id=27 AND NOT published") == "1"
        assert trino(f"SELECT id FROM {table} ORDER BY id") == "42"
        after = set(trino(snapshot_query).splitlines())
        reported = sql(db, f"SELECT published_snapshot_id FROM {SEG} "
                       "WHERE relid='mixed'::regclass AND published")
        assert after - initial == {reported} and initial <= after, (initial, after, reported)
        print("legacy: staged=2 published=1 bound_pending=1 stale=40001 Trino=[42] new_snapshots=1")
    finally:
        trino(f"DROP TABLE IF EXISTS {table}; DROP SCHEMA iceberg.{ns}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true", help="also check the legacy publisher with Trino")
    parser.add_argument("--restart-fixture", metavar="PGDATA",
                        help="crash and restart this disposable cluster after the upgrade claim commits")
    args = parser.parse_args()
    admin = os.environ.get("PGDATABASE", "postgres")
    for upgrade_case in (False, True):
        name = "claim_smoke_" + uuid.uuid4().hex
        sql(admin, f"CREATE DATABASE {name}")
        try:
            run(name, upgrade_case, args.restart_fixture if upgrade_case else None)
            check_capacity(name)
            if args.live and not upgrade_case:
                live_legacy(name)
        finally:
            sql(admin, f"DROP DATABASE {name} WITH (FORCE)")
