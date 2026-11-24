"""Boundary contracts for direct COW mutations (spec 18 §3a–3c).

Trino is the independent oracle throughout. One fixture database per run
against the fixture stack (Lakekeeper on localhost:8181, S3 on
localhost:9000, fork PostgreSQL on $PGHOST/$PGPORT).

T1  managed DELETE of published rows: exact read-back, Trino agrees at
    the new snapshot, a foreign table pinned to the OLD snapshot still
    returns the original rows (MVCC across the overwrite)
T3  mixed transaction: staged INSERTs + tombstoned published row commit
    as one overwrite, every row exactly once; control: mutating a
    same-transaction row → 55000 "cannot modify a row inserted"
T5  zero-row DELETE/UPDATE: no new snapshot, rows unchanged
T6  FDW write-refusal control: INSERT/UPDATE/DELETE on an external
    foreign table → core 0A000, remote snapshot unchanged
T7  savepoint rollback of a COW mutation: row restored, zero catalog
    POSTs (snapshot unchanged), rows unchanged
T8  delete-bearing source snapshot: 0A000 "Iceberg deletes" refusal
"""

import importlib
import json
from pathlib import Path
from uuid import uuid4

from publication_fixture import sql

trino = importlib.import_module("bound-publication-restart").trino

CATALOG = "http://localhost:8181/catalog"
S3 = "http://localhost:9000"


def snap(ns):
    return trino(f'SELECT snapshot_id FROM iceberg.{ns}."events$history" '
                 f"ORDER BY made_current_at DESC LIMIT 1").strip()


def rows(db, name):
    return sql(db, f"SELECT coalesce(string_agg(id::text, ',' ORDER BY id), '') "
                   f"FROM {name}").strip()


def trino_rows(ns):
    return trino(f"SELECT id FROM iceberg.{ns}.events ORDER BY id").strip().replace("\n", ",")


def setup(db, ns, name, values):
    sql(db, f"CREATE TABLE {name} (id bigint) USING lakehouse")
    sql(db, f"SELECT lakehouse.attach_table('{name}'::regclass, "
            f"'{ns}.events', 'cow_catalog')")
    got = rows(db, name)
    assert got == values, (name, got, values)


def t1(db, ns):
    """Happy path: delete two of four rows, verify MVCC across the
    overwrite with a foreign table pinned to the old snapshot."""
    setup(db, ns, "t1", "10,20,30,40")
    old = snap(ns)
    sql(db, "BEGIN;\n"
            "DELETE FROM t1 WHERE id IN (20, 30);\n"
            "UPDATE t1 SET id = 41 WHERE id = 40;\n"
            "COMMIT")
    assert rows(db, "t1") == "10,41", rows(db, "t1")
    assert trino_rows(ns) == "10,41", trino_rows(ns)
    sql(db, f"CREATE FOREIGN TABLE t1_old (id bigint) SERVER cow_catalog "
            f"OPTIONS (table '{ns}.events')")
    pinned = sql(db, f"SELECT coalesce(string_agg(id::text, ',' ORDER BY id), '') "
                     f"FROM t1_old FOR VERSION AS OF {old}").strip()
    assert pinned == "10,20,30,40", (old, pinned)
    print(json.dumps({"case": "t1-cow-happy-path", "old_snapshot": old,
                      "new_rows": "10,41", "pinned_rows": pinned}), flush=True)


def t3(db, ns):
    """Mixed transaction: staged INSERTs + one tombstoned published row,
    one overwrite, zero duplication; unaddressable-row control."""
    setup(db, ns, "t3", "1,2,3,4")
    old = snap(ns)
    sql(db, "BEGIN;\n"
            "INSERT INTO t3 VALUES (100), (101);\n"
            f"SAVEPOINT s3;\n"
            "DO $$ BEGIN\n"
            "    DELETE FROM t3 WHERE id = 100;\n"
            "    RAISE EXCEPTION 'control failed: same-transaction row was mutable';\n"
            "EXCEPTION WHEN object_not_in_prerequisite_state THEN\n"
            "    IF SQLERRM NOT LIKE '%cannot modify a row inserted in this transaction%' THEN\n"
            "        RAISE;\n"
            "    END IF;\n"
            "END $$;\n"
            "ROLLBACK TO s3;\n"
            "DELETE FROM t3 WHERE id = 2;\n"
            "COMMIT")
    got = rows(db, "t3")
    assert got == "1,3,4,100,101", got
    assert trino_rows(ns) == "1,3,4,100,101", trino_rows(ns)
    print(json.dumps({"case": "t3-mixed-transaction", "old_snapshot": old,
                      "rows": got}), flush=True)


def t5(db, ns):
    """Zero-row mutation is a no-op: snapshot unchanged, rows unchanged."""
    setup(db, ns, "t5", "7,8")
    before = snap(ns)
    sql(db, "BEGIN;\n"
            "DELETE FROM t5 WHERE false;\n"
            "UPDATE t5 SET id = 99 WHERE false;\n"
            "COMMIT")
    assert rows(db, "t5") == "7,8", rows(db, "t5")
    after = snap(ns)
    assert after == before, (before, after)
    print(json.dumps({"case": "t5-zero-row-noop", "snapshot": before}), flush=True)


def t6(db, ns):
    """FDW write refusal: core 0A000 before any catalog request."""
    sql(db, f"CREATE FOREIGN TABLE ftest (id bigint) SERVER cow_catalog "
            f"OPTIONS (table '{ns}.events')")
    before = snap(ns)
    sql(db, "DO $$ BEGIN\n"
            "    INSERT INTO ftest VALUES (1);\n"
            "    RAISE EXCEPTION 'control failed: foreign INSERT accepted';\n"
            "EXCEPTION WHEN feature_not_supported THEN\n"
            "    IF SQLERRM NOT LIKE '%cannot insert into foreign table%' THEN RAISE; END IF;\n"
            "END $$;\n"
            "DO $$ BEGIN\n"
            "    UPDATE ftest SET id = 2;\n"
            "    RAISE EXCEPTION 'control failed: foreign UPDATE accepted';\n"
            "EXCEPTION WHEN feature_not_supported THEN\n"
            "    IF SQLERRM NOT LIKE '%cannot update foreign table%' THEN RAISE; END IF;\n"
            "END $$;\n"
            "DO $$ BEGIN\n"
            "    DELETE FROM ftest;\n"
            "    RAISE EXCEPTION 'control failed: foreign DELETE accepted';\n"
            "EXCEPTION WHEN feature_not_supported THEN\n"
            "    IF SQLERRM NOT LIKE '%cannot delete from foreign table%' THEN RAISE; END IF;\n"
            "END $$")
    assert snap(ns) == before, (before, snap(ns))
    print(json.dumps({"case": "t6-fdw-write-refusal", "snapshot": before}), flush=True)


def t7(db, ns):
    """Savepoint rollback restores the row; nothing is published."""
    setup(db, ns, "t7", "5,6")
    before = snap(ns)
    sql(db, "BEGIN;\n"
            "SAVEPOINT s7;\n"
            "DELETE FROM t7 WHERE id = 5;\n"
            "ROLLBACK TO s7;\n"
            "COMMIT")
    assert rows(db, "t7") == "5,6", rows(db, "t7")
    assert snap(ns) == before, (before, snap(ns))
    print(json.dumps({"case": "t7-savepoint-rollback", "snapshot": before}), flush=True)


def t8(db, ns):
    """Delete-bearing source snapshot refuses before any rewrite."""
    trino(f"DELETE FROM iceberg.{ns}.events WHERE id = 20")
    assert trino(f"SELECT count(*) FROM iceberg.{ns}."
                 f"\"events$files\" WHERE content = 1").strip() >= "1"
    sql(db, f"CREATE TABLE t8 (id bigint) USING lakehouse")
    sql(db, f"SELECT lakehouse.attach_table('t8'::regclass, "
            f"'{ns}.events', 'cow_catalog')")
    assert rows(db, "t8") == "10,30", rows(db, "t8")
    sql(db, "DO $$ BEGIN\n"
            "    DELETE FROM t8 WHERE id = 30;\n"
            "    RAISE EXCEPTION 'control failed: delete-bearing rewrite accepted';\n"
            "EXCEPTION WHEN feature_not_supported THEN\n"
            "    IF SQLERRM NOT LIKE '%Iceberg delete%' THEN RAISE; END IF;\n"
            "END $$;\n"
            "DO $$ BEGIN\n"
            "    UPDATE t8 SET id = 31 WHERE id = 30;\n"
            "    RAISE EXCEPTION 'control failed: delete-bearing rewrite accepted';\n"
            "EXCEPTION WHEN feature_not_supported THEN\n"
            "    IF SQLERRM NOT LIKE '%Iceberg delete%' THEN RAISE; END IF;\n"
            "END $$")
    print(json.dumps({"case": "t8-delete-bearing-refusal"}), flush=True)


def main():
    if not __debug__:
        raise RuntimeError("the boundary fixture requires Python assertions")
    ns = "cowbd_" + uuid4().hex[:12]
    trino(f"CREATE SCHEMA iceberg.{ns}")
    name = "cowbd_" + uuid4().hex[:8]
    try:
        sql("postgres", f"CREATE DATABASE {name} TEMPLATE template0 "
                        f"ENCODING 'UTF8' LOCALE 'C'")
        try:
            sql(name, "CREATE EXTENSION lakehouse;\n"
                      "CREATE FOREIGN DATA WRAPPER cow_wrapper "
                      "HANDLER lakehouse.lakehouse_fdw_handler;\n"
                      f"CREATE SERVER cow_catalog FOREIGN DATA WRAPPER cow_wrapper "
                      f"OPTIONS (endpoint '{CATALOG}', warehouse 'smoke', "
                      f"s3_endpoint '{S3}');\n"
                      f"CREATE USER MAPPING FOR CURRENT_USER SERVER cow_catalog "
                      f"OPTIONS (s3_access_key 'lakehouse-test', "
                      f"s3_secret_key 'lakehouse-test-password')")
            for case, sub in (("t1", "t1"), ("t3", "t3"), ("t5", "t5"),
                              ("t6", None), ("t7", "t7"), ("t8", "t8")):
                sub_ns = f"{ns}_{case}"
                trino(f"CREATE SCHEMA iceberg.{sub_ns}")
                try:
                    trino(f"CREATE TABLE iceberg.{sub_ns}.events (id bigint) "
                          f"WITH (format_version = 2)")
                    seed = {"t1": "10, 20, 30, 40", "t3": "1, 2, 3, 4",
                            "t5": "7, 8", "t7": "5, 6", "t8": "10, 20, 30"}.get(case)
                    if seed:
                        trino(f"INSERT INTO iceberg.{sub_ns}.events VALUES {seed}")
                    if case == "t6":
                        t6(name, sub_ns)
                    else:
                        globals()[case](name, sub_ns)
                finally:
                    trino(f"DROP TABLE IF EXISTS iceberg.{sub_ns}.events; "
                          f"DROP SCHEMA iceberg.{sub_ns}")
        finally:
            sql("postgres", f"DROP DATABASE {name} WITH (FORCE)")
    finally:
        trino(f"DROP SCHEMA iceberg.{ns}")
    print("cow-boundary: all cases green", flush=True)


if __name__ == "__main__":
    main()
