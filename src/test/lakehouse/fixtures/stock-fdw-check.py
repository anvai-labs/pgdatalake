"""Exercise the read-only package on a disposable stock PostgreSQL 19 cluster."""

import json
import os
import shlex
import subprocess
import time
import uuid

from publication_fixture import sql


def trino_command(statement):
    return ["docker", "exec", os.getenv("TRINO_CONTAINER", "pgdatalake-trino-1"),
           "trino", "--output-format", "TSV", "--execute", statement]


def trino(statement):
    return subprocess.check_output(trino_command(statement), text=True, timeout=90).strip()


def main():
    started = time.monotonic()
    db = "stock_" + uuid.uuid4().hex
    restored = db + "_restore"
    table = f"iceberg.{db}.events"
    assert int(sql("postgres", "SHOW server_version_num")) // 10000 == 19
    sql("postgres", f"CREATE DATABASE {db}")
    try:
        sql(db, "CREATE EXTENSION lakehouse")
        assert sql(db, "SELECT extversion FROM pg_extension WHERE extname='lakehouse'") == "1.0-fdw"
        assert sql(db, "SELECT count(*) FROM pg_am WHERE amname='lakehouse'") == "0"
        assert sql(db, "SELECT to_regclass('lakehouse.lakehouse_delta_segments') IS NULL") == "t"
        sql("postgres", f"CREATE DATABASE {restored}")
        trino(f"CREATE SCHEMA iceberg.{db}")
        try:
            trino(f"CREATE TABLE {table} (id bigint, seq integer) WITH (format_version=2)")
            trino(f"INSERT INTO {table} VALUES (10,1),(20,2),(20,3),(30,4)")
            history = f'SELECT snapshot_id FROM iceberg.{db}."events$history" ORDER BY made_current_at DESC LIMIT 1'
            parent = int(trino(history))
            sql(db, f"""
                CREATE SERVER lake FOREIGN DATA WRAPPER lakehouse_fdw OPTIONS
                  (endpoint 'http://localhost:8181/catalog', warehouse 'smoke', s3_endpoint 'http://localhost:9000');
                CREATE USER MAPPING FOR CURRENT_USER SERVER lake OPTIONS
                  (s3_access_key 'lakehouse-test', s3_secret_key 'lakehouse-test-password');
                CREATE FOREIGN TABLE old_rows(id bigint) SERVER lake OPTIONS
                  (table '{db}.events', snapshot_id '{parent}');
                CREATE FOREIGN TABLE live_rows(id bigint) SERVER lake OPTIONS (table '{db}.events');
            """)
            # Keep the PG session and generic plan alive across a real external commit.
            delete = shlex.join(trino_command(f"DELETE FROM {table} WHERE seq=3"))
            reads = sql(db, rf"""
                SET plan_cache_mode = force_generic_plan;
                PREPARE live AS SELECT array_agg(id ORDER BY id) FROM live_rows;
                EXECUTE live;
                \! {delete}
                EXECUTE live;
            """)
            assert reads.splitlines() == ["{10,20,20,30}", "{10,20,30}"], reads
            child = int(trino(history))
            assert child != parent
            assert int(trino(f'SELECT count(*) FROM iceberg.{db}."events$files" WHERE content=1')) > 0
            sql(db, f"""
                CREATE FOREIGN TABLE new_rows(id bigint) SERVER lake OPTIONS
                  (table '{db}.events', snapshot_id '{child}');
            """)
            plan = json.loads(sql(db, """
                SET min_parallel_table_scan_size = 0;
                SET parallel_setup_cost = 0;
                SET parallel_tuple_cost = 0;
                EXPLAIN (FORMAT JSON) SELECT id FROM live_rows
            """))
            assert plan[0]["Plan"]["Node Type"] == "Foreign Scan", plan
            assert not plan[0]["Plan"]["Parallel Aware"], plan
            dump = subprocess.check_output(["pg_dump", "--schema-only", db], text=True)
            sql(restored, dump)
            for name, pin, expected in (("old_rows", parent, [10, 20, 20, 30]),
                                        ("new_rows", child, [10, 20, 30])):
                reference = trino(f"SELECT id FROM {table} FOR VERSION AS OF {pin} ORDER BY id")
                assert [int(row) for row in reference.splitlines()] == expected
                for database in (db, restored):
                    actual = sql(database, f"SELECT id FROM {name} ORDER BY id")
                    assert actual == reference, (database, pin, actual, reference)
                print(f"snapshot={pin} rows={expected} stock/restore/Trino=equal", flush=True)
            for statement in ("INSERT INTO new_rows VALUES (42)", "UPDATE new_rows SET id=42",
                              "DELETE FROM new_rows"):
                sql(db, statement, error="0A000")
        finally:
            trino(f"DROP TABLE IF EXISTS {table}")
            trino(f"DROP SCHEMA iceberg.{db}")
    finally:
        sql("postgres", f"DROP DATABASE IF EXISTS {restored}")
        sql("postgres", f"DROP DATABASE {db}")

    print(f"Stock smoke completed in {time.monotonic() - started:.2f}s", flush=True)


if __name__ == "__main__":
    main()
