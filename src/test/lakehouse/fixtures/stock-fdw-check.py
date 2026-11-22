"""Exercise the read-only package on a disposable stock PostgreSQL 19 cluster."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
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


def check_view_mappings(db):
    # The role DDL is transactional and db is unique per fixture run.
    owner, reader = f"{db}_owner", f"{db}_reader"
    actual = sql(db, f"""
        BEGIN;
        CREATE ROLE {owner};
        CREATE ROLE {reader};
        GRANT USAGE ON FOREIGN SERVER lake TO {owner}, {reader};
        GRANT SELECT ON new_rows TO {owner}, {reader};
        CREATE USER MAPPING FOR {owner} SERVER lake OPTIONS
          (s3_access_key 'lakehouse-test', s3_secret_key 'lakehouse-test-password');
        CREATE VIEW owner_rows AS SELECT id FROM new_rows;
        ALTER VIEW owner_rows OWNER TO {owner};
        CREATE VIEW invoker_rows WITH (security_invoker=true) AS SELECT id FROM new_rows;
        ALTER VIEW invoker_rows OWNER TO {owner};
        GRANT SELECT ON owner_rows, invoker_rows TO {reader};
        SET LOCAL ROLE {reader};
        SET LOCAL plan_cache_mode = force_generic_plan;
        PREPARE from_owner AS SELECT array_agg(id ORDER BY id) FROM owner_rows;
        PREPARE from_invoker AS SELECT array_agg(id ORDER BY id) FROM invoker_rows;
        EXECUTE from_owner;
        DO $check$ BEGIN
            EXECUTE 'EXECUTE from_invoker';
            RAISE EXCEPTION 'invoker read without its mapping';
        EXCEPTION WHEN undefined_object THEN NULL; END $check$;
        RESET ROLE;
        CREATE USER MAPPING FOR {reader} SERVER lake OPTIONS
          (s3_access_key 'lakehouse-test', s3_secret_key 'lakehouse-test-password');
        SET LOCAL ROLE {reader};
        EXECUTE from_invoker;
        RESET ROLE;
        DROP USER MAPPING FOR {owner} SERVER lake;
        SET LOCAL ROLE {reader};
        DO $check$ BEGIN
            EXECUTE 'EXECUTE from_owner';
            RAISE EXCEPTION 'owner view retained a dropped mapping';
        EXCEPTION WHEN undefined_object THEN NULL; END $check$;
        EXECUTE from_invoker;
        ROLLBACK;
    """)
    assert actual.splitlines() == ["{10,20,30}"] * 3, actual


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path, help="write evidence from this successful run")
    output = parser.parse_args().receipt
    if not __debug__:
        raise RuntimeError("the smoke requires Python assertions")
    if output is not None and output.exists():
        raise FileExistsError(output)
    started = time.monotonic()
    db = "stock_" + uuid.uuid4().hex
    restored = db + "_restore"
    table = f"iceberg.{db}.events"
    server_version = int(sql("postgres", "SHOW server_version_num"))
    assert server_version // 10000 == 19
    sql("postgres", f"CREATE DATABASE {db}")
    try:
        sql(db, "CREATE EXTENSION lakehouse")
        extension_version = sql(db, "SELECT extversion FROM pg_extension WHERE extname='lakehouse'")
        assert extension_version == "1.0-fdw"
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
            snapshots = []
            for name, pin, expected in (("old_rows", parent, [10, 20, 20, 30]),
                                        ("new_rows", child, [10, 20, 30])):
                reference = trino(f"SELECT id FROM {table} FOR VERSION AS OF {pin} ORDER BY id")
                assert [int(row) for row in reference.splitlines()] == expected
                for database in (db, restored):
                    actual = sql(database, f"SELECT id FROM {name} ORDER BY id")
                    assert actual == reference, (database, pin, actual, reference)
                snapshots.append({"snapshot_id": str(pin), "rows": [int(r) for r in actual.splitlines()]})
                print(f"snapshot={pin} rows={expected} stock/restore/Trino=equal", flush=True)
            check_view_mappings(db)
            for statement in ("INSERT INTO new_rows VALUES (42)", "UPDATE new_rows SET id=42",
                              "DELETE FROM new_rows"):
                sql(db, statement, error="0A000")
        finally:
            trino(f"DROP TABLE IF EXISTS {table}")
            trino(f"DROP SCHEMA iceberg.{db}")
    finally:
        sql("postgres", f"DROP DATABASE IF EXISTS {restored}")
        sql("postgres", f"DROP DATABASE {db}")

    if output is not None:
        # Hash the module from the fixture server's installation. Run in a clean
        # checkout and disposable prefix; this is a smoke receipt, not provenance.
        libdir = sql("postgres", "SELECT setting FROM pg_config WHERE name='PKGLIBDIR'")
        module = Path(libdir) / "anvai_lake_fdw.so"
        with module.open("rb") as artifact:
            digest = hashlib.file_digest(artifact, "sha256").hexdigest()
        core_sha = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True,
                                           cwd=Path(__file__).resolve().parent).strip()
        receipt = {
            "contract_major": 1, "contract_minor": 0,
            "profile": "stock-pg19-lakekeeper-seaweedfs-trino-v2",
            "core_sha": core_sha, "module_sha256": digest,
            "platform": f"{platform.system().lower()}-{platform.machine()}",
            "server_version_num": server_version, "extension_version": extension_version,
            "passed": ["published_serial_scan", "snapshot_id_option", "v2_position_delete",
                       "prepared_ref_refresh", "schema_restore", "foreign_dml_rejected",
                       "view_mapping_roles"],
            "parent": snapshots[0], "child": snapshots[1],
        }
        example_path = (Path(__file__).resolve().parents[4]
                        / "specs/lakehouse/modules/fixtures/stock-reader-v1.json")
        example = json.loads(example_path.read_text())["receipt"]
        for key in ("contract_major", "profile", "extension_version"):
            assert receipt[key] == example[key], key
        assert set(example) <= receipt.keys()
        assert set(example["passed"]) <= set(receipt["passed"])
        for pin in ("parent", "child"):
            assert receipt[pin]["rows"] == example[pin]["rows"]
        with output.open("x", encoding="utf-8") as stream:
            stream.write(json.dumps(receipt, indent=2) + "\n")
    print(f"Stock smoke completed in {time.monotonic() - started:.2f}s", flush=True)


if __name__ == "__main__":
    main()
