"""Exercise the read-only package on a disposable stock PostgreSQL 19 cluster."""

import argparse
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import platform
import shlex
import subprocess
import time
from threading import Thread
from urllib.request import urlopen
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
        SELECT count(*) FROM owner_rows;
        DO $check$ BEGIN
            EXECUTE 'EXECUTE from_invoker';
            RAISE EXCEPTION 'invoker read without its mapping';
        EXCEPTION WHEN undefined_object THEN NULL; END $check$;
        RESET ROLE;
        CREATE USER MAPPING FOR {reader} SERVER lake OPTIONS
          (s3_access_key 'lakehouse-test', s3_secret_key 'lakehouse-test-password');
        SET LOCAL ROLE {reader};
        EXECUTE from_invoker;
        SELECT count(*) FROM invoker_rows;
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
    assert actual.splitlines() == ["{10,20,30}", "3", "{10,20,30}", "3", "{10,20,30}"], actual


def check_scan_abort_cleanup(db):
    # Both failures occur after a row has reached SQL projection. Continue
    # through a savepoint in the same session so backend exit cannot hide
    # leaked foreign scan handles.
    result = subprocess.run(
        ["psql", "-XqAt", "-v", "ON_ERROR_STOP=1", "-d", db],
        input=r"""
            SET client_min_messages = debug1;
            CREATE FUNCTION cancel_read(bigint) RETURNS bigint AS $body$
            BEGIN
                PERFORM pg_cancel_backend(pg_backend_pid());
                PERFORM pg_sleep(1);
                RETURN $1;
            END
            $body$ LANGUAGE plpgsql VOLATILE;
            BEGIN;
            SAVEPOINT before_read;
            \set ON_ERROR_STOP off
            SELECT 1 / (id - id) FROM old_rows;
            \echo :SQLSTATE
            \set ON_ERROR_STOP on
            ROLLBACK TO before_read;
            SELECT array_agg(id ORDER BY id) FROM old_rows;
            \set ON_ERROR_STOP off
            SELECT cancel_read(id) FROM old_rows;
            \echo :SQLSTATE
            \set ON_ERROR_STOP on
            ROLLBACK TO before_read;
            SELECT array_agg(id ORDER BY id) FROM old_rows;
            COMMIT;
        """,
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
    )
    rows = result.stdout.splitlines()
    assert rows == ["22012", "{10,20,20,30}", "57014", "{10,20,20,30}"], result
    cleanup = "released FDW scan session and row reader during context reset"
    assert result.stderr.count(cleanup) == 2, result.stderr
    print("FDW SQL error and cancellation released both handles before same-session retry",
          flush=True)


def check_ref_advance_during_fallback(db, table, history, child):
    loads = []

    class Catalog(BaseHTTPRequestHandler):
        def do_GET(self):
            with urlopen("http://localhost:8181" + self.path, timeout=5) as upstream:
                body = upstream.read()
                content_type = upstream.headers["Content-Type"]
            if self.path.split("?", 1)[0].endswith(f"/namespaces/{db}/tables/events"):
                loads.append(json.loads(body)["metadata"]["current-snapshot-id"])
                if len(loads) == 1:
                    # Complete a real commit after saving the load response.
                    # The first query can see only the saved response.
                    trino(f"INSERT INTO {table} VALUES (40,5)")
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Catalog) as server:
        thread = Thread(target=server.serve_forever)
        thread.start()
        try:
            result = sql(db, f"""
                BEGIN;
                ALTER SERVER lake OPTIONS (SET endpoint 'http://127.0.0.1:{server.server_port}/catalog');
                SELECT count(*) FROM live_rows;
                ROLLBACK;
            """)
        finally:
            server.shutdown()
            thread.join()
    latest = int(trino(history))
    assert latest != child
    assert (result, loads) == ("3", [child]), (result, loads)
    assert sql(db, "SELECT array_agg(id ORDER BY id) FROM live_rows") == "{10,20,30,40}"
    print(f"head {child} → {latest} during LOAD; one LOAD and COUNT {result}", flush=True)


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
    fork = os.environ.get("COUNT_FORK_SMOKE") == "1"
    assert server_version // 10000 == (20 if fork else 19)
    sql("postgres", f"CREATE DATABASE {db}")
    try:
        sql(db, "CREATE EXTENSION lakehouse")
        extension_version = sql(db, "SELECT extversion FROM pg_extension WHERE extname='lakehouse'")
        assert extension_version == ("1.6" if fork else "1.0-fdw")
        if not fork:
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
            check_scan_abort_cleanup(db)
            # Keep the PG session and generic plan alive across a real external commit.
            delete = shlex.join(trino_command(f"DELETE FROM {table} WHERE seq=3"))
            reads = sql(db, rf"""
                SET plan_cache_mode = force_generic_plan;
                PREPARE live AS SELECT array_agg(id ORDER BY id) FROM live_rows;
                PREPARE live_count AS SELECT count(*) FROM live_rows;
                EXECUTE live;
                EXECUTE live_count;
                \! {delete}
                EXECUTE live;
                EXECUTE live_count;
            """)
            print("same-backend ref advance:", reads.splitlines(), flush=True)
            assert reads.splitlines() == ["{10,20,20,30}", "4", "{10,20,30}", "3"], reads
            child = int(trino(history))
            assert child != parent
            assert int(trino(f'SELECT count(*) FROM iceberg.{db}."events$files" WHERE content=1')) > 0
            sql(db, f"""
                CREATE FOREIGN TABLE new_rows(id bigint) SERVER lake OPTIONS
                  (table '{db}.events', snapshot_id '{child}');
            """)
            if not fork:
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
            for ft, want in [("old_rows", "4"), ("new_rows", "3")]:
                plan = json.loads(sql(db, f"EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON) SELECT count(*) FROM {ft}"))[0]["Plan"]
                print("setrefs", ft, plan.get("Node Type"), plan.get("Output"), plan.get("Lakehouse count"), flush=True)
                rows = sql(db, f"SET plan_cache_mode=force_generic_plan; PREPARE n AS SELECT count(*) FROM {ft}; EXECUTE n; EXECUTE n; SELECT count(id) FROM {ft}")
                print("setrefs result", ft, rows.splitlines(), flush=True)
                assert plan["Node Type"] == "Foreign Scan", plan
                mode = plan["Lakehouse count"]
                if ft == "old_rows":
                    assert mode.startswith("computed at execution from manifest "), mode
                else:
                    assert mode == "counted visible rows at the selected snapshot", mode
                assert rows.splitlines() == [want, want, want]
            check_view_mappings(db)
            actual = sql(db, """
                BEGIN;
                DROP USER MAPPING FOR CURRENT_USER SERVER lake;
                EXPLAIN (VERBOSE, FORMAT JSON) SELECT count(*) FROM old_rows;
                SELECT count(*) FROM old_rows LIMIT 0;
                ROLLBACK;
            """)
            plan = json.loads(actual)[0]["Plan"]
            assert plan["Node Type"] == "Foreign Scan", plan
            assert plan["Lakehouse count"] == "resolved on demand", plan
            print("EXPLAIN and LIMIT 0 succeeded with no mapping", flush=True)
            for statement in ("INSERT INTO new_rows VALUES (42)", "UPDATE new_rows SET id=42",
                              "DELETE FROM new_rows"):
                sql(db, statement, error="0A000")
            check_ref_advance_during_fallback(db, table, history, child)
            if fork:
                counts = sql(db, f"""
                    SELECT array_agg(id ORDER BY id) FROM live_rows FOR VERSION AS OF {child};
                    SELECT count(*) FROM live_rows FOR VERSION AS OF {child};
                    SELECT count(*) FROM live_rows;
                """)
                assert counts.splitlines() == ["{10,20,30}", "3", "4"], counts
                print(f"native historical pin {child} retained COUNT 3 with latest COUNT 4", flush=True)
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
