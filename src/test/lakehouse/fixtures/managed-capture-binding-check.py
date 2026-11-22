"""Red/green fixture for ordinary TableAM INSERT and durable capture enrollment."""

import argparse
import json
import os
import subprocess
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

from publication_fixture import HDR, MEM, PSQL, SEG, claim, sql


@dataclass(frozen=True)
class Binding:
    incarnation: uuid.UUID
    generation: int
    identity: str
    authority: uuid.UUID
    table: uuid.UUID
    binding: uuid.UUID
    root: str

    @classmethod
    def create(cls, incarnation, generation, name, authority, table=None):
        return cls(incarnation, generation, f"fixture.{name}", authority,
                   table or uuid.uuid4(), uuid.uuid4(), f"s3://fixture-bucket/{name}")

    def enroll_sql(self, *, provider=False):
        function = "lakehouse.enroll_managed_capture" if provider else "capture_fixture.enroll"
        schema_and_spec = ", 0, 0" if provider else ""
        return (
            f"SELECT {function}('events'::regclass, "
            f"'{self.identity}', 'capture_catalog', '{self.incarnation}'::uuid, "
            f"'{self.binding}'::uuid, {self.generation}::bigint, "
            f"'{self.authority}'::uuid, '{self.table}'::uuid, '{self.root}'{schema_and_spec});"
        )

    def imprint(self):
        return [str(self.authority), str(self.table), str(self.binding),
                str(self.incarnation), self.generation]


# A test-side record of the approved facts. Only the legacy attach call
# reaches production code. Writing an imprint from this mock into the ledger
# would defeat the red test.
BASELINE_ADAPTER = """
    CREATE TABLE capture_fixture.enrollment (
        binding_id uuid PRIMARY KEY, incarnation_id uuid NOT NULL,
        generation bigint NOT NULL, relid oid NOT NULL, authority_id uuid NOT NULL,
        table_uuid uuid NOT NULL, storage_root text NOT NULL,
        revoked boolean NOT NULL DEFAULT false
    );
    CREATE TABLE capture_fixture.bound_required (relid oid PRIMARY KEY);
    CREATE FUNCTION capture_fixture.require_binding(target regclass)
    RETURNS void LANGUAGE sql AS $$
        INSERT INTO capture_fixture.bound_required VALUES (target);
    $$;
    CREATE FUNCTION capture_fixture.enroll(
        target regclass, identity text, server_name text, incarnation uuid,
        binding uuid, generation bigint, authority uuid, remote_table uuid, storage_root text
    ) RETURNS void LANGUAGE plpgsql AS $$
    BEGIN
        PERFORM lakehouse.attach_table(target, identity, server_name);
        INSERT INTO capture_fixture.enrollment VALUES
            (binding, incarnation, generation, target, authority, remote_table, storage_root);
    END $$;
    CREATE FUNCTION capture_fixture.revoke(target regclass, binding uuid, epoch bigint)
    RETURNS void LANGUAGE plpgsql AS $$
    BEGIN
        UPDATE capture_fixture.enrollment SET revoked
            = true WHERE binding_id=binding AND relid=target AND generation=epoch;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'fixture could not revoke its enrolled generation';
        END IF;
    END $$;
    CREATE FUNCTION capture_fixture.provenance(segment_id bigint)
    RETURNS jsonb LANGUAGE sql AS $$
        SELECT '[null,null,null]'::jsonb;
    $$;
"""

EXPECTED_RED = [
    "old-imprint", "writer-acl", "claim-original", "late-imprint",
    "legacy-reattach", "revoked-insert", "replacement-imprint", "recreated-imprint",
]


def race_transition(db, role, staged, transition, release, retry=None):
    with (
        ThreadPoolExecutor(max_workers=1) as executor,
        subprocess.Popen(PSQL + ["-d", db], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, text=True) as writer,
    ):
        def send(statement):
            writer.stdin.write(statement + "\n")
            writer.stdin.flush()

        try:
            send(f"SET ROLE {role}; BEGIN; {staged} SELECT 'staged';")
            assert writer.stdout.readline().strip() == "staged"
            changer = executor.submit(sql, db, transition, app="capture_transition")
            deadline = time.monotonic() + 10
            while sql(db, """
                SELECT count(*) FROM pg_stat_activity
                WHERE datname = current_database() AND application_name = 'capture_transition'
                  AND cardinality(pg_blocking_pids(pid)) = 1
            """) != "1":
                if changer.done():
                    changer.result()
                    raise AssertionError("transition completed before capture released its lock")
                if time.monotonic() >= deadline:
                    raise TimeoutError("managed capture transition did not wait for the writer")
                time.sleep(0.05)
            send(f"{release} SELECT 'released';")
            assert writer.stdout.readline().strip() == "released"
            changer.result(timeout=10)
            _, error = writer.communicate(retry or "ROLLBACK;\n", timeout=10)
            if retry:
                assert writer.returncode and "ERROR:  55000:" in error, error
            else:
                assert writer.returncode == 0, error
        finally:
            if writer.poll() is None:
                writer.terminate()


def verify_restart(db, data):
    before = {name: sql(db, f"SELECT to_jsonb(r) FROM {name} r ORDER BY 1")
              for name in (SEG, HDR, MEM, "lakehouse.managed_capture_state",
                           "lakehouse.managed_capture_bindings")}
    data = data.resolve()
    assert Path(sql(db, "SHOW data_directory")).resolve() == data
    assert sql(db, "SHOW fsync") == "on"
    assert sql(db, "SHOW synchronous_commit") == "on"
    start = time.monotonic()
    subprocess.run(
        [os.getenv("PG_CTL", "pg_ctl"), "restart", "-D", str(data),
         "-m", "immediate", "-w", "-l", str(data.parent / "capture-restart.log")],
        check=True, timeout=60,
    )
    for name, prior in before.items():
        assert sql(db, f"SELECT to_jsonb(r) FROM {name} r ORDER BY 1") == prior, name
    print(f"capture WAL recovery preserved ledger and binding state in {time.monotonic() - start:.3f}s")


def check_cow_cutover(db, requests, catalog_replies):
    metadata_path = "/fixture-bucket/cow_events/metadata.json"
    route = "/v1/namespaces/fixture/tables/cow_events"
    catalog_replies.update({
        ("GET", "/v1/config"): {"defaults": {}, "overrides": {}},
        ("GET", route): {"metadata-location": f"s3:/{metadata_path}", "config": {}},
        ("POST", route): {"metadata-location": f"s3:/{metadata_path}", "config": {}},
        ("GET", metadata_path): {
            "format-version": 2, "table-uuid": str(uuid.uuid4()),
            "location": "s3://fixture-bucket/cow_events", "last-sequence-number": 0,
            "current-schema-id": 0, "snapshots": [],
            "schemas": [{"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "id", "required": False, "type": "long"},
            ]}],
        },
    })
    try:
        result = subprocess.run(PSQL + ["-d", db], check=False, input="""
            CREATE TABLE cow_events (id bigint) USING lakehouse WITH (write_mode='cow');
            SELECT lakehouse.attach_table('cow_events', 'fixture.cow_events', 'capture_catalog');
            ALTER USER MAPPING FOR CURRENT_USER SERVER capture_catalog
                OPTIONS (ADD s3_access_key 'fixture', ADD s3_secret_key 'fixture');
            BEGIN;
            INSERT INTO cow_events VALUES (80);
            SELECT lakehouse.require_managed_capture('cow_events');
            COMMIT;
        """, capture_output=True, text=True, timeout=35)
        observed = [(method, urlsplit(path).path) for method, path in requests]
        assert result.returncode and "ERROR:  55000:" in result.stderr, (result, observed)
        assert observed == [("GET", "/v1/config"), ("GET", route), ("GET", metadata_path)], observed
        assert sql(db, "SELECT count(*) FROM lakehouse.managed_capture_state "
                       "WHERE relid = 'cow_events'::regclass") == "0"
    finally:
        catalog_replies.clear()
        requests.clear()
    print("legacy COW cutover refused after session open, before any catalog POST")


def exercise(db, adapter, baseline, endpoint, requests, catalog_replies, role, restart_dir):
    sql(db, f"""
        CREATE EXTENSION lakehouse;
        CREATE SCHEMA capture_fixture;
        CREATE SERVER capture_catalog FOREIGN DATA WRAPPER lakehouse_fdw OPTIONS
            (endpoint '{endpoint}', warehouse 'fixture', s3_endpoint '{endpoint}');
        CREATE ROLE {role};
        GRANT USAGE ON FOREIGN SERVER capture_catalog TO {role};
        CREATE USER MAPPING FOR CURRENT_USER SERVER capture_catalog;
        CREATE USER MAPPING FOR {role} SERVER capture_catalog;
        CREATE TABLE events (id bigint) USING lakehouse WITH (write_mode='staged');
        GRANT INSERT ON events TO {role};
        CREATE TABLE legacy (id bigint) USING lakehouse WITH (write_mode='staged');
        SELECT lakehouse.attach_table('legacy', 'fixture.legacy', 'capture_catalog');
    """)
    assert sql(db, "SELECT extversion FROM pg_extension WHERE extname='lakehouse'") == "1.6"
    sql(db, adapter)
    if not baseline:
        assert sql(db, "SELECT extversion FROM pg_extension WHERE extname='lakehouse'") == "1.7"
        sql(db, f"""
            DROP USER MAPPING FOR {role} SERVER capture_catalog;
            REVOKE USAGE ON FOREIGN SERVER capture_catalog FROM {role};
            GRANT USAGE ON SCHEMA lakehouse TO {role};
            GRANT SELECT ON events TO {role};
        """)
        check_cow_cutover(db, requests, catalog_replies)
    failed = []

    def check(gate, actual, expected):
        if actual != expected:
            failed.append(gate)
            print(f"FAIL {gate}: got {actual!r}, expected {expected!r}")

    def seal(rows):
        return json.loads(sql(db, f"""
            SELECT jsonb_agg(jsonb_build_array(
                s.seg_id, s.row_count,
                (s.payload_digest IS NOT NULL AND s.payload_digest = sha256(s.payload)),
                jsonb_build_array(s.authority_id, s.table_uuid)
                  || capture_fixture.provenance(s.seg_id))
                ORDER BY s.seg_id)
            FROM {SEG} s
            WHERE convert_from(s.payload, 'UTF8')::jsonb = '{json.dumps(rows)}'::jsonb
              AND s.published IS FALSE;
        """) or "null")

    def require_capture(gate, binding, values):
        matches = seal(values)
        assert matches is not None and len(matches) == 1, (gate, values, matches)
        seg, count, digest_ok, imprint = matches[0]
        assert count == len(values), matches
        check(gate, [digest_ok, imprint], [True, binding.imprint()])
        print(f"capture seg={seg} rows={values!r} generation={binding.generation}")
        return seg

    def enrolled(binding):
        sql(db, binding.enroll_sql())
        requests.clear()  # Enrollment may verify remote state. Capture may not.

    def foreground(statement, *, error=None):
        sql(db, statement, error=error)
        assert not requests, f"foreground requested catalog or storage: {requests!r}"

    def write(statement):
        return statement if baseline else f"SET ROLE {role}; {statement}"

    foreground("INSERT INTO legacy VALUES (10);")
    legacy = seal([[10]])
    assert legacy and legacy[0][2:] == [False, [None] * 5], legacy
    incarnation = uuid.uuid4()
    authority = uuid.uuid4()
    first = Binding.create(incarnation, 1, "events", authority)
    if not baseline:
        sql(db, f"SET ROLE {role}; SELECT lakehouse.require_managed_capture('events');", error="42501")
        sql(db, f"SET ROLE {role}; {first.enroll_sql(provider=True)}", error="42501")
        sql(db, "SELECT lakehouse.require_managed_capture('legacy');", error="55000")
    enrolled(first)
    hostile_path = ""
    if not baseline:
        sql(db, f"""
            CREATE SCHEMA capture_trap AUTHORIZATION {role};
            SET ROLE {role};
            CREATE FUNCTION capture_trap.trap(oid, oid) RETURNS bool LANGUAGE plpgsql AS $$
            BEGIN
                RAISE EXCEPTION 'capture executed a caller-defined operator';
            END $$;
            CREATE OPERATOR capture_trap.= (FUNCTION = capture_trap.trap, LEFTARG = oid, RIGHTARG = oid);
        """)
        hostile_path = "SET search_path = capture_trap, public, pg_catalog;"
        foreground(write("SELECT id FROM events;"), error="0A000")
        foreground(write("SELECT count(*) FROM events;"), error="0A000")
        foreground(write(f"SELECT lakehouse.revoke_managed_capture('events', '{first.binding}', 1);"),
                   error="42501")
    foreground(write(hostile_path + """
        BEGIN; INSERT INTO events VALUES (20), (20);
        SAVEPOINT s; INSERT INTO events VALUES (98); ROLLBACK TO s; COMMIT;
        BEGIN; INSERT INTO events VALUES (99); ROLLBACK;
    """))
    assert seal([[98]]) is None and seal([[99]]) is None
    original = require_capture("old-imprint", first, [[20], [20]])
    for direct_sql in (
        f"SELECT seg_id FROM {SEG}",
        f"INSERT INTO {SEG} (relid,payload) VALUES ('events'::regclass, '[[79]]')",
        f"UPDATE {SEG} SET row_count=0",
        f"DELETE FROM {SEG}",
    ):
        foreground(f"SET ROLE {role}; {direct_sql};", error="42501")
    if baseline:
        foreground(f"SET ROLE {role}; INSERT INTO events VALUES (21);", error="42501")
        failed.append("writer-acl")
    else:
        foreground(f"SET ROLE {role}; BEGIN; INSERT INTO events VALUES (21); ROLLBACK;")
    assert seal([[21]]) is None and seal([[79]]) is None

    operation = str(uuid.uuid4())
    original_claim = claim(operation, str(original), "events", str(first.authority), str(first.table))
    if baseline:
        sql(db, original_claim, error="55000")
        failed.append("claim-original")
    else:
        digest = sql(db, original_claim)
        expected = sql(db, f"""
            SELECT encode(sha256(int8send(seg_id) || int4send(row_count) || payload_digest), 'hex')
            FROM {SEG} WHERE seg_id={original}
        """)
        assert digest == expected, (digest, expected)

    foreground(write("INSERT INTO events VALUES (30);"))
    late = require_capture("late-imprint", first, [[30]])
    assert late != original
    members = sql(db, f"SELECT seg_id FROM {MEM} WHERE operation_id='{operation}' ORDER BY seg_id")
    assert members == ("" if baseline else str(original)), members

    if not baseline:
        # The open operation reserves the target. The late segment is still
        # independent, but a second operation cannot claim the target yet.
        sql(db, claim(str(uuid.uuid4()), str(late), "events",
                      str(first.authority), str(first.table)), error="40001")

    old_imprint = (seal([[20], [20]]), seal([[30]]))
    legacy_reattach = "SELECT lakehouse.attach_table('events', 'fixture.unapproved', 'capture_catalog');"
    if baseline:
        foreground(legacy_reattach)
        assert sql(db, """
            SELECT identity FROM lakehouse.managed_tables WHERE relid='events'::regclass
        """) == "fixture.unapproved"
        failed.append("legacy-reattach")
        foreground("SELECT lakehouse.attach_table('events', 'fixture.events', 'capture_catalog');")
    else:
        foreground(legacy_reattach, error="55000")

    sql(db, f"SELECT capture_fixture.revoke('events', '{first.binding}', {first.generation});")
    requests.clear()
    if baseline:
        foreground("INSERT INTO events VALUES (40);")
        assert seal([[40]]) is not None
        failed.append("revoked-insert")
    else:
        foreground(write("INSERT INTO events VALUES (40);"), error="55000")
        assert seal([[40]]) is None
        foreground(write(f"""
            DO $$
            BEGIN
                BEGIN
                    INSERT INTO events VALUES (40);
                    RAISE EXCEPTION 'revoked capture succeeded';
                EXCEPTION WHEN SQLSTATE '55000' THEN NULL;
                END;
                ASSERT current_user = '{role}';
                BEGIN
                    PERFORM seg_id FROM {SEG};
                    RAISE EXCEPTION 'capture left the ledger role active';
                EXCEPTION WHEN insufficient_privilege THEN NULL;
                END;
            END $$;
        """))

    replacement = Binding.create(incarnation, 2, "replacement", authority)
    enrolled(replacement)
    foreground(write("INSERT INTO events VALUES (50);"))
    require_capture("replacement-imprint", replacement, [[50]])
    assert (seal([[20], [20]]), seal([[30]])) == old_imprint
    sql(db, claim(str(uuid.uuid4()), str(late), "events",
                  str(replacement.authority), str(replacement.table)), error="55000")

    recreate = """
        DROP TABLE events;
        CREATE TABLE events (id bigint) USING lakehouse WITH (write_mode='staged');
    """
    # A reused name can still create an independent legacy relation.
    foreground(recreate + """
        SELECT lakehouse.attach_table('events', 'fixture.legacy_recreated', 'capture_catalog');
        INSERT INTO events VALUES (61);
    """)
    legacy_recreated = seal([[61]])
    assert legacy_recreated and len(legacy_recreated) == 1
    assert legacy_recreated[0][2:] == [False, [None] * 5], legacy_recreated

    # A different new relation opts in before supplying its enrollment.
    foreground(recreate + "SELECT capture_fixture.require_binding('events');")
    sql(db, f"GRANT INSERT ON events TO {role};")
    # The mock only records opt-in. Baseline reports its old unattached error.
    foreground(write("INSERT INTO events VALUES (60);"), error="0A000" if baseline else "55000")
    assert seal([[60]]) is None
    recreated = Binding.create(uuid.uuid4(), 1, "replacement", authority,
                               table=replacement.table)
    enrolled(recreated)
    foreground(write("INSERT INTO events VALUES (70);"))
    require_capture("recreated-imprint", recreated, [[70]])
    assert (seal([[20], [20]]), seal([[30]])) == old_imprint
    assert seal([[10]]) == legacy and seal([[61]]) == legacy_recreated
    assert not requests
    captures = json.loads(sql(db, f"""
        SELECT jsonb_agg(convert_from(payload, 'UTF8')::jsonb ORDER BY seg_id) FROM {SEG};
    """))
    expected = [[[10]], [[20], [20]], [[30]], [[50]], [[61]], [[70]]]
    if baseline:
        expected.insert(3, [[40]])
    assert captures == expected, captures
    print(f"all retained payloads={captures!r}")

    if not baseline:
        def revoke(binding):
            return f"SELECT capture_fixture.revoke('events', '{binding.binding}', {binding.generation});"

        # A write and revocation in the same transaction must fail together.
        foreground(f"BEGIN; INSERT INTO events VALUES (71); {revoke(recreated)} COMMIT;",
                   error="55000")
        assert seal([[71]]) is None
        race_transition(db, role, "INSERT INTO events VALUES (72);", revoke(recreated), "COMMIT;")
        require_capture("commit-before-revoke", recreated, [[72]])
        foreground(write("INSERT INTO events VALUES (73);"), error="55000")

        next_binding = Binding.create(recreated.incarnation, 2, "race_two", authority)
        enrolled(next_binding)
        final_binding = Binding.create(recreated.incarnation, 3, "race_three", authority)
        race_transition(
            db, role, "SAVEPOINT s; INSERT INTO events VALUES (74);",
            f"BEGIN; {revoke(next_binding)} {final_binding.enroll_sql()} COMMIT;",
            "ROLLBACK TO s;", "INSERT INTO events VALUES (75);\n",
        )
        assert seal([[74]]) is None and seal([[75]]) is None
        foreground(write("INSERT INTO events VALUES (76);"))
        require_capture("capture-after-savepoint-race", final_binding, [[76]])
        print("capture races proved commit-before-revoke and stale-savepoint refusal")

        # A second INSERT must refuse the new layout before touching the
        # first INSERT's staged array. Rolling back the DDL restores it.
        changed_slot = sql(db, r"""
            BEGIN; INSERT INTO events VALUES (771);
            SAVEPOINT s; ALTER TABLE events ADD COLUMN extra varchar(9);
            \set ON_ERROR_STOP off
            INSERT INTO events VALUES (772, 'drift');
            \echo :SQLSTATE
            \set ON_ERROR_STOP on
            ROLLBACK TO s;
            INSERT INTO events VALUES (773); COMMIT;
        """)
        assert changed_slot == "55000", changed_slot
        require_capture("schema-savepoint", final_binding, [[771], [773]])
        assert seal([[772, "drift"]]) is None
        assert not requests, requests

        # A changed source shape needs a fresh schema approval.
        foreground("BEGIN; INSERT INTO events VALUES (77); "
                   "ALTER TABLE events ADD COLUMN extra varchar(9); COMMIT;", error="55000")
        assert seal([[77]]) is None
        foreground("ALTER TABLE events ADD COLUMN extra varchar(9);")
        foreground(write("INSERT INTO events VALUES (77, 'abc');"), error="55000")
        assert seal([[77, "abc"]]) is None
        sql(db, revoke(final_binding))
        shaped_binding = Binding.create(recreated.incarnation, 4, "shaped", authority)
        enrolled(shaped_binding)
        foreground(write(r"INSERT INTO events VALUES (78, E'a\\bé');"))
        require_capture("reenrolled-schema", shaped_binding, [[78, "a\\bé"]])
        foreground("ALTER TABLE events ALTER COLUMN extra TYPE varchar(12);")
        foreground(write("INSERT INTO events VALUES (77, 'abc');"), error="55000")
        assert not requests, requests
        if restart_dir:
            verify_restart(db, restart_dir)
    return failed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--expect-baseline-red", action="store_true")
    modes.add_argument("--adapter", type=Path, metavar="SQL")
    modes.add_argument("--provider", action="store_true")
    parser.add_argument("--restart-fixture", type=Path, metavar="PGDATA")
    args = parser.parse_args()
    if args.expect_baseline_red and args.restart_fixture:
        parser.error("--restart-fixture requires the upgraded capture provider")
    if not __debug__:
        raise RuntimeError("the capture fixture requires Python assertions")

    suffix = uuid.uuid4().hex[:12]
    db, role = f"capture_fixture_{suffix}", f"capture_writer_{suffix}"
    requests = []
    catalog_replies = {}

    class Recorder(BaseHTTPRequestHandler):
        def respond(self):
            requests.append((self.command, self.path))
            path = urlsplit(self.path).path
            content = ({} if catalog_replies and self.command == "PUT"
                       and path.startswith("/fixture-bucket/cow_events/")
                       else catalog_replies.get((self.command, path)))
            if content is None:
                self.send_error(503)
                return
            body = json.dumps(content).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Last-Modified", self.date_time_string())
            self.send_header("ETag", '"fixture"')
            self.end_headers()
            self.wfile.write(body)

        do_GET = do_HEAD = do_PUT = do_POST = respond

        def log_message(self, *_args):
            pass

    baseline = args.expect_baseline_red
    adapter_path = (Path(__file__).with_name("managed-capture-binding-adapter.sql")
                    if args.provider else args.adapter)
    adapter = BASELINE_ADAPTER if baseline else adapter_path.read_text(encoding="utf-8")
    sql("postgres", f"CREATE DATABASE {db}")
    try:
        with ThreadingHTTPServer(("127.0.0.1", 0), Recorder) as server:
            thread = threading.Thread(target=server.serve_forever)
            thread.start()
            try:
                failed = exercise(db, adapter, baseline,
                                  f"http://127.0.0.1:{server.server_port}", requests, catalog_replies, role,
                                  args.restart_fixture)
            finally:
                server.shutdown()
                thread.join()
    finally:
        sql("postgres", f"DROP DATABASE {db} WITH (FORCE); DROP ROLE IF EXISTS {role};")

    print(f"capture gates={failed!r}; foreground HTTP requests=0")
    assert failed == (EXPECTED_RED if baseline else []), failed


if __name__ == "__main__":
    main()
