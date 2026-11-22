"""Red/green fixture for ordinary TableAM INSERT and durable capture enrollment."""

import argparse
import json
import threading
import uuid
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from publication_fixture import MEM, SEG, claim, sql


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

    def enroll_sql(self):
        return (
            "SELECT capture_fixture.enroll('events'::regclass, "
            f"'{self.identity}', 'capture_catalog', '{self.incarnation}'::uuid, "
            f"'{self.binding}'::uuid, {self.generation}::bigint, "
            f"'{self.authority}'::uuid, '{self.table}'::uuid, '{self.root}');"
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


def exercise(db, adapter, baseline, endpoint, requests, role):
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
    sql(db, adapter)
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
    enrolled(first)
    foreground(write("""
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
    return failed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--expect-baseline-red", action="store_true")
    modes.add_argument("--adapter", type=Path, metavar="SQL")
    args = parser.parse_args()
    if not __debug__:
        raise RuntimeError("the capture fixture requires Python assertions")

    suffix = uuid.uuid4().hex[:12]
    db, role = f"capture_fixture_{suffix}", f"capture_writer_{suffix}"
    requests = []

    class Recorder(BaseHTTPRequestHandler):
        def respond(self):
            requests.append((self.command, self.path))
            self.send_error(503)

        do_GET = do_HEAD = do_PUT = do_POST = respond

        def log_message(self, *_args):
            pass

    baseline = args.expect_baseline_red
    adapter = BASELINE_ADAPTER if baseline else args.adapter.read_text(encoding="utf-8")
    sql("postgres", f"CREATE DATABASE {db}")
    try:
        with ThreadingHTTPServer(("127.0.0.1", 0), Recorder) as server:
            thread = threading.Thread(target=server.serve_forever)
            thread.start()
            try:
                failed = exercise(db, adapter, baseline,
                                  f"http://127.0.0.1:{server.server_port}", requests, role)
            finally:
                server.shutdown()
                thread.join()
    finally:
        sql("postgres", f"DROP DATABASE {db} WITH (FORCE); DROP ROLE IF EXISTS {role};")

    print(f"capture gates={failed!r}; foreground HTTP requests=0")
    assert failed == (EXPECTED_RED if baseline else []), failed


if __name__ == "__main__":
    main()
