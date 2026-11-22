"""Exercise compatible-pin decisions using live PG visibility and pinned Trino rows."""

import argparse
import json
from importlib import import_module
from uuid import uuid4

from publication_fixture import SEG, sql

trino_fixture = import_module("bound-publication-restart")
login = import_module("scoped-read-check").login
trino = trino_fixture.trino


class IncompatibleCut(ValueError):
    """No proven published pin is compatible with the local snapshot."""


def compatible_cut(source, candidates, visible):
    """Consume authorized ancestry order and complete membership from a provider.

    The source identity covers the table's entire local publication lineage.
    An empty membership is proof of absence. None means missing proof.
    """
    for scope, pin, included, base_rows in candidates:
        if scope != source:
            raise IncompatibleCut("binding_changed")
        if included is None or not included <= visible.keys():
            continue
        overlay_ids = sorted(visible.keys() - included)
        rows = list(base_rows)
        for seg_id in overlay_ids:
            rows.extend(visible[seg_id][0])
        return pin, overlay_ids, sorted(rows)
    raise IncompatibleCut("no_compatible_pin")


def ledger_snapshot(session):
    data, = session(f"""
        SELECT json_build_array(pg_current_snapshot()::text,
            coalesce(json_agg(json_build_array(seg_id, convert_from(payload, 'UTF8')::json,
                                               published) ORDER BY seg_id), '[]'::json))
        FROM {SEG} WHERE relid='events'::regclass
    """)
    pg_snapshot, segments = json.loads(data)
    visible = {seg_id: ([row[0] for row in payload], published)
               for seg_id, payload, published in segments}
    return pg_snapshot, visible


def certify(scope, pin, included, namespace, expected):
    reference = trino(f"SELECT id FROM iceberg.{namespace}.events FOR VERSION AS OF {pin} ORDER BY id")
    rows = [int(value) for value in reference.splitlines()]
    assert rows == expected, (pin, rows)
    return scope, pin, included, rows


def check(name, expected, scope, candidates, observation):
    pg_snapshot, visible = observation
    try:
        result = compatible_cut(scope, candidates, visible)
    except IncompatibleCut as exc:
        result = str(exc)
    assert result == expected, (name, result, expected, pg_snapshot, visible)
    print(json.dumps({"case": name, "pg_snapshot": pg_snapshot,
                      "visible_ids": list(visible), "result": result}, sort_keys=True), flush=True)


def exercise(db, namespace, role, owner, expect_runtime_red):
    relation = f"iceberg.{namespace}.events"
    trino(f"CREATE TABLE {relation} (id bigint) WITH (format_version=2)")
    trino(f"INSERT INTO {relation} VALUES (10)")
    history = f'SELECT snapshot_id FROM iceberg.{namespace}."events$refs" WHERE name=\'main\''
    parent = trino(history)
    _, _, metadata = trino_fixture.enrolled_metadata(namespace)
    table = metadata["table-uuid"]
    root = metadata["location"]
    authority, incarnation, binding = (str(uuid4()) for _ in range(3))
    source = (authority, table, "main")
    base = certify(source, parent, set(), namespace, [10])

    sql(db, f"""
        CREATE EXTENSION lakehouse;
        ALTER EXTENSION lakehouse UPDATE TO '1.7';
        CREATE SERVER cut_catalog FOREIGN DATA WRAPPER lakehouse_fdw OPTIONS
            (endpoint 'http://127.0.0.1:8181/catalog', warehouse 'smoke',
             s3_endpoint 'http://127.0.0.1:9000');
        CREATE TABLE events(id bigint) USING lakehouse WITH (write_mode='staged');
        GRANT INSERT, SELECT ON events TO {role};
        SELECT lakehouse.enroll_managed_capture('events'::regclass, '{namespace}.events',
            'cut_catalog', '{incarnation}', '{binding}', 1, '{authority}', '{table}', '{root}', 0, 0);
    """)
    with login(db, owner) as before, login(db, owner) as between, login(db, owner) as fresh:
        before("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        first_view = ledger_snapshot(before)
        assert first_view[1] == {}, first_view
        sql(db, f"SET ROLE {role}; INSERT INTO events VALUES (20);")
        between("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        second_view = ledger_snapshot(between)
        first_id, = second_view[1]
        assert second_view[1] == {first_id: ([20], False)}, second_view
        sql(db, f"""
            BEGIN; SET LOCAL ROLE {role};
            INSERT INTO events VALUES (20);
            SAVEPOINT thrown_away; INSERT INTO events VALUES (99); ROLLBACK TO thrown_away;
            COMMIT;
            BEGIN; SET LOCAL ROLE {role}; INSERT INTO events VALUES (98); ROLLBACK;
        """)
        fresh_view = ledger_snapshot(fresh)
        assert len(fresh_view[1]) == 2, fresh_view
        late_id, = fresh_view[1].keys() - {first_id}
        assert fresh_view[1] == {first_id: ([20], False), late_id: ([20], False)}, fresh_view

        # Trino stages the remote batch for this fixture. A synthetic certificate
        # identifies its members for the decision oracle. A runtime reader must
        # independently prove that certificate using retained publication evidence.
        trino(f"INSERT INTO {relation} VALUES (20), (20)")
        child = trino(history)
        assert child != parent
        published = certify(source, child, {first_id, late_id}, namespace, [10, 20, 20])
        unknown = source, child, None, published[3]
        wrong = (authority, str(uuid4()), "main"), child, set(), published[3]
        candidates = [published, base]

        check("before-both-commits", (parent, [], [10]), source, candidates, first_view)
        check("mixed-batch-between-commits", (parent, [first_id], [10, 20]), source,
              candidates, second_view)
        check("missing-compatible-parent", "no_compatible_pin", source, [published], second_view)
        check("accepted-before-local-completion", (child, [], [10, 20, 20]), source,
              candidates, fresh_view)
        check("unknown-membership-no-output", "no_compatible_pin", source, [unknown], fresh_view)
        check("unknown-head-fallback", (parent, [first_id, late_id], [10, 20, 20]), source,
              [unknown, base], fresh_view)
        check("replaced-binding-no-output", "binding_changed", source, [wrong, base], fresh_view)

        # Model a local completion visibility change after the catalog advances.
        sql(db, f"UPDATE {SEG} SET published=true WHERE relid='events'::regclass")
        assert ledger_snapshot(before) == first_view
        assert ledger_snapshot(between) == second_view
        completed = ledger_snapshot(fresh)
        assert completed[1] == {first_id: ([20], True), late_id: ([20], True)}, completed
        check("after-local-completion", (child, [], [10, 20, 20]), source, candidates, completed)
        check("retained-parent-after-completion", (parent, [first_id, late_id], [10, 20, 20]), source,
              [base], completed)

    if expect_runtime_red:
        sql(db, f"SET ROLE {role}; SELECT count(id) FROM events", error="0A000")
        print("bound SQL overlay remains gated with 0A000", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expect-runtime-red", action="store_true",
                        help="assert that ordinary managed SQL remains gated")
    args = parser.parse_args()
    if not __debug__:
        raise RuntimeError("the cut fixture requires Python assertions")
    namespace = "managed_cut_" + uuid4().hex
    role = namespace + "_writer"
    owner = sql("postgres", "SELECT current_user")
    trino(f"CREATE SCHEMA iceberg.{namespace}")
    try:
        sql("postgres", f"CREATE DATABASE {namespace} TEMPLATE template0 ENCODING 'UTF8' LOCALE 'C'")
        try:
            sql(namespace, f"CREATE ROLE {role}")
            exercise(namespace, namespace, role, owner, args.expect_runtime_red)
        finally:
            sql("postgres", f"DROP DATABASE {namespace}")
            sql("postgres", f"DROP ROLE IF EXISTS {role}")
    finally:
        trino(f"DROP TABLE IF EXISTS iceberg.{namespace}.events")
        trino(f"DROP SCHEMA iceberg.{namespace}")


if __name__ == "__main__":
    main()
