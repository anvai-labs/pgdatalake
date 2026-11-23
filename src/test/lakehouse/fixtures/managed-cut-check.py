"""Exercise compatible-pin decisions using live PG visibility and pinned Trino rows."""

import argparse
from contextlib import contextmanager
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import subprocess
from threading import Thread
from importlib import import_module
from uuid import uuid4

from publication_fixture import HDR, MEM, SEG, sql

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


@contextmanager
def dropped_catalog_reply(route):
    # Use the real catalog for metadata and commit. Withhold success only
    # after the exact allowed POST has reached it and was accepted.
    posts, lookups = [], []

    class Gateway(BaseHTTPRequestHandler):
        def do_GET(self):
            purpose = self.headers.get("Authorization")
            lookups.append((self.path, purpose))
            if self.path != route:
                self.send_error(404)
                return
            if purpose not in ("Bearer fixture-send", "Bearer fixture-lookup"):
                self.send_error(403)
                return
            self.reply(*trino_fixture.catalog_request("GET", self.path))

        def do_POST(self):
            if self.path != route or self.headers.get("Authorization") != "Bearer fixture-send":
                posts.append((403, b""))
                self.send_error(403)
                return
            payload = self.rfile.read(int(self.headers["Content-Length"]))
            if posts:
                posts.append((409, payload))
                self.send_error(409)
                return
            status, _ = trino_fixture.catalog_request(
                "POST", self.path, payload, {"Content-Type": "application/json"})
            posts.append((status, payload))
            self.reply(504 if 200 <= status < 300 else status, b"")

        def reply(self, status, body):
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Gateway) as gateway:
        thread = Thread(target=gateway.serve_forever)
        thread.start()
        try:
            yield f"http://127.0.0.1:{gateway.server_port}/catalog", posts, lookups
        finally:
            gateway.shutdown()
            thread.join()


def verified_membership(db, operation):
    document = sql(db, f"""
        SELECT json_build_object('pin', c.verified_snapshot_id::text,
                                 'binding', c.capture_binding_id,
                                 'source', json_build_array(c.authority_id, c.table_uuid, b.ref_name),
                                 'members', json_agg(s.seg_id ORDER BY s.seg_id))
        FROM {HDR} c JOIN lakehouse.publication_binding_descriptors b ON b.binding_id=c.capture_binding_id
            JOIN {MEM} m USING (operation_id)
            JOIN {SEG} s USING (seg_id)
        WHERE c.operation_id='{operation}' AND c.verified_snapshot_id IS NOT NULL
            AND s.published AND s.published_snapshot_id=c.verified_snapshot_id
        GROUP BY c.operation_id, b.ref_name
    """)
    assert document, "worker left no verified membership"
    return json.loads(document)


def run_worker(worker, db, operation, approval, *, lookup_only=False, error=None,
               member_byte_budget=None):
    request = {"database": db, "operation_id": operation, "admitted_transport": True,
               "lookup_only": lookup_only, "approved_target": approval,
               "member_byte_budget": member_byte_budget}
    result = subprocess.run([worker], input=json.dumps(request), timeout=35, text=True, capture_output=True)
    if error:
        assert result.returncode and error in result.stderr, (result.returncode, result.stderr)
        return None
    result.check_returncode()
    return result.stdout.strip()


def exercise(db, namespace, role, owner, worker, expect_runtime_red):
    relation = f"iceberg.{namespace}.events"
    trino(f"CREATE TABLE {relation} (id bigint) WITH (format_version=2)")
    trino(f"INSERT INTO {relation} VALUES (10)")
    history = f'SELECT snapshot_id FROM iceberg.{namespace}."events$refs" WHERE name=\'main\''
    parent = trino(history)
    route, _, metadata = trino_fixture.enrolled_metadata(namespace)
    table = metadata["table-uuid"]
    root = metadata["location"]
    authority, incarnation, binding = (str(uuid4()) for _ in range(3))
    source = (authority, table, "main")
    base = certify(source, parent, set(), namespace, [10])

    prefix = route.split("/namespaces/", 1)[0].removeprefix("/catalog/v1").lstrip("/")
    with dropped_catalog_reply(route) as (endpoint, posts, lookups):
        approval = trino_fixture.approved_target(namespace, endpoint, prefix, authority, table,
                                                  root.rstrip("/"), binding, incarnation)
        check_visibility(db, role, owner, worker, approval, base, history, (route, posts, lookups))

    if expect_runtime_red:
        sql(db, f"SET ROLE {role}; SELECT count(id) FROM events", error="0A000")
        print("bound SQL overlay remains gated with 0A000", flush=True)


def check_visibility(db, role, owner, worker, approval, base, history, gateway):
    route, posts, lookups = gateway
    source, parent, _, _ = base
    authority, table, _ = source
    namespace, = approval["namespace_components"]
    endpoint, prefix = approval["catalog_endpoint"], approval["catalog_prefix"]
    root, binding = approval["storage_root"], approval["binding_id"]
    incarnation = approval["incarnation_id"]
    sql(db, f"""
        CREATE EXTENSION lakehouse;
        ALTER EXTENSION lakehouse UPDATE TO '1.10';
        CREATE SERVER cut_catalog FOREIGN DATA WRAPPER lakehouse_fdw OPTIONS
            (endpoint '{endpoint}', warehouse 'smoke',
             s3_endpoint 'http://127.0.0.1:9000');
        CREATE TABLE events(id bigint) USING lakehouse WITH (write_mode='staged');
        GRANT INSERT, SELECT ON events TO {role};
        SELECT lakehouse.enroll_managed_capture('events'::regclass, '{namespace}.events',
            'cut_catalog', '{incarnation}', '{binding}', 1, '{authority}', '{table}', '{root}', 0, 0);
        SELECT lakehouse.grant_publication_send('{binding}', '{prefix}', 'us-east-1');
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

        operation = str(uuid4())
        candidates = sql(db, "SELECT segment_id, binding_id, eligible FROM "
                         "lakehouse.discover_managed_publication_segments(NULL, 16) ORDER BY segment_id")
        assert candidates == f"{first_id}|{binding}|t\n{late_id}|{binding}|t", candidates
        discover = ("SELECT operation_id, binding_id, mode FROM "
                    "lakehouse.discover_managed_publication_claims(NULL, 16)")
        sql(db, f"SELECT lakehouse.claim_managed_publication('{operation}', ARRAY[{first_id},{late_id}]::bigint[])")
        assert sql(db, discover) == f"{operation}|{binding}|send"
        run_worker(worker, db, operation, approval, member_byte_budget=1,
                   error="managed publication exceeds its member budget")
        assert not posts and not lookups
        run_worker(worker, db, operation, approval | {"storage_root": root.rstrip("/") + "-wrong"},
                   error="publication credentials lack matching approval")
        assert not posts and not lookups
        assert sql(db, f"SELECT count(*) FROM lakehouse.publication_attempts WHERE operation_id='{operation}'") == "0"
        assert run_worker(worker, db, operation, approval) == "hold dispatch_unknown"
        assert len(posts) == 1 and 200 <= posts[0][0] < 300
        child = trino(history)
        assert child != parent
        assert sql(db, f"""
            SELECT a.expected_parent = {parent}
                   AND a.proposed_snapshot_id = {child}
                   AND encode(a.proposal_digest, 'hex') = '{hashlib.sha256(posts[0][1]).hexdigest()}'
                   AND c.verified_snapshot_id IS NULL
            FROM lakehouse.publication_attempts a JOIN {HDR} c USING (operation_id)
            WHERE a.operation_id='{operation}'
        """) == "t"
        _, _, head = trino_fixture.enrolled_metadata(namespace)
        snapshot = next(s for s in head["snapshots"] if str(s["snapshot-id"]) == child)
        assert head["table-uuid"] == table and str(snapshot["parent-snapshot-id"]) == parent
        assert ledger_snapshot(fresh)[1] == fresh_view[1]
        assert sql(db, discover) == f"{operation}|{binding}|lookup"

        assert run_worker(worker, db, operation, approval, lookup_only=True) == f"verified {child} 2"
        receipt = verified_membership(db, operation)
        assert receipt == {"pin": child, "binding": binding, "source": list(source),
                           "members": [first_id, late_id]}, receipt
        assert len(posts) == 1
        assert lookups == [(route, "Bearer fixture-send"), (route, "Bearer fixture-lookup")], lookups
        assert sql(db, discover) == ""
        print(json.dumps({"case": "verified-worker-cut", "parent": parent, "pin": child,
                          "operation": operation, "members": receipt["members"],
                          "catalog_posts": len(posts), "catalog_gets": len(lookups),
                          "wrong_approval_catalog_requests": 0, "overbudget_catalog_requests": 0}, sort_keys=True),
              flush=True)
        published = certify(tuple(receipt["source"]), receipt["pin"], set(receipt["members"]),
                            namespace, [10, 20, 20])
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

        assert ledger_snapshot(before) == first_view
        assert ledger_snapshot(between) == second_view
        completed = ledger_snapshot(fresh)
        assert completed[1] == {first_id: ([20], True), late_id: ([20], True)}, completed
        check("after-local-completion", (child, [], [10, 20, 20]), source, candidates, completed)
        check("retained-parent-after-completion", (parent, [first_id, late_id], [10, 20, 20]), source,
              [base], completed)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", required=True, type=Path)
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
            exercise(namespace, namespace, role, owner, args.worker.resolve(), args.expect_runtime_red)
        finally:
            sql("postgres", f"DROP DATABASE {namespace}")
            sql("postgres", f"DROP ROLE IF EXISTS {role}")
    finally:
        trino(f"DROP TABLE IF EXISTS iceberg.{namespace}.events")
        trino(f"DROP SCHEMA iceberg.{namespace}")


if __name__ == "__main__":
    main()
