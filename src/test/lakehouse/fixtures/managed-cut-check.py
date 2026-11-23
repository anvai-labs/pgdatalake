"""Exercise compatible-pin decisions using live PG visibility and pinned Trino rows."""

import argparse
from contextlib import closing, contextmanager
import hashlib
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import subprocess
from threading import Thread
from importlib import import_module
from urllib.parse import urlsplit
from uuid import uuid4

from publication_fixture import HDR, MEM, SEG, sql

trino_fixture = import_module("bound-publication-restart")
scoped_read = import_module("scoped-read-check")
login = scoped_read.login
trino = trino_fixture.trino


EXPIRED = object()
SCOPE_FIELDS = ("authority_id", "table_uuid", "ref_name", "incarnation_id", "storage_root")
MAX_CATALOG_BYTES = 2 * 1024 * 1024


def select_cut(worker, source, catalog, candidates, visible, *, pending_head=None,
               reader_object_endpoint=None, load_overlay=None):
    def membership(included):
        if included is EXPIRED:
            return {"proof": "expired"}
        if included is None:
            return {"proof": "unknown"}
        return {"proof": "complete", "included": [str(segment) for segment in sorted(included)]}

    request = {
        "scope": dict(zip(SCOPE_FIELDS, source, strict=True)),
        "catalog": catalog,
        "candidates": [
            {"scope": dict(zip(SCOPE_FIELDS, scope, strict=True)), "pin": pin,
             "membership": membership(included)}
            for scope, pin, included, _ in candidates
        ],
        "visible": [
            {"segment_id": str(seg_id), "payload_retained": payload is not None}
            for seg_id, (payload, _) in visible.items()
        ],
    }
    if pending_head is not None:
        request.update(pending_head=pending_head, reader_object_endpoint=reader_object_endpoint)
    result = subprocess.run([worker, "--select-managed-cut"], input=json.dumps(request),
                            check=True, capture_output=True, text=True, timeout=10)
    cut = json.loads(result.stdout)
    if "error" in cut:
        return cut["error"]
    pin = cut["pin"]
    overlay_ids = [int(seg_id) for seg_id in cut["overlay_segment_ids"]]
    base, = (rows for _, candidate, _, rows in candidates if candidate == pin)
    rows = list(base)
    payloads = load_overlay(overlay_ids) if load_overlay is not None else [
        visible[seg_id][0] for seg_id in overlay_ids
    ]
    for payload in payloads:
        rows.extend(payload)
    return pin, overlay_ids, sorted(rows)


def ledger_snapshot(session, binding):
    data, = session(f"""
        SELECT json_build_array(pg_current_snapshot()::text,
            coalesce((SELECT json_agg(json_build_array(seg_id,
                convert_from(payload, 'UTF8')::json, published,
                encode(sha256(payload), 'hex')) ORDER BY seg_id)
                FROM {SEG} WHERE relid='events'::regclass), '[]'::json),
            coalesce((SELECT json_agg(json_build_array(segment_id, row_count,
                encode(payload_digest, 'hex')) ORDER BY segment_id)
                FROM lakehouse.get_managed_read_visible_segments('events', '{binding}')), '[]'::json))
    """)
    pg_snapshot, segments, evidence = json.loads(data)
    assert evidence == [[seg_id, len(payload), digest] for seg_id, payload, _, digest in segments], evidence
    visible = {seg_id: ([row[0] for row in payload], published)
               for seg_id, payload, published, _ in segments}
    return pg_snapshot, visible


def ledger_additions(db, binding, *pins):
    return json.loads(sql(db, f"""
        SELECT json_object_agg(snapshot_id::text, additions)
        FROM lakehouse.get_managed_read_additions('events', '{binding}', ARRAY[{','.join(pins)}]::bigint[])
    """))


def retained_pending_head(db, binding, child, parent):
    value = sql(db, f"""
        BEGIN READ ONLY;
        SELECT lakehouse.get_managed_read_pending_head('events', '{binding}', {child}, {parent});
        ROLLBACK;
    """)
    return json.loads(value) if value else None


def overlay_call(binding, ids, *, target="events", max_rows=65536, max_bytes=8388608):
    requested = f"ARRAY[{','.join(str(i) for i in ids)}]::bigint[]"
    return (f"lakehouse.load_managed_read_overlay('{target}', '{binding}', "
            f"{requested}, {max_rows}, {max_bytes})")


def overlay_bytes(session, binding, ids):
    result, = session(f"""
        SELECT coalesce(json_agg(json_build_array(segment_id, row_count, encode(payload_digest, 'hex'),
               encode(payload, 'hex')) ORDER BY segment_id), '[]'::json)
        FROM {overlay_call(binding, ids)}
    """)
    rows = json.loads(result)
    assert [row[0] for row in rows] == ids, rows
    for _, row_count, digest, body in rows:
        payload = bytes.fromhex(body)
        assert hashlib.sha256(payload).hexdigest() == digest
        assert len(json.loads(payload)) == row_count
    return rows


def load_overlay(session, binding, ids):
    return [[row[0] for row in json.loads(bytes.fromhex(body))]
            for _, _, _, body in overlay_bytes(session, binding, ids)]


def bounded_table_get(address, route, bearer=None):
    headers = {} if bearer is None else {"Authorization": f"Bearer {bearer}"}
    with closing(HTTPConnection(address, timeout=10)) as conn:
        conn.request("GET", route, headers=headers)
        response = conn.getresponse()
        body = response.read(MAX_CATALOG_BYTES + 1)
    if len(body) > MAX_CATALOG_BYTES:
        return 413, b""
    return response.status, body


@contextmanager
def reader_manifest_probe(manifest):
    requests = []
    state = {"corrupt": False}
    expected = "/" + manifest.removeprefix("s3://")

    def reply(method, path, headers):
        requests.append((method, path, headers.get("Range")))
        if method != "GET" or path != expected:
            return 403, [], b""
        status, returned_headers, data = scoped_read.upstream("127.0.0.1:9000", method, path, dict(headers))
        if state["corrupt"] and status == 206 and data:
            data = bytes([data[0] ^ 1]) + data[1:]
        return status, returned_headers, data

    with scoped_read.recorder(reply) as endpoint:
        yield endpoint, requests, state


def certify(scope, pin, included, namespace, expected):
    reference = trino(f"SELECT id FROM iceberg.{namespace}.events FOR VERSION AS OF {pin} ORDER BY id")
    rows = [int(value) for value in reference.splitlines()]
    assert rows == expected, (pin, rows)
    return scope, pin, included, rows


def check(worker, name, expected, scope, catalog, candidates, observation, **proof):
    pg_snapshot, visible = observation
    result = select_cut(worker, scope, catalog, candidates, visible, **proof)
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
            if purpose not in ("Bearer fixture-send", "Bearer fixture-lookup", "Bearer fixture-cut"):
                self.send_error(403)
                return
            if purpose == "Bearer fixture-cut":
                self.reply(*bounded_table_get("127.0.0.1:8181", self.path))
            else:
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
                                 'source', json_build_array(c.authority_id, c.table_uuid, b.ref_name,
                                                            b.incarnation_id, b.storage_root),
                                 'members', json_agg(s.seg_id ORDER BY s.seg_id))
        FROM {HDR} c JOIN lakehouse.publication_binding_descriptors b ON b.binding_id=c.capture_binding_id
            JOIN {MEM} m USING (operation_id)
            JOIN {SEG} s USING (seg_id)
        WHERE c.operation_id='{operation}' AND c.verified_snapshot_id IS NOT NULL
            AND s.published AND s.published_snapshot_id=c.verified_snapshot_id
        GROUP BY c.operation_id, b.ref_name, b.incarnation_id, b.storage_root
    """)
    assert document, "worker left no verified membership"
    return json.loads(document)


def run_worker(worker, db, operation, approval, hint, *, error=None, member_byte_budget=None):
    request = {"database": db, "operation_id": operation, "admitted_transport": True,
               "dispatch_hint": hint, "approved_target": approval,
               "member_byte_budget": member_byte_budget}
    result = subprocess.run([worker], input=json.dumps(request), timeout=35, text=True, capture_output=True)
    if error:
        assert result.returncode and error in result.stderr, (result.returncode, result.stderr)
        return None
    result.check_returncode()
    return result.stdout.strip()


def run_native_lookup(db, role, operation, binding, generation, child, posts, lookups):
    function = trino_fixture.NATIVE_LOOKUP
    call = f"SET ROLE {role}; SELECT {function}('{operation}', {generation});"
    reads_before = len(lookups)
    trino_fixture.install_native_lookup_role(db, role, upgrade=False)
    sql(db, call, error="42501")
    sql(db, f"GRANT EXECUTE ON FUNCTION {function}(uuid, bigint) TO {role};")
    sql(db, call, error="42501")
    trino_fixture.approve_native_lookup(db, role, binding, expired=True)
    sql(db, call, error="42501")
    assert sql(db, f"SELECT owner_generation FROM {HDR} WHERE operation_id='{operation}'") == str(generation)
    assert len(posts) == 1 and len(lookups) == reads_before, "unapproved or expired lookup touched the provider"

    sql(db, f"""
        UPDATE lakehouse.publication_lookup_grants
           SET expires_at=clock_timestamp() + interval '90 seconds'
         WHERE binding_id='{binding}';
        SELECT lakehouse.revoke_publication_send('{binding}');
    """)
    sql(db, f"SET ROLE {role}; SELECT {function}('{operation}', {generation - 1});", error="40001")
    assert len(posts) == 1 and len(lookups) == reads_before, "stale native lookup touched the provider"
    result = json.loads(sql(db, call))
    assert result == {"status": "verified", "owner_generation": str(generation + 1),
                      "snapshot_id": child, "members": 2}, result
    print(json.dumps({"case": "native_lookup_reconciled", "snapshot_id": child,
                      "catalog_posts": len(posts), "catalog_gets": len(lookups),
                      "send_grant_revoked": True, "no_mapping_or_expired_sqlstate": "42501",
                      "stale_sqlstate": "40001"}, sort_keys=True), flush=True)


def exercise(db, namespace, role, owner, worker, expect_runtime_red, native_lookup):
    relation = f"iceberg.{namespace}.events"
    trino(f"CREATE TABLE {relation} (id bigint) WITH (format_version=2)")
    trino(f"INSERT INTO {relation} VALUES (10)")
    history = f'SELECT snapshot_id FROM iceberg.{namespace}."events$refs" WHERE name=\'main\''
    parent = trino(history)
    route, _, metadata = trino_fixture.enrolled_metadata(namespace)
    table = metadata["table-uuid"]
    root = metadata["location"]
    authority, incarnation, binding = (str(uuid4()) for _ in range(3))
    source = (authority, table, "main", incarnation, root.rstrip("/"))
    base = certify(source, parent, set(), namespace, [10])

    prefix = route.split("/namespaces/", 1)[0].removeprefix("/catalog/v1").lstrip("/")
    with dropped_catalog_reply(route) as (endpoint, posts, lookups):
        approval = trino_fixture.approved_target(namespace, endpoint, prefix, authority, table,
                                                  root.rstrip("/"), binding, incarnation)
        check_visibility(db, role, owner, worker, approval, base, history, (route, posts, lookups), native_lookup)

    if expect_runtime_red:
        sql(db, f"SET ROLE {role}; SELECT count(id) FROM events", error="0A000")
        print("bound SQL overlay remains gated with 0A000", flush=True)


def check_visibility(db, role, owner, worker, approval, base, history, gateway, native_lookup):
    route, posts, lookups = gateway
    source, parent, _, _ = base
    authority, table, _, incarnation, _ = source
    namespace, = approval["namespace_components"]
    endpoint, prefix = approval["catalog_endpoint"], approval["catalog_prefix"]
    root, binding = approval["storage_root"], approval["binding_id"]
    assert incarnation == approval["incarnation_id"]
    enroll = f"""SELECT lakehouse.enroll_managed_capture_with_anchor(
        'events', '{namespace}.events', 'cut_catalog', '{incarnation}', '{binding}',
        '{authority}', '{table}', '{root}', 0, 0, {{anchor}})"""
    prior_binding, prior_lineage = (str(uuid4()) for _ in range(2))
    sql(db, f"""
        CREATE EXTENSION lakehouse VERSION '1.18';
        DROP EXTENSION lakehouse;
        CREATE EXTENSION lakehouse;
        ALTER EXTENSION lakehouse UPDATE TO '1.14';
        CREATE SERVER cut_catalog FOREIGN DATA WRAPPER lakehouse_fdw OPTIONS
            (endpoint '{endpoint}', warehouse 'smoke',
             s3_endpoint 'http://127.0.0.1:9000');
        CREATE TABLE prior_events(id bigint) USING lakehouse WITH (write_mode='staged');
        SELECT lakehouse.enroll_managed_capture('prior_events', '{namespace}.events',
            'cut_catalog', '{prior_lineage}', '{prior_binding}', 1, '{authority}', '{table}', '{root}', 0, 0);
        CREATE TABLE events(id bigint) USING lakehouse WITH (write_mode='staged');
        GRANT INSERT, SELECT ON events TO {role};
    """)
    sql(db, enroll.format(anchor=parent), error="42883")
    sql(db, "ALTER EXTENSION lakehouse UPDATE TO '1.15'")
    assert not sql(db, f"SELECT * FROM lakehouse.get_managed_read_scope('prior_events', '{prior_binding}')")
    sql(db, f"SELECT lakehouse.get_managed_read_visible_segments('prior_events', '{prior_binding}')",
        error="55000")
    sql(db, enroll.format(anchor=0), error="22023")
    sql(db, f"""
        BEGIN;
        INSERT INTO {SEG}(relid, payload, row_count, published)
            VALUES ('events'::regclass, convert_to('[[12]]', 'UTF8'), 1, true);
        {enroll.format(anchor=parent)};
    """, error="55000")
    assert not sql(db, "SELECT active_binding_id FROM lakehouse.managed_capture_state WHERE relid='events'::regclass")
    sql(db, enroll.format(anchor=parent))
    sql(db, enroll.format(anchor=parent), error="55000")
    scope = json.loads(sql(db, f"""
        SELECT json_build_array(authority_id, table_uuid, ref_name, incarnation_id,
                                storage_root, anchor_snapshot::text)
        FROM lakehouse.get_managed_read_scope('events', '{binding}')
    """))
    assert scope == [*source, parent], scope
    anchor = scope[-1]
    assert ledger_additions(db, binding, parent) == {parent: []}
    for statement in ("UPDATE lakehouse.managed_read_anchors SET snapshot_id=1",
                      "DELETE FROM lakehouse.managed_read_anchors",
                      "TRUNCATE lakehouse.managed_read_anchors"):
        sql(db, statement, error="55000")
    for evidence in (f"lakehouse.get_managed_read_scope('events', '{binding}')",
                     f"lakehouse.get_managed_read_visible_segments('events', '{binding}')",
                     f"lakehouse.get_managed_read_additions('events', '{binding}', ARRAY[{parent}])",
                     "lakehouse.managed_read_anchors"):
        sql(db, f"SET ROLE {role}; SELECT * FROM {evidence}", error="42501")
    for invalid in ("NULL::bigint[]", "'{}'::bigint[]", f"ARRAY[{parent},{parent}]",
                    f"ARRAY[[{parent}],[{parent}]]", f"array_fill({parent}::bigint, ARRAY[513])"):
        sql(db, f"SELECT lakehouse.get_managed_read_additions('events', '{binding}', {invalid})", error="22023")
    sql(db, "ALTER EXTENSION lakehouse UPDATE TO '1.17'")
    sql(db, f"SELECT * FROM {overlay_call(binding, [])}", error="42883")
    sql(db, "ALTER EXTENSION lakehouse UPDATE TO '1.18'")
    for accessor in (overlay_call(binding, []),
                     f"lakehouse.get_managed_read_pending_head('events', '{binding}', 2, 1)"):
        sql(db, f"SET ROLE {role}; SELECT * FROM {accessor}", error="42501")
    assert not sql(db, f"SELECT * FROM {overlay_call(binding, [])}")
    sql(db, f"SELECT * FROM {overlay_call(binding, [], target='prior_events')}", error="55000")
    sql(db, f"SELECT lakehouse.get_managed_read_pending_head('prior_events', '{binding}', 2, 1)", error="55000")
    sql(db, f"SELECT lakehouse.grant_publication_send('{binding}', '{prefix}', 'us-east-1')")

    with login(db, owner) as before, login(db, owner) as between, login(db, owner) as fresh:
        before("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        first_view = ledger_snapshot(before, binding)
        assert first_view[1] == {}, first_view
        sql(db, f"SET ROLE {role}; INSERT INTO events VALUES (20);")
        between("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        second_view = ledger_snapshot(between, binding)
        first_id, = second_view[1]
        assert second_view[1] == {first_id: ([20], False)}, second_view
        sql(db, f"""
            BEGIN; SET LOCAL ROLE {role};
            INSERT INTO events VALUES (20);
            SAVEPOINT thrown_away; INSERT INTO events VALUES (99); ROLLBACK TO thrown_away;
            COMMIT;
            BEGIN; SET LOCAL ROLE {role}; INSERT INTO events VALUES (98); ROLLBACK;
        """)
        fresh("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        fresh_view = ledger_snapshot(fresh, binding)
        assert len(fresh_view[1]) == 2, fresh_view
        late_id, = fresh_view[1].keys() - {first_id}
        assert fresh_view[1] == {first_id: ([20], False), late_id: ([20], False)}, fresh_view

        held_middle = overlay_bytes(between, binding, [first_id])
        held_fresh = overlay_bytes(fresh, binding, [first_id, late_id])
        assert load_overlay(fresh, binding, [first_id, late_id]) == [[20], [20]]
        assert all(bytes.fromhex(row[3]) == b"[[20]]" for row in held_fresh)
        for invalid in ([0], [first_id, first_id], [late_id, first_id]):
            sql(db, f"SELECT * FROM {overlay_call(binding, invalid)}", error="22023")
        for cap in ({"max_rows": 1}, {"max_bytes": 1}):
            sql(db, f"SELECT * FROM {overlay_call(binding, [first_id, late_id], **cap)}", error="54000")
        for cap in ({"max_rows": 65537}, {"max_bytes": 8388609}):
            sql(db, f"SELECT * FROM {overlay_call(binding, [], **cap)}", error="22023")
        sql(db, f"SELECT * FROM {overlay_call(binding, [first_id, late_id + 1])}", error="55000")
        between("SAVEPOINT invisible_payload")
        between(f"SELECT * FROM {overlay_call(binding, [first_id, late_id])}", sqlstate="55000")
        between("ROLLBACK TO SAVEPOINT invisible_payload")
        assert overlay_bytes(between, binding, [first_id]) == held_middle

        operation = str(uuid4())
        candidates = sql(db, "SELECT segment_id, binding_id, eligible FROM "
                         "lakehouse.discover_managed_publication_segments(NULL, 16) ORDER BY segment_id")
        assert candidates == f"{first_id}|{binding}|t\n{late_id}|{binding}|t", candidates
        def discover(mode):
            row = sql(db, "SELECT row_to_json(c) FROM lakehouse.discover_managed_publication_claims(NULL, 16) c")
            if mode is None:
                assert not row, row
                return None
            hint = json.loads(row)
            assert (hint["operation_id"], hint["binding_id"], hint["mode"]) == (operation, binding, mode), hint
            return hint

        sql(db, f"SELECT lakehouse.claim_managed_publication('{operation}', ARRAY[{first_id},{late_id}]::bigint[])")
        old_hint = discover("send")
        run_worker(worker, db, operation, approval, old_hint, member_byte_budget=1,
                   error="managed publication exceeds its member budget")
        hint = discover("send")
        assert hint["owner_generation"] > old_hint["owner_generation"], (hint, old_hint)
        assert run_worker(worker, db, operation, approval, old_hint) == "rediscover"
        assert discover("send") == hint, "stale send wake advanced the owner"
        assert not posts and not lookups
        run_worker(worker, db, operation, approval | {"storage_root": root.rstrip("/") + "-wrong"},
                   hint, error="publication credentials lack matching approval")
        assert not posts and not lookups
        assert sql(db, f"SELECT count(*) FROM lakehouse.publication_attempts WHERE operation_id='{operation}'") == "0"
        assert run_worker(worker, db, operation, approval, discover("send")) == "hold dispatch_unknown"
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
        status, body = bounded_table_get(urlsplit(endpoint).netloc, route, "fixture-cut")
        assert status == 200
        loaded = json.loads(body)
        assert loaded["metadata-location"].startswith(root + "/metadata/")
        head = loaded["metadata"]
        snapshot = next(s for s in head["snapshots"] if str(s["snapshot-id"]) == child)
        assert head["table-uuid"] == table and str(snapshot["parent-snapshot-id"]) == parent
        observation = {key: approval[key] for key in ("authority_id", "incarnation_id", "ref_name")}
        observation.update(anchor=anchor, metadata=head)
        assert ledger_snapshot(fresh, binding)[1] == fresh_view[1]
        pending = ledger_additions(db, binding, child, parent)
        assert pending == {parent: [], child: None}, pending
        unknown = certify(source, child, pending[child], namespace, [10, 20, 20])
        candidate = retained_pending_head(db, binding, child, parent)
        assert candidate and candidate["operation_id"] == operation, candidate
        assert [int(member["segment_id"]) for member in candidate["members"]] == [first_id, late_id]
        assert fresh(f"""SELECT lakehouse.get_managed_read_pending_head(
            'events', '{binding}', {child}, {parent}) IS NULL""") == ["t"]
        fallback = (parent, [first_id, late_id], [10, 20, 20])
        manifest = snapshot["manifest-list"]
        with reader_manifest_probe(manifest) as (reader_object_endpoint, requests, probe_state):
            def check_head(name, expected, view, proof, candidates=None, session=fresh):
                check(worker, name, expected, source, observation, candidates or [unknown, base], view,
                      pending_head=proof, reader_object_endpoint=reader_object_endpoint,
                      load_overlay=lambda ids: load_overlay(session, binding, ids))

            for name, expected, view, session in (
                ("old", (parent, [], [10]), first_view, before),
                ("middle", (parent, [first_id], [10, 20]), second_view, between),
                ("fresh", (child, [], [10, 20, 20]), fresh_view, fresh),
            ):
                check_head(f"ledger-accepted-unfinalized-{name}", expected, view, candidate, session=session)
            exact_request = ("GET", "/" + manifest.removeprefix("s3://"), "bytes=0-1048576")
            assert requests == [exact_request] * 3, requests
            check_head("pending-head-missing-member", fallback, fresh_view,
                       candidate | {"members": candidate["members"][:-1]})
            check_head("pending-head-changed-proposal", fallback, fresh_view,
                       candidate | {"proposal_body": candidate["proposal_body"] + "20"})
            check_head("pending-head-changed-scope", "binding_changed", fresh_view,
                       candidate | {"scope": candidate["scope"] | {"incarnation_id": str(uuid4())}})
            check_head("pending-head-no-proved-parent", "no_compatible_pin", fresh_view, candidate,
                       [unknown, (source, parent, None, [10])])
            assert len(requests) == 3, requests
            probe_state["corrupt"] = True
            check_head("pending-head-changed-manifest", fallback, fresh_view, candidate)
            assert requests == [exact_request] * 4, requests
        assert retained_pending_head(db, binding, child, parent) == candidate
        assert ledger_snapshot(fresh, binding)[1] == fresh_view[1]
        assert ledger_additions(db, binding, child, parent) == pending
        assert len(posts) == 1
        assert lookups == [(route, "Bearer fixture-send"), (route, "Bearer fixture-cut")], lookups
        print(json.dumps({"case": "read-only-pending-head", "accepted_child": child,
                          "unfinalized_owner_unchanged": True, "reader_catalog_gets": 1,
                          "reader_manifest_range_gets": len(requests),
                          "corrupt_manifest_fallback": parent, "reader_catalog_posts": 0}),
              flush=True)
        lookup_hint = discover("lookup")
        assert run_worker(worker, db, operation, approval, lookup_hint | {"mode": "send"}) == "rediscover"
        assert discover("lookup") == lookup_hint and len(posts) == 1 and len(lookups) == 2

        if native_lookup:
            run_native_lookup(db, role + "_lookup", operation, binding, lookup_hint["owner_generation"],
                              child, posts, lookups)
        else:
            assert run_worker(worker, db, operation, approval, lookup_hint) == f"verified {child} 2"
        receipt = verified_membership(db, operation)
        assert receipt == {"pin": child, "binding": binding, "source": list(source),
                           "members": [first_id, late_id]}, receipt
        assert len(posts) == 1
        assert lookups == [(route, f"Bearer fixture-{purpose}")
                           for purpose in ("send", "cut", "lookup")], lookups
        finalized = ledger_additions(db, binding, child, parent)
        assert finalized == {parent: [], child: [first_id, late_id]}, finalized
        wrong_parent = str(next(pin for pin in range(1, 4) if str(pin) not in (parent, child)))
        unknown_edge = ledger_additions(db, binding, child, wrong_parent, parent)
        assert unknown_edge == {parent: [], wrong_parent: None, child: None}, unknown_edge
        partial = json.loads(sql(db, f"""
            BEGIN;
            DELETE FROM {MEM} WHERE operation_id='{operation}' AND seg_id={late_id};
            SELECT json_object_agg(snapshot_id::text, additions)
            FROM lakehouse.get_managed_read_additions('events', '{binding}', ARRAY[{child},{parent}]);
            ROLLBACK;
        """))
        assert partial == {parent: [], child: None}, partial
        print(json.dumps({"case": "managed-ledger-evidence", "anchor": parent, "head": child,
                          "accepted_unfinalized": pending, "finalized": finalized,
                          "partial_receipt": partial}, sort_keys=True), flush=True)
        discover(None)
        print(json.dumps({"case": "verified-worker-cut", "parent": parent, "pin": child,
                          "operation": operation, "members": receipt["members"],
                          "catalog_posts": len(posts), "catalog_gets": len(lookups),
                          "stale_wake_catalog_requests": 0, "wrong_mode_catalog_requests": 0,
                          "wrong_approval_catalog_requests": 0, "overbudget_catalog_requests": 0}, sort_keys=True),
              flush=True)
        published = certify(tuple(receipt["source"]), receipt["pin"], set(finalized[child]),
                            namespace, [10, 20, 20])
        unknown = source, child, None, published[3]
        wrong = (authority, str(uuid4()), *source[2:]), child, set(), published[3]
        expired_parent = (source, parent, EXPIRED, [])
        candidates = [published, base]

        def assert_cut(name, expected, candidates, view, catalog=observation):
            check(worker, name, expected, source, catalog, candidates, view)

        assert_cut("before-both-commits", (parent, [], [10]), candidates, first_view)
        assert_cut("mixed-batch-between-commits", (parent, [first_id], [10, 20]), candidates, second_view)
        assert_cut("missing-compatible-parent", "no_compatible_pin", [published], second_view)
        assert_cut("expired-compatible-parent", "no_compatible_pin", [published, expired_parent], second_view)
        without_parent = head | {"snapshots": [s for s in head["snapshots"] if str(s["snapshot-id"]) != parent]}
        assert_cut("provider-missing-anchor", "ancestry_unavailable", candidates, second_view,
                   observation | {"metadata": without_parent})
        assert_cut("invalid-candidate-order", "invalid_proof", [base, published], second_view)
        _, visible = second_view
        reclaimed = {seg_id: (None, published) for seg_id, (_, published) in visible.items()}
        assert_cut("missing-overlay-payload", "no_compatible_pin", candidates, (second_view[0], reclaimed))
        assert_cut("fresh-mvcc-with-finalized-evidence", (child, [], [10, 20, 20]), candidates, fresh_view)
        assert_cut("unknown-membership-no-output", "no_compatible_pin", [unknown], fresh_view)
        assert_cut("unknown-head-fallback", (parent, [first_id, late_id], [10, 20, 20]),
                   [unknown, base], fresh_view)
        assert_cut("replaced-binding-no-output", "binding_changed", [wrong, base], fresh_view)

        assert ledger_snapshot(before, binding) == first_view
        assert ledger_snapshot(between, binding) == second_view
        assert not retained_pending_head(db, binding, child, parent)
        assert overlay_bytes(between, binding, [first_id]) == held_middle
        assert overlay_bytes(fresh, binding, [first_id, late_id]) == held_fresh
        fresh("COMMIT")
        completed = ledger_snapshot(fresh, binding)
        assert completed[1] == {first_id: ([20], True), late_id: ([20], True)}, completed
        assert_cut("after-local-completion", (child, [], [10, 20, 20]), candidates, completed)
        assert_cut("retained-parent-after-completion", (parent, [first_id, late_id], [10, 20, 20]),
                   [base], completed)
        assert overlay_bytes(fresh, binding, [first_id, late_id]) == held_fresh
        print(json.dumps({"case": "read-only-overlay-payloads", "retained_segments": [first_id, late_id],
                          "held_and_fresh_payloads_match": True, "held_middle_unchanged": True,
                          "original_data_snapshot_has_pending_intent": False,
                          "pending_evidence_after_finalize": None, "over_budget_sqlstate": "54000",
                          "invisible_members_sqlstate": "55000"}, sort_keys=True), flush=True)

    def rotate(previous, generation, target_identity):
        successor = str(uuid4())
        sql(db, f"""
            SELECT lakehouse.revoke_managed_capture('events', '{previous}', {generation - 1});
            SELECT lakehouse.enroll_managed_capture('events', '{target_identity}', 'cut_catalog',
                '{incarnation}', '{successor}', {generation}, '{authority}', '{table}', '{root}', 0, 0);
        """)
        return successor

    with login(db, owner) as old_scope:
        old_scope("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        assert overlay_bytes(old_scope, binding, [first_id, late_id]) == held_fresh
        successive = rotate(binding, 2, f"{namespace}.events")
        old_scope(f"SELECT * FROM {overlay_call(successive, [first_id])}", sqlstate="55000")
    assert ledger_additions(db, successive, child, parent) == finalized
    sql(db, f"SELECT lakehouse.get_managed_read_visible_segments('events', '{binding}')", error="55000")
    sql(db, f"SELECT * FROM {overlay_call(binding, [first_id])}", error="55000")
    with login(db, owner) as successor:
        assert overlay_bytes(successor, successive, [first_id, late_id]) == held_fresh
    sql(db, f"""
        BEGIN;
        INSERT INTO {SEG}(relid, payload, row_count, authority_id, table_uuid, capture_binding_id)
        SELECT 'events'::regclass, '[[8]]', 1, '{authority}', '{table}', '{successive}'
        FROM generate_series(1, 16383);
        SELECT lakehouse.get_managed_read_visible_segments('events', '{successive}');
    """, error="54000")
    changed = rotate(successive, 3, f"{namespace}.sibling")
    restored = rotate(changed, 4, f"{namespace}.events")
    sql(db, f"SELECT lakehouse.get_managed_read_visible_segments('events', '{restored}')", error="55000")
    sql(db, f"SELECT * FROM {overlay_call(restored, [first_id])}", error="55000")
    print(json.dumps({"case": "managed-ledger-generations", "unchanged_scope_preserved": True,
                      "old_binding_rejected": True, "successor_refused_by_held_snapshot": True,
                      "route_discontinuity_rejected": True}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", required=True, type=Path)
    parser.add_argument("--expect-runtime-red", action="store_true",
                        help="assert that ordinary managed SQL remains gated")
    parser.add_argument("--native-lookup", action="store_true",
                        help="recover lost reply with the native 1.12 worker")
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
            exercise(namespace, namespace, role, owner, args.worker.resolve(),
                     args.expect_runtime_red, args.native_lookup)
        finally:
            sql("postgres", f"DROP DATABASE {namespace}")
            sql("postgres", f"DROP ROLE IF EXISTS {role}_lookup, {role}")
    finally:
        trino(f"DROP TABLE IF EXISTS iceberg.{namespace}.events")
        trino(f"DROP SCHEMA iceberg.{namespace}")


if __name__ == "__main__":
    main()
