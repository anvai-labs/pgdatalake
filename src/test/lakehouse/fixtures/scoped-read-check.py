"""Exercise a bound S3 read with an independent catalog and object recorder."""
import argparse
import contextlib
import gzip
import http.client
import http.server
from importlib import import_module
import json
import os
import queue
import re
import secrets
import subprocess
import threading
import time
import uuid
import urllib.request

from publication_fixture import PSQL, sql

trino = import_module("stock-fdw-check").trino


@contextlib.contextmanager
def recorder(callback):
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.respond()

        def do_HEAD(self):
            self.respond()

        def respond(self):
            status, headers, data = callback(self.command, self.path, self.headers)
            self.send_response(status)
            for name, value in headers:
                if name.lower() not in ("connection", "transfer-encoding"):
                    self.send_header(name, value)
            self.end_headers()
            if self.command != "HEAD":
                try:
                    self.wfile.write(data)
                except (BrokenPipeError, ConnectionResetError):
                    # A client enforcing a response-size limit can close
                    # the connection immediately after reading the headers.
                    pass

        def log_message(self, *_args):
            pass

    with http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}"
        finally:
            server.shutdown()
            thread.join()


def catalog_identity(namespace, table="events"):
    # Only this fixture enrolls from a freshly created fixture table. Real
    # deployments take these values from their independent enrollment record.
    catalog = "http://127.0.0.1:8181/catalog"
    with urllib.request.urlopen(catalog + "/v1/config?warehouse=smoke", timeout=10) as response:
        config = json.load(response)
    prefix = (config.get("defaults", {}) | config.get("overrides", {})).get("prefix", "")
    route = f'/v1/{prefix + "/" if prefix else ""}namespaces/{namespace}/tables/{table}'
    with urllib.request.urlopen(catalog + route, timeout=10) as response:
        return prefix, route, json.load(response)


def upstream(address, method, path, headers):
    with contextlib.closing(http.client.HTTPConnection(address, timeout=10)) as conn:
        conn.request(method, path, headers=headers)
        response = conn.getresponse()
        return response.status, response.getheaders(), response.read()


def exercise(db, parent, child, prefix, route, enrolled):
    metadata_location = enrolled["metadata-location"]
    metadata = enrolled["metadata"]
    table_uuid = metadata["table-uuid"]
    root = metadata["location"]
    assert root.startswith("s3://") and metadata_location.startswith(root + "/")
    root_path = "/" + root.removeprefix("s3://")
    metadata_path = "/" + metadata_location.removeprefix("s3://")
    object_requests = []
    catalog_requests = []
    redirect_requests = []
    state = {"case": "allowed"}
    # Include valid JSON above the scoped wire and expansion budgets.
    padding = "x" * (8 * 1024 * 1024)

    def redirect_target(method, path, _headers):
        redirect_requests.append((method, path))
        return 403, [], b""

    with recorder(redirect_target) as forbidden:
        def catalog_response(method, path, _headers):
            catalog_requests.append((method, path))
            if method != "GET" or path != route:
                return 404, [], b""
            case = state["case"]
            if case == "warm-then-sibling" and len(catalog_requests) == 2:
                state["warm_requests"] = len(object_requests)
                case = "sibling-metadata"
            if case == "catalog-redirect":
                return 307, [("Location", forbidden + route)], b""
            result = enrolled | {"config": {"s3.endpoint": forbidden, "s3.region": "wrong"}}
            if case == "sibling-metadata":
                result["metadata-location"] = root + "-old/metadata/invalid.metadata.json"
            if case == "oversized-catalog":
                result["metadata"] = result["metadata"] | {"fixture-padding": padding}
            body = json.dumps(result).encode()
            return 200, [("Content-Type", "application/json"), ("Content-Length", str(len(body)))], body

        def object_response(method, path, headers):
            object_requests.append((method, path))
            if not path.startswith(root_path + "/"):
                return 403, [], b""
            case = state["case"]
            if path == metadata_path:
                if case == "storage-redirect":
                    return 307, [("Location", forbidden + metadata_path)], b""
                if case in ("changed-uuid", "changed-root", "sibling-manifest-list", "oversized-gzip"):
                    body = json.loads(json.dumps(metadata))
                    if case == "changed-uuid":
                        body["table-uuid"] = str(uuid.uuid4())
                    elif case == "changed-root":
                        body["location"] = root + "-old"
                    elif case == "sibling-manifest-list":
                        for snap in body["snapshots"]:
                            snap["manifest-list"] = root + "-old/metadata/invalid.avro"
                    else:
                        body["fixture-padding"] = padding
                    data = json.dumps(body).encode()
                    if case == "oversized-gzip":
                        data = gzip.compress(data)
                    reply_headers = [("Content-Length", str(len(data))),
                                     ("Last-Modified", "Sat, 21 Nov 2026 14:00:00 GMT"),
                                     ("ETag", '"test"')]
                    if headers.get("Range"):
                        assert headers["Range"].startswith("bytes=0-")
                        assert int(headers["Range"].removeprefix("bytes=0-")) >= len(data) - 1
                        reply_headers.append(("Content-Range", f"bytes 0-{len(data) - 1}/{len(data)}"))
                        return 206, reply_headers, data
                    return 200, reply_headers, data

            # Preserve the Host and signed S3 headers so the store sees the
            # original request.
            return upstream("127.0.0.1:9000", method, path, dict(headers))

        with recorder(catalog_response) as catalog, recorder(object_response) as storage:
            sql(db, f"""
                CREATE SERVER enrolled FOREIGN DATA WRAPPER lakehouse_fdw OPTIONS
                    (endpoint '{catalog}', warehouse 'smoke', s3_endpoint '{storage}',
                     s3_region 'us-east-1', catalog_prefix '{prefix}',
                     expected_table_uuid '{table_uuid}', storage_root '{root}');
                CREATE USER MAPPING FOR CURRENT_USER SERVER enrolled OPTIONS
                    (s3_access_key 'lakehouse-test', s3_secret_key 'lakehouse-test-password');
                CREATE FOREIGN TABLE parent_rows(id bigint) SERVER enrolled
                    OPTIONS (table '{db}.events', snapshot_id '{parent}');
                CREATE FOREIGN TABLE child_rows(id bigint) SERVER enrolled
                    OPTIONS (table '{db}.events', snapshot_id '{child}');
                CREATE FOREIGN TABLE no_pin(id bigint) SERVER enrolled
                    OPTIONS (table '{db}.events');
                CREATE FOREIGN TABLE absent_pin(id bigint) SERVER enrolled
                    OPTIONS (table '{db}.events', snapshot_id '-1');
            """)
            for name, expected in (("parent_rows", "10\n20\n20\n30"),
                                   ("child_rows", "10\n20\n30")):
                catalog_requests.clear()
                object_requests.clear()
                actual = sql(db, f"SELECT id FROM {name} ORDER BY id")
                assert actual == expected, (name, actual)
                # TWO loads of the SAME route per query since the relsize
                # slice (spec 19 §6.5): one at plan time (planner-visible
                # relsize metadata session, cached per relid per backend)
                # and one at execution (snapshot-pinned session). The scope
                # invariant is unchanged: only this route is touched, never
                # a namespace LIST or any other endpoint.
                assert catalog_requests == [("GET", route)] * 2, catalog_requests
                assert object_requests and all(
                    path.startswith(root_path + "/") for _, path in object_requests
                ), object_requests
                # the metadata fetch happens TWICE per query since the
                # relsize slice (plan-time sizing session + executor's
                # snapshot-pinned session; the Rust block cache is
                # session-scoped, so no cross-session dedup). Scope is
                # still exact: every path stays under root_path and the
                # manifest trio never reaches outside it.
                assert object_requests.count(("GET", metadata_path)) == 2, object_requests
                print(f"{name}: {len(object_requests)} scoped storage requests", flush=True)

            state["case"] = "warm-then-sibling"
            catalog_requests.clear()
            object_requests.clear()
            actual = sql(db, """
                SET plan_cache_mode = force_generic_plan;
                PREPARE warm AS SELECT array_agg(id ORDER BY id) FROM child_rows;
                EXECUTE warm;
                DO $check$ BEGIN
                    EXECUTE 'EXECUTE warm';
                    RAISE EXCEPTION 'reused plan accepted a sibling metadata path';
                EXCEPTION WHEN insufficient_privilege THEN NULL; END $check$;
            """)
            assert actual == "{10,20,30}", actual
            assert catalog_requests == [("GET", route)] * 2, catalog_requests
            assert len(object_requests) == state["warm_requests"] > 0, object_requests
            print("generic prepared scan warmed allowed objects, then refused sibling before IO",
                  flush=True)

            for case, sqlstate, expect_metadata in (
                ("sibling-metadata", "42501", False),
                ("changed-uuid", "55000", True),
                ("changed-root", "55000", True),
                ("sibling-manifest-list", "42501", True),
                ("catalog-redirect", "08000", False),
                ("storage-redirect", "08000", True),
                ("oversized-catalog", "08000", False),
                ("oversized-gzip", "08000", True),
            ):
                state["case"] = case
                catalog_requests.clear()
                object_requests.clear()
                sql(db, "SELECT id FROM child_rows", error=sqlstate)
                # plan-time sizing load + executor load (see above); the
                # crafted reply is served to both, so the failing sqlstate
                # still comes from the executor's session
                assert catalog_requests == [("GET", route)] * 2, (case, catalog_requests)
                expected = [("GET", metadata_path)] if expect_metadata else []
                assert object_requests == expected, (case, object_requests)
                if case.startswith("oversized-"):
                    print(json.dumps({"case": case, "sqlstate": sqlstate, "pins": [str(parent), str(child)],
                                      "catalog_calls": len(catalog_requests), "object_calls": len(object_requests)},
                                     sort_keys=True), flush=True)

            state["case"] = "allowed"
            catalog_requests.clear()
            object_requests.clear()
            sql(db, "SELECT id FROM no_pin", error="0A000")
            assert not catalog_requests and not object_requests
            sql(db, "SELECT id FROM absent_pin", error="22023")
            assert catalog_requests == [("GET", route)], catalog_requests
            assert object_requests == [("GET", metadata_path)], object_requests

            # Scope belongs to the server owner. A foreign-table option
            # cannot narrow or expand the enrolled authority.
            sql(db, f"ALTER FOREIGN TABLE child_rows OPTIONS (ADD storage_root '{root}')",
                error="HV00D")
            if sql(db, "SELECT count(*) FROM pg_am WHERE amname='lakehouse'") == "1":
                sql(db, f"""
                    CREATE TABLE managed(id bigint) USING lakehouse;
                    SELECT lakehouse.attach_table('managed'::regclass, '{db}.events', 'enrolled');
                """)
                catalog_requests.clear()
                object_requests.clear()
                sql(db, "SELECT id FROM managed", error="0A000")
                assert not catalog_requests and not object_requests

            assert not redirect_requests, redirect_requests
            print(f"pins {parent}, {child}: exact rows and bound read refusals verified", flush=True)


@contextlib.contextmanager
def login(db, role, env=None):
    # Keep the backend and its prepared statements while the admin uses a
    # separate local connection for mapping rotation and revocation.
    with subprocess.Popen(
        PSQL + ["-w", "-d", db, "-U", role, "-v", "ON_ERROR_STOP=off", "-v", "VERBOSITY=terse"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1, env=env,
    ) as psql:
        lines = queue.SimpleQueue()
        marker = "scoped-check-" + uuid.uuid4().hex

        def collect():
            for line in psql.stdout:
                lines.put(line.rstrip("\n"))
            lines.put(None)

        thread = threading.Thread(target=collect, daemon=True)
        thread.start()

        def run(statement, sqlstate="00000"):
            psql.stdin.write(f"{statement.rstrip(';')};\n\\echo {marker} :SQLSTATE\n")
            psql.stdin.flush()
            output = []
            while True:
                line = lines.get(timeout=35)
                assert line is not None, (role, output)
                if line.startswith(marker + " "):
                    assert line == f"{marker} {sqlstate}", (role, output, line)
                    break
                output.append(line)
            if sqlstate != "00000":
                assert output and output[0].startswith("ERROR:"), (role, output)
                return []
            return output

        try:
            if env is not None:
                protocol, = run("SELECT version FROM pg_stat_ssl WHERE pid = pg_backend_pid() AND ssl "
                                "AND system_user = 'scram-sha-256:' || current_user")
                assert protocol.startswith("TLS"), (role, protocol)
                print(json.dumps({"check": "tenant_transport", "protocol": protocol,
                                  "auth": "scram-sha-256", "sslmode": env["PGSSLMODE"]}), flush=True)
            yield run
        finally:
            psql.stdin.close()
            try:
                psql.wait(timeout=5)
            except subprocess.TimeoutExpired:
                psql.kill()
                psql.wait(timeout=5)
            thread.join(timeout=5)


def tenant_roles(db):
    return f"{db}_a", f"{db}_b"


def check_tls_login(db, role, env):
    # Pin the libpq failure to the intended cause before running positive reads.
    for patch, expected in (
        ({"PGHOST": "wrong-name.invalid"}, "does not match host name"),
        ({"PGPASSWORD": secrets.token_hex(24)}, "password authentication failed"),
        ({"PGSSLMODE": "disable", "PGCHANNELBINDING": "disable"}, "no pg_hba.conf entry"),
    ):
        result = subprocess.run(
            PSQL + ["-w", "-d", db, "-U", role, "-c", "SELECT 1"],
            env=env | patch, capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0 or expected not in result.stderr:
            raise AssertionError(f"missing TLS login refusal for {expected}")


def exercise_tenants(db, parent, child, sibling, primary, secondary, tls_root):
    role_a, role_b = tenant_roles(db)
    prefix, route_a, table_a = primary
    prefix_b, route_b, table_b = secondary
    assert prefix == prefix_b
    root_a, root_b = [table["metadata"]["location"] for table in (table_a, table_b)]
    assert root_a == f"s3://warehouse/scope-a/{db}", root_a
    assert root_b == f"s3://warehouse/scope-a-old/{db}", root_b
    assert table_a["metadata"]["table-uuid"] != table_b["metadata"]["table-uuid"]
    path_a, path_b = ["/" + root.removeprefix("s3://") + "/" for root in (root_a, root_b)]
    metadata_a, metadata_b = [
        "/" + table["metadata-location"].removeprefix("s3://") for table in (table_a, table_b)
    ]
    assert metadata_a.startswith(path_a) and metadata_b.startswith(path_b)
    key_a, key_b, key_rotated = "scope-a", "scope-b", "scope-a-rotated"
    tokens = {route_a: (key_a, key_rotated), route_b: (key_b,)}
    catalog_requests, object_requests = [], []
    return_sibling = False
    store_address = os.getenv("SCOPED_READ_STORE_ADDR", "127.0.0.1:9000")

    def catalog_response(method, path, headers):
        token = headers.get("Authorization", "").removeprefix("Bearer ")
        catalog_requests.append((method, path, token))
        if method != "GET" or token not in tokens.get(path, ()):
            return 403, [], b""
        # The local Lakekeeper profile has no auth. The recorder enforces its
        # per-route token contract and forwards actual Lakekeeper responses.
        upstream_path = route_b if return_sibling and path == route_a else path
        return upstream("127.0.0.1:8181", method, "/catalog" + upstream_path, {})

    def object_response(method, path, headers):
        # Do not enforce storage grants in this recorder. SeaweedFS evaluates
        # the original SigV4 signature and prefix policy independently.
        credential = re.search(r"\bCredential=([^/ ]+)", headers.get("Authorization", ""))
        key = credential[1] if credential else None
        response = upstream(store_address, method, path, dict(headers))
        object_requests.append((key, path, response[0], len(response[2])))
        return response

    def check(name, session, statement, pin, rows, *, route=None, token=None,
              key=None, path=None, sqlstate="00000", denied_object=None):
        catalog_requests.clear()
        object_requests.clear()
        started = time.monotonic()
        actual = session(statement, sqlstate)
        duration_ms = round((time.monotonic() - started) * 1000, 3)
        assert actual == rows, (name, actual, rows)
        catalog_expected = [("GET", route, token)] if route else []
        assert catalog_requests == catalog_expected, (name, catalog_requests)
        if denied_object:
            assert [(k, p, s) for k, p, s, _ in object_requests] == [
                (key, denied_object, 403)], (name, object_requests)
        elif path:
            assert object_requests and all(
                observed_key == key and observed_path.startswith(path) and status in (200, 206)
                for observed_key, observed_path, status, _ in object_requests
            ), (name, object_requests)
        else:
            assert not object_requests, (name, object_requests)
        print(json.dumps({
            "case": name, "snapshot_id": str(pin),
            "rows": [int(row) for row in actual[0].strip("{}").split(",")] if actual else [],
            "sqlstate": sqlstate,
            "samples": 1, "duration_ms": duration_ms, "catalog_calls": len(catalog_requests),
            "object_calls": len(object_requests), "object_bytes": sum(
                size for _, _, status, size in object_requests if status in (200, 206)),
            "provider_forbidden": sum(status == 403 for _, _, status, _ in object_requests),
            "forbidden_root_calls": sum(not path or not p.startswith(path)
                                        for _, p, _, _ in object_requests),
            "mapping_key": key, "catalog_token": token,
        }, sort_keys=True), flush=True)

    with recorder(catalog_response) as catalog, recorder(object_response) as storage:
        login_env = {}
        for role in (role_a, role_b):
            if tls_root:
                password = secrets.token_hex(24)
                sql(db, f"SET password_encryption='scram-sha-256'; CREATE ROLE {role} LOGIN PASSWORD '{password}'")
                login_env[role] = dict(
                    os.environ, PGHOST="localhost", PGHOSTADDR="127.0.0.1", PGSSLMODE="verify-full",
                    PGSSLROOTCERT=tls_root, PGREQUIREAUTH="scram-sha-256", PGCHANNELBINDING="require",
                    PGGSSENCMODE="disable", PGPASSWORD=password, PGCONNECT_TIMEOUT="5", LC_ALL="C",
                )
            else:
                sql(db, f"CREATE ROLE {role} LOGIN")
                login_env[role] = None
        if tls_root:
            check_tls_login(db, role_a, login_env[role_a])

        for server, table, root, role, key in (
            ("tenant_a", table_a, root_a, role_a, key_a),
            ("tenant_b", table_b, root_b, role_b, key_b),
        ):
            sql(db, f"""
                CREATE SERVER {server} FOREIGN DATA WRAPPER lakehouse_fdw OPTIONS
                    (endpoint '{catalog}', warehouse 'smoke', s3_endpoint '{storage}',
                     s3_region 'us-east-1', catalog_prefix '{prefix}',
                     expected_table_uuid '{table["metadata"]["table-uuid"]}', storage_root '{root}');
                GRANT USAGE ON FOREIGN SERVER {server} TO {role};
                CREATE USER MAPPING FOR {role} SERVER {server} OPTIONS
                    (token '{key}', s3_access_key '{key}', s3_secret_key '{key}-password');
            """)

        sql(db, f"""
            CREATE FOREIGN TABLE tenant_parent(id bigint) SERVER tenant_a
                OPTIONS (table '{db}.events', snapshot_id '{parent}');
            CREATE FOREIGN TABLE tenant_child(id bigint) SERVER tenant_a
                OPTIONS (table '{db}.events', snapshot_id '{child}');
            CREATE FOREIGN TABLE tenant_sibling(id bigint) SERVER tenant_b
                OPTIONS (table '{db}.events_old', snapshot_id '{sibling}');
            GRANT SELECT ON tenant_parent, tenant_child TO {role_a}, {role_b};
            GRANT SELECT ON tenant_sibling TO {role_b};
            CREATE VIEW owner_parent AS SELECT id FROM tenant_parent;
            CREATE VIEW invoker_parent WITH (security_invoker=true) AS SELECT id FROM tenant_parent;
            ALTER VIEW owner_parent OWNER TO {role_a};
            ALTER VIEW invoker_parent OWNER TO {role_a};
            GRANT SELECT ON owner_parent, invoker_parent TO {role_b};
            CREATE VIEW owner_sibling AS SELECT id FROM tenant_sibling;
            ALTER VIEW owner_sibling OWNER TO {role_b};
            GRANT SELECT ON owner_sibling TO {role_a};
        """)

        with login(db, role_a, login_env[role_a]) as a, login(db, role_b, login_env[role_b]) as b:
            who = "SELECT session_user || ':' || current_user || ':' || pg_backend_pid()"
            login_a, = a(who)
            login_b, = b(who)
            assert login_a.startswith(f"{role_a}:{role_a}:")
            assert login_b.startswith(f"{role_b}:{role_b}:")
            assert login_a.rsplit(":", 1)[1] != login_b.rsplit(":", 1)[1]
            for session in (a, b):
                session("SET plan_cache_mode = force_generic_plan")
            a("PREPARE pinned AS SELECT array_agg(id ORDER BY id) FROM tenant_parent")
            b("PREPARE pinned AS SELECT array_agg(id ORDER BY id) FROM tenant_sibling")
            b("PREPARE owner_read AS SELECT array_agg(id ORDER BY id) FROM owner_parent")
            b("PREPARE invoker_read AS SELECT array_agg(id ORDER BY id) FROM invoker_parent")
            # The Trino time-travel queries separately verify these rows and pins.
            parent_rows, child_rows, sibling_rows = ["{10,20,20,30}"], ["{10,20,30}"], ["{90,90,99}"]
            allowed_a = dict(route=route_a, token=key_a, key=key_a, path=path_a)
            allowed_b = dict(route=route_b, token=key_b, key=key_b, path=path_b)
            rotated = dict(route=route_a, token=key_rotated, key=key_rotated, path=path_a)
            check("a-prepared-parent", a, "EXECUTE pinned", parent, parent_rows, **allowed_a)
            check("a-direct-child", a, "SELECT array_agg(id ORDER BY id) FROM tenant_child",
                  child, child_rows, **allowed_a)
            check("b-prepared-sibling", b, "EXECUTE pinned", sibling, sibling_rows, **allowed_b)
            check("b-owner-view", b, "EXECUTE owner_read", parent, parent_rows, **allowed_a)
            check("b-invoker-view-missing-mapping", b, "EXECUTE invoker_read", parent, [],
                  sqlstate="42704")

            # Warm B's bytes in A's own backend through an authorized owner view.
            check("a-authorized-sibling-cache-warm", a,
                  "SELECT array_agg(id ORDER BY id) FROM owner_sibling",
                  sibling, sibling_rows, **allowed_b)
            return_sibling = True
            try:
                check("a-warm-plan-sibling-catalog-result", a, "EXECUTE pinned", parent, [],
                      route=route_a, token=key_a, sqlstate="42501")
            finally:
                return_sibling = False

            # Wiring the valid B key to A's relation still cannot read A's
            # bytes. This is an actual provider 403, on A's approved root.
            sql(db, f"""
                ALTER USER MAPPING FOR {role_a} SERVER tenant_a OPTIONS
                    (SET s3_access_key '{key_b}', SET s3_secret_key '{key_b}-password');
            """)
            check("a-warm-plan-provider-denial", a, "EXECUTE pinned", parent, [],
                  route=route_a, token=key_a, key=key_b, path=path_a,
                  sqlstate="08000", denied_object=metadata_a)
            sql(db, f"""
                ALTER USER MAPPING FOR {role_b} SERVER tenant_b OPTIONS
                    (SET s3_access_key '{key_a}', SET s3_secret_key '{key_a}-password');
            """)
            check("b-warm-plan-provider-denial", b, "EXECUTE pinned", sibling, [],
                  route=route_b, token=key_b, key=key_a, path=path_b,
                  sqlstate="08000", denied_object=metadata_b)
            sql(db, f"""
                ALTER USER MAPPING FOR {role_b} SERVER tenant_b OPTIONS
                    (SET s3_access_key '{key_b}', SET s3_secret_key '{key_b}-password');
            """)
            sql(db, f"""
                ALTER USER MAPPING FOR {role_a} SERVER tenant_a OPTIONS
                    (SET token '{key_rotated}', SET s3_access_key '{key_rotated}',
                     SET s3_secret_key '{key_rotated}-password');
            """)
            check("a-warm-plan-rotated", a, "EXECUTE pinned", parent, parent_rows, **rotated)
            with login(db, role_a, login_env[role_a]) as reopened:
                reopened_login, = reopened(who)
                assert reopened_login.startswith(f"{role_a}:{role_a}:") and reopened_login != login_a
                check("a-reopened-rotated", reopened,
                      "SELECT array_agg(id ORDER BY id) FROM tenant_parent", parent, parent_rows,
                      **rotated)
            sql(db, f"DROP USER MAPPING FOR {role_a} SERVER tenant_a")
            check("a-warm-plan-revoked", a, "EXECUTE pinned", parent, [], sqlstate="42704")
            check("b-warm-owner-plan-revoked", b, "EXECUTE owner_read", parent, [],
                  sqlstate="42704")
            check("b-warm-sibling-still-allowed", b, "EXECUTE pinned", sibling, sibling_rows,
                  **allowed_b)
            assert a(who) == [login_a] and b(who) == [login_b]


def main():
    if not __debug__:
        raise RuntimeError("the smoke requires Python assertions")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tls-root-cert", help="use verify-full TCP and SCRAM for the tenant logins")
    args = parser.parse_args()
    db = "bound_" + uuid.uuid4().hex
    table = f"iceberg.{db}.events"
    other = f"iceberg.{db}.events_old"
    sql("postgres", f"CREATE DATABASE {db}")
    try:
        sql(db, "CREATE EXTENSION lakehouse")
        trino(f"CREATE SCHEMA iceberg.{db}")
        try:
            trino(f"CREATE TABLE {table} (id bigint, seq integer) WITH "
                  f"(format_version=2, location='s3://warehouse/scope-a/{db}')")
            trino(f"INSERT INTO {table} VALUES (10,1),(20,2),(20,3),(30,4)")
            history = (
                f'SELECT snapshot_id FROM iceberg.{db}."events$history" '
                'ORDER BY made_current_at DESC LIMIT 1'
            )
            parent = int(trino(history))
            trino(f"DELETE FROM {table} WHERE seq=3")
            child = int(trino(history))
            assert child != parent
            enrolled = catalog_identity(db)
            exercise(db, parent, child, *enrolled)

            trino(f"CREATE TABLE {other} (id bigint) WITH "
                  f"(format_version=2, location='s3://warehouse/scope-a-old/{db}')")
            trino(f"INSERT INTO {other} VALUES (90),(90),(99)")
            sibling = int(trino(
                f'SELECT snapshot_id FROM iceberg.{db}."events_old$history" '
                'ORDER BY made_current_at DESC LIMIT 1'
            ))
            for source, pin, expected in (
                (table, parent, "10\n20\n20\n30"), (table, child, "10\n20\n30"),
                (other, sibling, "90\n90\n99"),
            ):
                actual = trino(f"SELECT id FROM {source} FOR VERSION AS OF {pin} ORDER BY id")
                assert actual == expected, (source, pin, actual)
            exercise_tenants(db, parent, child, sibling, enrolled, catalog_identity(db, "events_old"),
                             args.tls_root_cert)
            if args.tls_root_cert:
                print(json.dumps({"check": "scoped_tls_scram", "refusals": ["hostname", "password", "cleartext"]}), flush=True)
        finally:
            trino(f"DROP TABLE IF EXISTS {other}")
            trino(f"DROP TABLE IF EXISTS {table}")
            trino(f"DROP SCHEMA iceberg.{db}")
    finally:
        sql("postgres", f"DROP DATABASE {db}")
        sql("postgres", "DROP ROLE IF EXISTS " + ", ".join(tenant_roles(db)))


if __name__ == "__main__":
    main()
