"""Exercise a bound S3 read with an independent catalog and object recorder."""
import contextlib
import http.client
import http.server
from importlib import import_module
import json
import threading
import uuid
import urllib.request

from publication_fixture import sql

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
                self.wfile.write(data)

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


def catalog_identity(namespace):
    # Only this fixture enrolls from a freshly created fixture table. Real
    # deployments take these values from their independent enrollment record.
    catalog = "http://127.0.0.1:8181/catalog"
    with urllib.request.urlopen(catalog + "/v1/config?warehouse=smoke", timeout=10) as response:
        config = json.load(response)
    prefix = (config.get("defaults", {}) | config.get("overrides", {})).get("prefix", "")
    route = f'/v1/{prefix + "/" if prefix else ""}namespaces/{namespace}/tables/events'
    with urllib.request.urlopen(catalog + route, timeout=10) as response:
        return prefix, route, json.load(response)


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
            return 200, [("Content-Type", "application/json")], json.dumps(result).encode()

        def object_response(method, path, headers):
            object_requests.append((method, path))
            if not path.startswith(root_path + "/"):
                return 403, [], b""
            case = state["case"]
            if path == metadata_path:
                if case == "storage-redirect":
                    return 307, [("Location", forbidden + metadata_path)], b""
                if case in ("changed-uuid", "changed-root", "sibling-manifest-list"):
                    body = json.loads(json.dumps(metadata))
                    if case == "changed-uuid":
                        body["table-uuid"] = str(uuid.uuid4())
                    elif case == "changed-root":
                        body["location"] = root + "-old"
                    else:
                        for snap in body["snapshots"]:
                            snap["manifest-list"] = root + "-old/metadata/invalid.avro"
                    data = json.dumps(body).encode()
                    return 200, [("Content-Length", str(len(data))),
                                 ("Last-Modified", "Sat, 21 Nov 2026 14:00:00 GMT"),
                                 ("ETag", '"test"')], data

            with contextlib.closing(http.client.HTTPConnection("127.0.0.1", 9000, timeout=10)) as upstream:
                # Preserve the Host and signed S3 headers. The store sees the
                # original request, with the proxy recording method and key.
                upstream.request(method, path, headers=dict(headers))
                response = upstream.getresponse()
                return response.status, response.getheaders(), response.read()

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
                assert catalog_requests == [("GET", route)], catalog_requests
                assert object_requests and all(
                    path.startswith(root_path + "/") for _, path in object_requests
                ), object_requests
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
            ):
                state["case"] = case
                catalog_requests.clear()
                object_requests.clear()
                sql(db, "SELECT id FROM child_rows", error=sqlstate)
                assert catalog_requests == [("GET", route)], (case, catalog_requests)
                expected = [("GET", metadata_path)] if expect_metadata else []
                assert object_requests == expected, (case, object_requests)

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


def main():
    if not __debug__:
        raise RuntimeError("the smoke requires Python assertions")
    db = "bound_" + uuid.uuid4().hex
    table = f"iceberg.{db}.events"
    sql("postgres", f"CREATE DATABASE {db}")
    try:
        sql(db, "CREATE EXTENSION lakehouse")
        trino(f"CREATE SCHEMA iceberg.{db}")
        try:
            trino(f"CREATE TABLE {table} (id bigint, seq integer) WITH (format_version=2)")
            trino(f"INSERT INTO {table} VALUES (10,1),(20,2),(20,3),(30,4)")
            history = (
                f'SELECT snapshot_id FROM iceberg.{db}."events$history" '
                'ORDER BY made_current_at DESC LIMIT 1'
            )
            parent = int(trino(history))
            trino(f"DELETE FROM {table} WHERE seq=3")
            child = int(trino(history))
            assert child != parent
            exercise(db, parent, child, *catalog_identity(db))
        finally:
            trino(f"DROP TABLE IF EXISTS {table}")
            trino(f"DROP SCHEMA iceberg.{db}")
    finally:
        sql("postgres", f"DROP DATABASE {db}")


if __name__ == "__main__":
    main()
