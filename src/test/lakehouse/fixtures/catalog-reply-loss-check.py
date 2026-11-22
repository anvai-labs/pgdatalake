"""Exercise SQL COMMIT when Lakekeeper accepts an append and its reply is lost."""

import http.client
import json
import os
import subprocess
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def trino(sql):
    container = os.getenv("TRINO_CONTAINER", "pgdatalake-trino-1")
    return subprocess.check_output(
        ["docker", "exec", container, "trino", "--output-format", "TSV", "--execute", sql],
        text=True,
    ).strip()


def main():
    namespace = "reply_loss_" + uuid.uuid4().hex[:12]
    table = f"iceberg.{namespace}.events"
    attempts = []

    class CatalogGateway(BaseHTTPRequestHandler):
        def do_GET(self):
            self.forward(b"")

        def do_POST(self):
            if not self.path.endswith(f"/namespaces/{namespace}/tables/events"):
                self.send_error(404)
                return
            self.forward(self.rfile.read(int(self.headers["Content-Length"])))

        def forward(self, payload):
            if not self.path.startswith("/catalog/v1/"):
                self.send_error(404)
                return
            upstream = http.client.HTTPConnection("127.0.0.1", 8181, timeout=20)
            try:
                headers = {k: self.headers[k] for k in ("Authorization", "Content-Type")
                           if k in self.headers}
                upstream.request(self.command, self.path, payload, headers)
                reply = upstream.getresponse()
                status, body = reply.status, reply.read()
                if self.command == "POST":
                    attempts.append((status, [u["action"] for u in json.loads(payload)["updates"]]))
                    if 200 <= status < 300:
                        status, body = 504, b""
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            finally:
                upstream.close()

    trino(f"CREATE SCHEMA iceberg.{namespace}")
    try:
        trino(f"CREATE TABLE {table} (id bigint) WITH (format_version=2); "
              f"INSERT INTO {table} VALUES 10")
        with ThreadingHTTPServer(("127.0.0.1", 0), CatalogGateway) as gateway:
            worker = threading.Thread(target=gateway.serve_forever)
            worker.start()
            try:
                # Local setup rolls back with the caller's failed COMMIT.
                sql = rf"""BEGIN;
CREATE EXTENSION IF NOT EXISTS lakehouse;
CREATE FOREIGN DATA WRAPPER {namespace} HANDLER lakehouse.lakehouse_fdw_handler;
CREATE SERVER {namespace} FOREIGN DATA WRAPPER {namespace} OPTIONS
    (endpoint 'http://127.0.0.1:{gateway.server_port}/catalog', warehouse 'smoke', s3_endpoint 'http://localhost:9000');
CREATE USER MAPPING FOR CURRENT_USER SERVER {namespace} OPTIONS
    (s3_access_key 'lakehouse-test', s3_secret_key 'lakehouse-test-password');
CREATE TABLE {namespace}(id bigint) USING lakehouse;
SELECT lakehouse.attach_table('{namespace}'::regclass, '{namespace}.events', '{namespace}');
INSERT INTO {namespace} VALUES (20);
\set ON_ERROR_STOP off
COMMIT;
\set commit_state :SQLSTATE
\set ON_ERROR_STOP on
SELECT :'commit_state';
"""
                state = subprocess.check_output(
                    [os.getenv("PSQL", "psql"), "-X", "-qAt", "-v", "ON_ERROR_STOP=1"],
                    input=sql, text=True, timeout=70,
                ).strip()
            finally:
                gateway.shutdown()
                worker.join()
        rows = trino(f"SELECT array_join(array_agg(id ORDER BY id), ',') FROM {table}")
        print(f"SQLSTATE={state} catalog commits={attempts} Trino rows={rows}")
        assert state == "40003" and rows == "10,20"
        assert len(attempts) == 1
        status, actions = attempts[0]
        assert 200 <= status < 300 and actions == ["add-snapshot", "set-snapshot-ref"]
    finally:
        trino(f"DROP TABLE IF EXISTS {table}; DROP SCHEMA iceberg.{namespace}")


if __name__ == "__main__":
    main()
