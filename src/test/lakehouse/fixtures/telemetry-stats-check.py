"""Telemetry + stats persistence (spec 19 §4, layer L0).

Captures per-statement lakehouse telemetry into the INTERNAL
lakehouse.query_stats table (admin-owned, PUBLIC revoked) with
owner-context inserts working for unprivileged invokers; retention
bounded; drain into the tenant's managed Iceberg archive + publish;
operator share fails closed without explicit audited consent and works
after grant_operator_telemetry.
"""

import importlib
import json
from uuid import uuid4

from publication_fixture import sql

trino = importlib.import_module("bound-publication-restart").trino

CATALOG = "http://localhost:8181/catalog"
S3 = "http://localhost:9000"


def main():
    if not __debug__:
        raise RuntimeError("the telemetry fixture requires Python assertions")
    ns = "telst_" + uuid4().hex[:12]
    name = "telst_" + uuid4().hex[:8]
    reader = "telst_r_" + uuid4().hex[:8]
    table = f"iceberg.{ns}.events"
    trino(f"CREATE SCHEMA iceberg.{ns}")
    trino(f"CREATE TABLE {table} (id bigint, amt double) WITH (format_version = 2)")
    trino(f"INSERT INTO {table} VALUES (1, 10.0), (2, 20.0), (3, 30.0)")
    archive = f"iceberg.{ns}.query_stats_archive"
    # NOTE: naive timestamp — the managed writer emits Iceberg timestamp
    # (UTC-normalized) without the tz-adjusted variant; values are UTC.
    trino(f'CREATE TABLE {archive} (captured_at timestamp(6), '
          'dbid bigint, userid bigint, queryid bigint, "rows" bigint, '
          'total_ms double, foreign_scans integer) '
          'WITH (format_version = 2)')
    try:
        sql("postgres", f"CREATE DATABASE {name} TEMPLATE template0 "
                        f"ENCODING 'UTF8' LOCALE 'C'")
        try:
            sql(name, f"""
                CREATE EXTENSION lakehouse;
                ALTER EXTENSION lakehouse UPDATE TO '1.22';
                CREATE FOREIGN DATA WRAPPER lh HANDLER lakehouse.lakehouse_fdw_handler;
                CREATE SERVER s FOREIGN DATA WRAPPER lh OPTIONS
                    (endpoint '{CATALOG}', warehouse 'smoke', s3_endpoint '{S3}');
                CREATE USER MAPPING FOR CURRENT_USER SERVER s OPTIONS
                    (s3_access_key 'lakehouse-test', s3_secret_key 'lakehouse-test-password');
                CREATE FOREIGN TABLE events (id bigint, amt double precision) SERVER s
                    OPTIONS (table '{ns}.events');
            """)

            def lines(stmt):
                return [x for x in sql(name, stmt).split("\n") if x]

            # CAPTURE: an ordinary foreign-scan query (run by the plain
            # admin user) must appear in the internal table after commit
            sql(name, "SELECT count(*) FROM events")
            r = sql(name, """
                SELECT count(*) FROM lakehouse.query_stats
                WHERE foreign_scans >= 1 AND total_ms IS NOT NULL
                  AND queryid IS NOT NULL
            """).strip()
            assert int(r) >= 1, f"no captured foreign-scan row: {r}"

            # B1: multi-statement TRANSACTION — every non-final
            # statement's record must survive to the commit flush with
            # valid values (the pre-fix bug read freed portal memory)
            sql(name, """
                BEGIN;
                SELECT count(*) FROM events;
                SELECT count(*) FROM events;
                SELECT count(*) FROM events;
                COMMIT;
            """)
            r = sql(name, """
                SELECT count(*) FROM lakehouse.query_stats
                WHERE foreign_scans >= 1 AND total_ms IS NOT NULL
                  AND total_ms >= 0 AND captured_at IS NOT NULL
            """).strip()
            assert int(r) >= 4, f"multi-statement tx capture lost rows: {r}"

            # B2: a statement that errors MID-EXECUTION must not poison
            # capture for later statements in the session
            sql(name, "SELECT 1.0 / (id - 3) FROM events;",
                error="22012")
            sql(name, "SELECT count(*) FROM events")
            r = sql(name, """
                SELECT count(*) FROM lakehouse.query_stats
                WHERE foreign_scans >= 1
            """).strip()
            assert int(r) >= 5, f"capture died after a run error: {r}"

            # negative case: a heap-only statement must not ADD a row
            # (count(*) over foreign tables legitimately shows
            # foreign_scans=0 — the foreign-count path plans a core
            # ForeignPath whose ForeignScan node the capture counts only
            # when present; heap statements must never appear at all)
            r = sql(name, "SELECT count(*) FROM lakehouse.query_stats").strip()
            before = int(r)
            sql(name, "SELECT count(*) FROM pg_class")
            r = sql(name, "SELECT count(*) FROM lakehouse.query_stats").strip()
            assert int(r) == before, f"heap-only statement captured: {r}"

            # INTERNAL: PUBLIC revoked — an unprivileged role can neither
            # read the table nor call the admin functions
            sql(name, f"CREATE ROLE {reader} LOGIN")
            sql(name, f"GRANT USAGE ON SCHEMA lakehouse TO {reader}")
            sql(name, f"GRANT SELECT ON events TO {reader}")
            sql(name, f"GRANT USAGE ON FOREIGN SERVER s TO {reader}")
            sql(name, f"CREATE USER MAPPING FOR {reader} SERVER s OPTIONS "
                      "(s3_access_key 'lakehouse-test', "
                      "s3_secret_key 'lakehouse-test-password')")
            # the non-admin's own query is STILL captured (owner-context
            # insert), even though the role cannot read the stats table
            sql(name, f"SET ROLE {reader}; SELECT count(*) FROM events; "
                      "RESET ROLE;")
            r = sql(name, f"""
                SELECT count(*) FROM lakehouse.query_stats
                WHERE userid <> (SELECT oid FROM pg_roles
                                 WHERE rolname = CURRENT_USER)
                  AND foreign_scans >= 1
            """).strip()
            assert int(r) >= 1, f"non-admin capture missing: {r}"
            sql(name, f"SET ROLE {reader}; "
                      "SELECT 1 FROM lakehouse.query_stats LIMIT 1",
                error="42501")
            # the reader lacks EXECUTE entirely (PUBLIC revoked) — the
            # fail-closed gate fires at 55000 for the ADMIN (below); the
            # reader never even gets that far
            sql(name, f"SET ROLE {reader}; "
                      "SELECT lakehouse.share_query_stats_with_operator()",
                error="42501")
            sql(name, "SELECT lakehouse.share_query_stats_with_operator()",
                error="55000")
            sql(name, f"SET ROLE {reader}; "
                      "SELECT lakehouse.grant_operator_telemetry('op')",
                error="42501")

            # ARCHIVE: bootstrap the managed Iceberg archive, flush, and
            # verify the heap table drained and the snapshot published
            sql(name, f"""
                -- naive timestamp: the writer's type map currently emits
                -- Iceberg timestamp (naive, UTC-normalized); a timestamptz
                -- column hits a schema/batch type mismatch (separate slice)
                CREATE TABLE lakehouse.query_stats_archive (
                    captured_at   timestamp,
                    dbid          bigint,
                    userid        bigint,
                    queryid       bigint,
                    rows          bigint,
                    total_ms      double precision,
                    foreign_scans integer
                ) USING lakehouse WITH (write_mode='staged');
                SELECT lakehouse.attach_table('lakehouse.query_stats_archive',
                                              '{ns}.query_stats_archive', 's');
            """)
            # two-phase: call N drains + stages ('staged', snapshot NULL);
            # call N+1 publishes (the publisher cannot emit segments staged
            # in its own transaction)
            r = sql(name, "SELECT archived, snapshot_id, state FROM "
                          "lakehouse.flush_query_stats()").strip()
            archived, snap, state = [x.strip() for x in r.split("|")]
            assert state == "staged" and snap == "" and int(archived) >= 2, r
            r = sql(name, "SELECT archived, snapshot_id, state FROM "
                          "lakehouse.flush_query_stats()").strip()
            archived, snap, state = [x.strip() for x in r.split("|")]
            assert state == "published" and snap != "", r
            r = sql(name, "SELECT count(*) FROM lakehouse.query_stats").strip()
            assert int(r) == 0, f"heap table did not drain: {r}"

            # CONSENT: explicit grant unlocks the operator share, and the
            # ledger records who/when; revoke closes it again
            sql(name, "SELECT lakehouse.grant_operator_telemetry("
                      "'operator-endpoint')")
            r = sql(name, "SELECT granted, endpoint IS NOT NULL, "
                          "granted_by = session_user "
                          "FROM lakehouse.telemetry_consent").strip()
            assert r == "t|t|t", r  # granted_by records the INVOKING admin
            # a share with nothing pending reports no change and stamps
            # nothing
            r = sql(name, "SELECT shared, snapshot_id, state FROM "
                          "lakehouse.share_query_stats_with_operator()").strip()
            shared, sh_snap, sh_state = [x.strip() for x in r.split("|")]
            assert sh_state == "no change", r
            r = sql(name, "SELECT shared_at IS NOT NULL FROM "
                          "lakehouse.telemetry_consent").strip()
            assert r == "f", r

            # a share WITH data: capture one more statement, share
            # (two-phase: stages on this call), then the second share
            # publishes + stamps the ledger
            sql(name, "SELECT count(*) FROM events")
            r = sql(name, "SELECT shared, snapshot_id, state FROM "
                          "lakehouse.share_query_stats_with_operator()").strip()
            shared, sh_snap, sh_state = [x.strip() for x in r.split("|")]
            assert sh_state == "staged" and int(shared) >= 1, r
            r = sql(name, "SELECT shared, snapshot_id, state FROM "
                          "lakehouse.share_query_stats_with_operator()").strip()
            shared, sh_snap, sh_state = [x.strip() for x in r.split("|")]
            assert sh_state == "published" and sh_snap != "", r
            r = sql(name, "SELECT shared_at IS NOT NULL FROM "
                          "lakehouse.telemetry_consent").strip()
            assert r == "t", r
            sql(name, "SELECT lakehouse.revoke_operator_telemetry()")
            r = sql(name, "SELECT granted FROM lakehouse.telemetry_consent").strip()
            assert r == "f", r
            sql(name, "SELECT lakehouse.share_query_stats_with_operator()",
                error="55000")

            print(json.dumps({"case": "telemetry-stats",
                              "archived": int(archived),
                              "snapshot_id": snap}), flush=True)
        finally:
            sql("postgres", f"DROP DATABASE {name} WITH (FORCE)")
    finally:
        trino(f"DROP TABLE IF EXISTS {archive}; DROP TABLE IF EXISTS {table}; "
              f"DROP SCHEMA iceberg.{ns}")


if __name__ == "__main__":
    main()
