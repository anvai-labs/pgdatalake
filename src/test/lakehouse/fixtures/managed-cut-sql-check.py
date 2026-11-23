"""Drive the lost-reply managed cut through ordinary bound TableAM SQL.

One writer POST is accepted behind the catalog reply-loss barrier; three
held reader snapshots observe the accepted append at exactly one
compatible pin each ([10], [10,20], [10,20,20]) with recorded pin and
overlay evidence, zero reader POSTs and one observation GET per scan.
A forced rescan keeps the original cut with zero catalog resolutions;
denied, expired and uncertifiable enrollments never emit a row.
"""

import argparse
import importlib
import json
from pathlib import Path
from time import sleep
from uuid import uuid4

from publication_fixture import HDR, SEG, sql

cut = importlib.import_module("managed-cut-check")
restart = importlib.import_module("bound-publication-restart")

login = importlib.import_module("scoped-read-check").login
trino = restart.trino

PROFILE = "lakehouse_publication_credentials"


def reader_mapping(db, reader, server):
    return sql(db, f"""
        SELECT m.oid FROM pg_user_mapping m
        JOIN pg_foreign_server s ON s.oid = m.umserver
        JOIN pg_roles r ON r.oid = m.umuser
        WHERE s.srvname = '{server}' AND r.rolname = '{reader}'
    """)


def install_reader_grant(db, binding, reader, prefix, *, purpose="read", server="reader_credentials",
                         ttl="15 minutes"):
    if sql(db, f"SELECT count(*) FROM pg_foreign_server WHERE srvname = '{server}'") != "1":
        sql(db, f"CREATE SERVER {server} FOREIGN DATA WRAPPER {PROFILE};")
    sql(db, f"""
        CREATE USER MAPPING IF NOT EXISTS FOR {reader} SERVER {server} OPTIONS (
            purpose '{purpose}', token 'fixture-read', s3_access_key 'lakehouse-reader',
            s3_secret_key 'lakehouse-reader-password');
    """)
    mapping = reader_mapping(db, reader, server)
    sql(db, f"SELECT lakehouse.grant_managed_read('{binding}', '{reader}', {mapping}, "
            f"catalog_prefix => '{prefix}', ttl => interval '{ttl}')")
    return mapping


def exercise(db, namespace, writer, reader, owner, worker):
    relation = f"iceberg.{namespace}.events"
    trino(f"CREATE TABLE {relation} (id bigint) WITH (format_version=2)")
    trino(f"INSERT INTO {relation} VALUES (10)")
    history = f'SELECT snapshot_id FROM iceberg.{namespace}."events$refs" WHERE name=\'main\''
    parent = trino(history)
    route, _, metadata = restart.enrolled_metadata(namespace)
    authority = metadata["table-uuid"]
    incarnation, binding = str(uuid4()), str(uuid4())
    table = metadata["table-uuid"]
    root = metadata["location"]
    prefix = route.split("/namespaces/", 1)[0].removeprefix("/catalog/v1").lstrip("/")

    with cut.dropped_catalog_reply(route) as (endpoint, posts, reads):
        approval = restart.approved_target(namespace, endpoint, prefix, authority, table,
                                           root.rstrip("/"), binding, incarnation)
        sql(db, f"""
            CREATE EXTENSION lakehouse;
            ALTER EXTENSION lakehouse UPDATE TO '1.19';
            CREATE SERVER cut_catalog FOREIGN DATA WRAPPER lakehouse_fdw OPTIONS
                (endpoint '{endpoint}', warehouse 'smoke',
                 s3_endpoint 'http://127.0.0.1:9000');
            CREATE TABLE events(id bigint) USING lakehouse WITH (write_mode='staged');
            SELECT lakehouse.attach_table('events', '{namespace}.events', 'cut_catalog');
            GRANT INSERT, SELECT ON events TO {writer}, {reader};
            GRANT USAGE ON SCHEMA lakehouse TO {reader};
            GRANT EXECUTE ON FUNCTION lakehouse.managed_read_cut_evidence() TO {reader};
            SELECT lakehouse.enroll_managed_capture_with_anchor('events', '{namespace}.events',
                'cut_catalog', '{incarnation}', '{binding}', '{authority}', '{table}',
                '{root.rstrip("/")}', 0, 0, {parent});
            SELECT lakehouse.grant_publication_send('{binding}', '{prefix}', 'us-east-1');
        """)

        # Baseline: unadmitted bound scan keeps the historical 0A000, and a
        # declined scan makes zero catalog requests.
        baseline = len(reads)
        sql(db, f"SET ROLE {reader}; SELECT id FROM events ORDER BY id;", error="0A000")
        assert len(reads) == baseline, reads
        print(json.dumps({"case": "baseline-0a000-untouched", "catalog_gets": 0}), flush=True)

        install_reader_grant(db, binding, reader, prefix)

        # Plain EXPLAIN opens no objects even with the grant present.
        explain_reads = len(reads)
        sql(db, f"SET ROLE {reader}; EXPLAIN SELECT id FROM events;")
        assert len(reads) == explain_reads, reads

        # Structural grant rejections: a wrong-purpose mapping and a PUBLIC
        # mapping are refused by the grant function itself.
        sql(db, f"""
            CREATE SERVER wrong_purpose FOREIGN DATA WRAPPER {PROFILE};
            CREATE USER MAPPING FOR {reader} SERVER wrong_purpose OPTIONS (
                purpose 'send', token 'fixture-send', s3_access_key 'lakehouse-test',
                s3_secret_key 'lakehouse-test-password');
        """)
        sql(db, f"SELECT lakehouse.grant_managed_read('{binding}', '{reader}', "
                f"{reader_mapping(db, reader, 'wrong_purpose')})", error="42501")
        sql(db, f"""
            CREATE SERVER public_purpose FOREIGN DATA WRAPPER {PROFILE};
            CREATE USER MAPPING FOR PUBLIC SERVER public_purpose OPTIONS (
                purpose 'read', token 'fixture-read', s3_access_key 'lakehouse-reader',
                s3_secret_key 'lakehouse-reader-password');
        """)
        public_mapping = sql(db, """
            SELECT m.oid FROM pg_user_mapping m
            JOIN pg_foreign_server s ON s.oid = m.umserver
            WHERE s.srvname = 'public_purpose'
        """)
        sql(db, f"SELECT lakehouse.grant_managed_read('{binding}', '{reader}', {public_mapping})",
            error="42501")

        # Two writer commits behind three held reader snapshots; ledger
        # instrumentation uses admin sessions inside the same visibility
        # windows (the reader itself never touches the ledger).
        with login(db, reader) as before_r:
            before_r("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            before_r("SELECT 1 AS held")
            with login(db, owner) as view_before:
                view_before("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                before_view = cut.ledger_snapshot(view_before, binding)
            assert before_view[1] == {}, before_view
            sql(db, f"SET ROLE {writer}; INSERT INTO events VALUES (20);")
            with login(db, reader) as between_r:
                between_r("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                between_r("SELECT 1 AS held")
                with login(db, owner) as view_middle:
                    view_middle("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                    middle_view = cut.ledger_snapshot(view_middle, binding)
                    first_id, = middle_view[1]
                    assert middle_view[1] == {first_id: ([20], False)}, middle_view
                    sql(db, f"""
                        BEGIN; SET LOCAL ROLE {writer};
                        INSERT INTO events VALUES (20);
                        SAVEPOINT thrown_away; INSERT INTO events VALUES (99);
                        ROLLBACK TO thrown_away; COMMIT;
                    """)
                    with login(db, reader) as fresh_r:
                        fresh_r("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                        fresh_r("SELECT 1 AS held")
                        with login(db, owner) as view_fresh:
                            view_fresh("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                            fresh_view = cut.ledger_snapshot(view_fresh, binding)
                            late_id, = fresh_view[1].keys() - {first_id}
                            assert fresh_view[1] == {first_id: ([20], False),
                                                     late_id: ([20], False)}, fresh_view

                            # One accepted writer POST behind the barrier.
                            operation = str(uuid4())
                            sql(db, f"SELECT lakehouse.claim_managed_publication('{operation}', "
                                    f"ARRAY[{first_id},{late_id}]::bigint[])")
                            hint = json.loads(sql(db, "SELECT row_to_json(c) FROM "
                                                      "lakehouse.discover_managed_publication_claims"
                                                      "(NULL, 16) c"))
                            assert hint["binding_id"] == binding, hint
                            assert cut.run_worker(worker, db, operation, approval, hint) \
                                == "hold dispatch_unknown"
                            assert len(posts) == 1 and 200 <= posts[0][0] < 300
                            child = trino(history)
                            assert child != parent
                            assert sql(db, f"""
                                SELECT a.expected_parent = {parent}
                                       AND a.proposed_snapshot_id = {child}
                                       AND c.verified_snapshot_id IS NULL
                                FROM lakehouse.publication_attempts a
                                JOIN {HDR} c USING (operation_id)
                                WHERE a.operation_id='{operation}'
                            """) == "t"

                            # The drill: three held snapshots, one cut each.
                            scan_reads = len(reads)
                            expected = (
                                (before_r, ["10"], parent, []),
                                (between_r, ["10", "20"], parent, [first_id]),
                                (fresh_r, ["10", "20", "20"], child, []),
                            )
                            for scanner, rows, pin, overlay in expected:
                                evidence = json.loads(
                                    scanner("SELECT lakehouse.managed_read_cut_evidence()")[0]) \
                                    if False else None
                                got = scanner("SELECT id FROM events ORDER BY id")
                                state = json.loads(
                                    scanner("SELECT lakehouse.managed_read_cut_evidence()")[0])
                                assert got == rows, (repr(got), repr(rows), state)
                                assert state == {"pin": int(pin), "proved": True,
                                                 "overlay": [int(i) for i in overlay]}, state
                            assert len(reads) - scan_reads == 3, reads[scan_reads:]
                            assert reads[scan_reads:] == [(route, "Bearer fixture-read")] * 3, reads
                            assert len(posts) == 1
                            print(json.dumps({"case": "managed-cut-sql-drill", "parent": parent,
                                              "child": child, "first_id": first_id,
                                              "late_id": late_id, "catalog_posts": len(posts),
                                              "reader_catalog_gets": len(reads) - scan_reads}),
                                  flush=True)

                            # Rescan keeps the cut with zero catalog resolutions.
                            fresh_r("DECLARE drill SCROLL CURSOR FOR SELECT id FROM events ORDER BY id")
                            assert fresh_r("FETCH ALL IN drill") == ["10", "20", "20"]
                            trino(f"INSERT INTO {relation} VALUES (30)")
                            head_after = trino(history)
                            assert head_after != child
                            rescan_reads = len(reads)
                            fresh_r("MOVE BACKWARD ALL IN drill")
                            assert fresh_r("FETCH ALL IN drill") == ["10", "20", "20"]
                            assert json.loads(
                                fresh_r("SELECT lakehouse.managed_read_cut_evidence()")[0]) \
                                == {"pin": int(child), "proved": True, "overlay": []}
                            assert len(reads) == rescan_reads, reads[rescan_reads:]
                            print(json.dumps({"case": "rescan-retained-cut", "head_moved_to": head_after,
                                              "catalog_resolutions": 0}), flush=True)
                            fresh_r("COMMIT")

                        # Release the held snapshots (their AccessShare
                        # locks would block the rotation's exclusive lock).
                        before_r("COMMIT")
                        between_r("COMMIT")

                        # Denied and expired scans decline with zero origin
                        # requests.
                        sql(db, f"SELECT lakehouse.revoke_managed_read('{binding}', '{reader}')")
                        with login(db, reader) as denied:
                            denied("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                            denied_reads = len(reads)
                            denied("SELECT id FROM events ORDER BY id", sqlstate="0A000")
                            assert len(reads) == denied_reads, reads
                            denied("COMMIT")
                        install_reader_grant(db, binding, reader, prefix,
                                             server="reader_credentials_v2", ttl="1 second")
                        sleep(1.5)
                        with login(db, reader) as expired:
                            expired("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                            expired_reads = len(reads)
                            expired("SELECT id FROM events ORDER BY id", sqlstate="0A000")
                            assert len(reads) == expired_reads, reads
                            expired("COMMIT")
                        sql(db, "DROP SERVER reader_credentials_v2 CASCADE")

                        # Nested SPI: a scan inside a plpgsql DO block
                        # installs the same cut, and the next statement
                        # keeps the reader identity and its snapshot.
                        install_reader_grant(db, binding, reader, prefix)
                        with login(db, reader) as nested:
                            nested("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                            nested("SELECT 1 AS held")
                            nested("DO $$ BEGIN PERFORM 1 FROM events LIMIT 1; END $$")
                            # After the rescan's Trino insert moved the
                            # head past the accepted child, the #88 verifier
                            # (head-only proof) declines and the cut falls
                            # back to the retained parent with the full
                            # overlay — the recorded unknown-head fallback.
                            nested_state = json.loads(
                                nested("SELECT lakehouse.managed_read_cut_evidence()")[0])
                            assert nested_state == {"pin": int(parent), "proved": False,
                                                    "overlay": [int(first_id), int(late_id)]}, \
                                nested_state
                            assert nested("SELECT id FROM events ORDER BY id") \
                                == ["10", "20", "20"]
                            assert nested("SELECT current_user") == [reader]
                            nested("COMMIT")
                        assert len(posts) == 1

                        # Scope rotation under a held snapshot (the
                        # expired-compatible-parent shape): the segments are
                        # sealed, so the reachable consistency error is the
                        # rotated generation — a reader holding the
                        # pre-rotation snapshot sees no anchor for the new
                        # binding in its statement snapshot and must report
                        # a consistency error by its first fetch, zero rows.
                        reclaim_reads = len(reads)
                        successor = str(uuid4())
                        with login(db, reader) as broken:
                            broken("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                            broken("SELECT 1 AS held")
                            sql(db, f"""
                                SELECT lakehouse.revoke_managed_capture('events', '{binding}', 1);
                                SELECT lakehouse.enroll_managed_capture('events',
                                    '{namespace}.events', 'cut_catalog', '{incarnation}',
                                    '{successor}', 2, '{authority}', '{table}',
                                    '{root.rstrip("/")}', 0, 0);
                            """)
                            install_reader_grant(db, successor, reader, prefix,
                                                 server="reader_credentials_v3")
                            broken("SAVEPOINT rotated")
                            broken("SELECT id FROM events ORDER BY id", sqlstate="55000")
                            broken("ROLLBACK TO SAVEPOINT rotated")
                            assert broken("SELECT current_user") == [reader]
                            broken("COMMIT")
                        assert len(posts) == 1
                        print(json.dumps({"case": "nested-spi-denials-rotated-scope",
                                          "reader_catalog_posts": 0,
                                          "rotated_scope_sqlstate": "55000",
                                          "reader_catalog_gets_during_consistency_error": len(reads) - reclaim_reads}), flush=True)

    trino(f"DROP TABLE IF EXISTS {relation}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", required=True, type=Path)
    args = parser.parse_args()
    if not __debug__:
        raise RuntimeError("the SQL drill requires Python assertions")
    namespace = "managed_sql_" + uuid4().hex[:12]
    writer, reader = namespace + "_writer", namespace + "_reader"
    owner = sql("postgres", "SELECT current_user")
    trino(f"CREATE SCHEMA iceberg.{namespace}")
    try:
        sql("postgres", f"CREATE DATABASE {namespace} TEMPLATE template0 ENCODING 'UTF8' LOCALE 'C'")
        sql(namespace, f"CREATE ROLE {writer}; CREATE ROLE {reader} LOGIN")
        try:
            exercise(namespace, namespace, writer, reader, owner, args.worker.resolve())
        finally:
            sql("postgres", f"DROP DATABASE {namespace} WITH (FORCE)")
            sql("postgres", f"DROP ROLE IF EXISTS {writer}, {reader}")
    finally:
        trino(f"DROP TABLE IF EXISTS iceberg.{namespace}.events; DROP SCHEMA iceberg.{namespace}")


if __name__ == "__main__":
    main()
