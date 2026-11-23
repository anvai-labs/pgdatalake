"""Exercise bounded, read-only discovery against a disposable publication ledger."""

import argparse
import json
from importlib import import_module

from publication_fixture import AUTH, HDR, MEM, SEG, bound, claim, sql

admission = import_module("publication-admission-check")
CLAIMS = "lakehouse.discover_managed_publication_claims"
SEGMENTS = "lakehouse.discover_managed_publication_segments"


def page(db, function, cursor=None, limit=1):
    after = "NULL" if cursor is None else f"'{cursor}'"
    return json.loads(sql(db, f"""
        SELECT COALESCE(json_agg(j), '[]'::json)
        FROM {function}({after}, {limit}) j
    """))


def check_discovery_cost(db):
    # Include a long completed history to catch regressions in the inner
    # page queries, including membership probes that hash the whole ledger.
    history = 4096
    sql(db, f"""
        INSERT INTO {SEG} (seg_id, relid, payload, row_count, authority_id, table_uuid,
                           published, published_snapshot_id)
        OVERRIDING SYSTEM VALUE
        SELECT n, 'legacy_src'::regclass, '[[60]]', 1, '{AUTH}', md5(n::text)::uuid, true, 100
        FROM generate_series(100000, {99999 + history}) n;
        INSERT INTO {HDR} (operation_id, authority_id, table_uuid, relid, input_digest, verified_snapshot_id)
        SELECT table_uuid, authority_id, table_uuid, relid, payload_digest, 100
        FROM {SEG} WHERE seg_id >= 100000;
        INSERT INTO {MEM} (seg_id, operation_id)
        SELECT seg_id, table_uuid FROM {SEG} WHERE seg_id >= 100000;
        ANALYZE {HDR};
        ANALYZE {MEM};
        ANALYZE {SEG};
    """)
    # These statistics belong to the same backend and transaction that ran
    # discovery, and include the queries inside its PL/pgSQL functions.
    result = sql(db, f"""
        BEGIN READ ONLY;
        SELECT count(*) FROM {CLAIMS}(NULL, 1);
        SELECT count(*) FROM {SEGMENTS}(NULL, 1);
        SELECT COALESCE(sum(seq_tup_read), 0)
        FROM pg_stat_xact_user_tables
        WHERE schemaname = 'lakehouse'
          AND relname IN ('publication_claims', 'publication_members', 'lakehouse_delta_segments');
        ROLLBACK;
    """).splitlines()
    assert result[:2] == ["1", "1"] and int(result[2]) < 64, result
    print(f"completed history: {history}; claim/segment page sizes: 1/1; sequential tuples read: {result[2]}")


def exercise(db, role, baseline):
    sql(db, f"""
        CREATE EXTENSION lakehouse;
        CREATE ROLE {role};
        GRANT USAGE ON SCHEMA lakehouse TO {role};
        CREATE SERVER publication_catalog FOREIGN DATA WRAPPER lakehouse_fdw
            OPTIONS (endpoint '{admission.CATALOG}', warehouse 'fixture',
                     s3_endpoint '{admission.OBJECTS}');
        CREATE TABLE legacy_src (id bigint);
    """)
    sql(db, bound(90000, "legacy_src"))
    sql(db, claim(ids="90000", relation="legacy_src"))
    sql(db, "ALTER EXTENSION lakehouse UPDATE TO '1.8';")
    if baseline:
        for function in (CLAIMS, SEGMENTS):
            sql(db, f"SELECT * FROM {function}(NULL, 1)", error="42883")
        print("expected red: 1.8 has no managed discovery cursors")
        return
    sql(db, "ALTER EXTENSION lakehouse UPDATE TO '1.9';")

    for function in (CLAIMS, SEGMENTS):
        sql(db, f"SET ROLE {role}; SELECT * FROM {function}(NULL, 1);", error="42501")
        for invalid in ("NULL", "-1", "0", "1025"):
            sql(db, f"SELECT * FROM {function}(NULL, {invalid});", error="22023")
        assert page(db, function) == []

    authority = admission.identifier()
    for name in ("orders", "receipts", "inventory", "order_alias"):
        sql(db, f"CREATE TABLE {name} (id bigint) USING lakehouse WITH (write_mode='staged');")

    orders_table = admission.identifier()
    orders_incarnation = admission.identifier()
    old = admission.enroll(db, "orders", orders_incarnation, 1, authority, orders_table)
    retired = admission.capture(db, "orders", 10)
    sql(db, admission.grant(old))
    sql(db, f"SELECT lakehouse.revoke_managed_capture('orders', '{old}', 1)")
    orders = admission.enroll(db, "orders", orders_incarnation, 2, authority, orders_table)
    send = admission.capture(db, "orders", 20, 20)
    late = admission.capture(db, "orders", 30)
    receipts_table = admission.identifier()
    receipts = admission.enroll(db, "receipts", admission.identifier(), 1, authority, receipts_table)
    verify = admission.capture(db, "receipts", 40)
    inventory_table = admission.identifier()
    inventory = admission.enroll(db, "inventory", admission.identifier(), 1, authority, inventory_table)
    independent = admission.capture(db, "inventory", 50)

    alias = admission.identifier()
    sql(db, f"""
        SELECT lakehouse.enroll_managed_capture(
            'order_alias', 'fixture.stage.orders', 'publication_catalog',
            '{admission.identifier()}', '{alias}', 1, '{authority}', '{orders_table}',
            's3://fixture-bucket/orders', 0, 0);
    """)
    alias_seg = admission.capture(db, "order_alias", 60)

    assert page(db, SEGMENTS, limit=1024) == [
        {"segment_id": seg_id, "binding_id": binding, "row_count": rows,
         "payload_bytes": len(payload), "eligible": False}
        for seg_id, binding, rows, payload in (
            (retired, old, 1, "[[10]]"),
            (send, orders, 2, "[[20],[20]]"),
            (late, orders, 1, "[[30]]"),
            (verify, receipts, 1, "[[40]]"),
            (independent, inventory, 1, "[[50]]"),
            (alias_seg, alias, 1, "[[60]]"),
        )
    ]
    for binding in (orders, receipts, inventory, alias):
        sql(db, admission.grant(binding))
    assert [(row["segment_id"], row["eligible"]) for row in page(db, SEGMENTS, limit=2)] == [
        (retired, False), (send, True),
    ]
    assert [row["segment_id"] for row in page(db, SEGMENTS, send, 2)] == [late, verify]
    assert page(db, SEGMENTS, independent)[0]["eligible"]
    assert page(db, SEGMENTS, alias_seg) == []

    send_op = "00000000-0000-0000-0000-000000000000"
    lookup_op = "00000000-0000-0000-0000-000000000001"
    held_op = "00000000-0000-0000-0000-000000000002"
    sql(db, admission.managed_claim(send_op, send))
    assert page(db, SEGMENTS, verify)[0]["eligible"]
    assert not page(db, SEGMENTS, independent)[0]["eligible"]
    sql(db, admission.managed_claim(admission.identifier(), alias_seg), error="40001")
    digest = sql(db, admission.managed_claim(lookup_op, verify))
    sql(db, admission.managed_claim(held_op, independent))
    admission.acquire(db, lookup_op)
    call, proposal = admission.authorization(lookup_op, digest, 101)
    assert int(sql(db, call)) > 0
    sql(db, admission.revoke(receipts) + admission.revoke(inventory))
    # A retired relation remains recoverable through the retained attempt.
    sql(db, "DROP TABLE receipts;")

    assert page(db, CLAIMS) == [
        {"operation_id": send_op, "owner_generation": 0, "binding_id": orders, "mode": "send"},
    ]
    assert page(db, CLAIMS, send_op, 2) == [
        {"operation_id": lookup_op, "owner_generation": 1, "binding_id": receipts, "mode": "lookup"},
        {"operation_id": held_op, "owner_generation": 0, "binding_id": inventory, "mode": "hold"},
    ]
    assert page(db, CLAIMS, held_op) == []
    assert all(not row["eligible"] for row in page(db, SEGMENTS, limit=1024))
    assert sql(db, f"SELECT count(*) FROM {HDR} WHERE owner_generation = 0") == "3"

    admission.acquire(db, send_op)
    sql(db, admission.revoke(orders))
    sql(db, admission.send_binding(send_op), error="55000")
    assert page(db, CLAIMS)[0]["mode"] == "hold"
    assert sql(db, admission.finalize(lookup_op, authority, receipts_table, digest, 101, proposal)) == "1"
    assert [row["operation_id"] for row in page(db, CLAIMS, limit=1024)] == [send_op, held_op]
    assert [row["segment_id"] for row in page(db, SEGMENTS, limit=1024)] == [
        retired, send, late, independent, alias_seg,
    ]
    assert sql(db, f"SELECT count(*) FROM {MEM}") == "4"
    check_discovery_cost(db)
    print("managed publication discovery: bounded pages, immutable generation and lookup after revoke/drop verified")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", action="store_true", help="verify the discovery API is missing on 1.8")
    args = parser.parse_args()
    if not __debug__:
        raise RuntimeError("fixture requires assertions")
    db = f"discovery_{admission.identifier().replace('-', '')}"
    role = f"{db}_writer"
    sql("postgres", f"CREATE DATABASE {db}")
    try:
        exercise(db, role, args.baseline)
    finally:
        sql("postgres", f"DROP DATABASE {db} WITH (FORCE); DROP ROLE IF EXISTS {role}")


if __name__ == "__main__":
    main()
