"""Exercise typed and bounded managed publisher ledger reads on a disposable cluster."""

import argparse
import hashlib
import json
from importlib import import_module

from publication_fixture import HDR, MEM, SEG, sql

admission = import_module("publication-admission-check")
JOB = "lakehouse.load_managed_publication_job"
MEMBERS = "lakehouse.load_managed_publication_members"


def job(db, op):
    payload = sql(db, f"SELECT row_to_json(j) FROM {JOB}('{op}') j;")
    return json.loads(payload) if payload else None


def members(db, op, generation, rows, size):
    return (
        f"SELECT segment_id, row_count, encode(payload, 'hex')"
        f" FROM {MEMBERS}('{op}', {generation}, {rows}, {size}) ORDER BY segment_id;"
    )


def exercise(db, role, baseline):
    sql(db, "CREATE EXTENSION lakehouse; ALTER EXTENSION lakehouse UPDATE TO '1.9';")
    if baseline:
        sql(db, f"SELECT * FROM {JOB}(NULL);", error="42883")
        sql(db, f"SELECT * FROM {MEMBERS}(NULL, 1, 1, 1);", error="42883")
        print("expected red: 1.9 lacks the typed managed ledger read APIs", flush=True)
        return
    sql(db, f"""
        ALTER EXTENSION lakehouse UPDATE TO '1.10';
        CREATE ROLE {role};
        CREATE SERVER publication_catalog FOREIGN DATA WRAPPER lakehouse_fdw
            OPTIONS (endpoint '{admission.CATALOG}', warehouse 'fixture',
                     s3_endpoint '{admission.OBJECTS}');
        CREATE TABLE items(value text) USING lakehouse WITH (write_mode='staged');
        ALTER TABLE {SEG} ALTER COLUMN payload SET STORAGE EXTERNAL;
    """)
    authority, remote, incarnation, op = (admission.identifier() for _ in range(4))
    binding = admission.enroll(db, "items", incarnation, 1, authority, remote)
    sql(db, admission.grant(binding))
    # Commit the small batch and the out-of-line payload in distinct transactions.
    sql(db, "INSERT INTO items VALUES ('a'), ('b');")
    sql(db, "INSERT INTO items SELECT string_agg(md5(i::text), '' ORDER BY i)"
            " FROM generate_series(1, 2048) AS s(i);")
    captured = sql(db, f"SELECT seg_id, row_count, encode(payload, 'hex') FROM {SEG} ORDER BY seg_id;")
    ids = [int(line.split("|", 1)[0]) for line in captured.splitlines()]
    assert len(ids) == 2
    toast = sql(db, f"SELECT reltoastrelid::regclass FROM pg_class WHERE oid='{SEG}'::regclass;")
    assert sql(db, f"SELECT count(*) > 0 FROM {toast};") == "t", "expected an external TOAST payload"
    payload_size = int(sql(db, f"SELECT sum(octet_length(payload)) FROM {SEG};"))
    assert payload_size > 65536

    digest = sql(db, admission.managed_claim(op, *ids))
    assert job(db, admission.identifier()) is None
    unsent = job(db, op)
    assert unsent == {
        "operation_id": op,
        "authority_id": authority,
        "table_uuid": remote,
        "binding_id": binding,
        "incarnation_id": incarnation,
        "binding_generation": 1,
        "owner_generation": 0,
        "input_digest": "\\x" + digest,
        "verified_snapshot_id": None,
        "attempt_owner_generation": None,
        "expected_parent": None,
        "proposed_snapshot_id": None,
        "proposal_digest": None,
        "proposal_body": None,
    }, unsent
    sql(db, f"SET ROLE {role}; SELECT * FROM {JOB}('{op}');", error="42501")
    sql(db, f"SET ROLE {role}; {members(db, op, 1, 3, payload_size)}", error="42501")
    sql(db, members(db, op, 0, 3, payload_size), error="40001")
    admission.acquire(db, op)
    sql(db, members(db, op, 1, 0, payload_size), error="22023")
    sql(db, members(db, op, 1, 3, "NULL"), error="22023")
    sql(db, members(db, op, 1, 2, payload_size), error="54000")
    sql(db, members(db, op, 1, 3, payload_size - 1), error="54000")
    assert sql(db, members(db, op, 1, 3, payload_size)) == captured
    assert sql(db, "SELECT count(*) FROM lakehouse.publication_attempts;") == "0"

    call, proposal = admission.authorization(op, digest, 101)
    assert int(sql(db, call)) > 0
    reserved = job(db, op)
    body_hex = reserved["proposal_body"].removeprefix("\\x")
    assert reserved["attempt_owner_generation"] == 1
    assert reserved["binding_generation"] == 1 and reserved["expected_parent"] == 100
    assert reserved["proposed_snapshot_id"] == 101 and reserved["verified_snapshot_id"] is None
    assert hashlib.sha256(bytes.fromhex(body_hex)).hexdigest() == proposal
    assert sql(db, f"SELECT lakehouse.advance_publication_generation('{op}', 1);") == "2"
    sql(db, members(db, op, 1, 3, payload_size), error="40001")
    sql(db, members(db, op, 2, 3, payload_size), error="55000")
    sql(db, admission.revoke(binding) + "DROP TABLE items;")
    assert job(db, op) == reserved | {"owner_generation": 2}
    finalized = admission.finalize(op, authority, remote, digest, 101, proposal).replace(", 1,", ", 2,", 1)
    assert sql(db, finalized) == "2"
    assert job(db, op) == reserved | {"owner_generation": 2, "verified_snapshot_id": 101}
    assert sql(db, f"SELECT count(*) FROM {MEM} WHERE operation_id='{op}';") == "2"
    assert sql(db, f"SELECT owner_generation FROM {HDR} WHERE operation_id='{op}';") == "2"
    print(f"managed ledger loads: {payload_size} logical bytes across 3 rows, exact budgets, "
          "stale owner and reserved send refused, original lookup bytes retained after revoke/drop", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", action="store_true")
    args = parser.parse_args()
    if not __debug__:
        raise RuntimeError("fixture requires assertions")
    db = f"ledger_load_{admission.identifier().replace('-', '')}"
    role = f"{db}_w"
    sql("postgres", f"CREATE DATABASE {db};")
    try:
        exercise(db, role, args.baseline)
    finally:
        sql("postgres", f"DROP DATABASE {db} WITH (FORCE); DROP ROLE IF EXISTS {role};")


if __name__ == "__main__":
    main()
