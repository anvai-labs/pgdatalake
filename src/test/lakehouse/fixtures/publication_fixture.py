"""SQL helpers and input fixtures for the PostgreSQL publication ledger smokes."""

import os
import subprocess
import uuid


AUTH, TABLE, OP, OTHER = [str(uuid.uuid4()) for _ in range(4)]
SEG = "lakehouse.lakehouse_delta_segments"
MEM = "lakehouse.publication_members"
HDR = "lakehouse.publication_claims"
PSQL = [os.getenv("PSQL", "psql"), "-XqAt", "-v", "ON_ERROR_STOP=1", "-v", "VERBOSITY=verbose"]


def sql(db, statement, error=None, app="ledger_check"):
    result = subprocess.run(PSQL + ["-d", db], input=statement, capture_output=True,
                            text=True, timeout=35, env=dict(os.environ, PGAPPNAME=app, PGCONNECT_TIMEOUT="5"))
    if error:
        assert result.returncode and f"ERROR:  {error}:" in result.stderr, result
    elif result.returncode:
        raise RuntimeError(result.stderr)
    return result.stdout.strip()


def claim(op=OP, ids="7,8", relation="events", authority=AUTH, table=TABLE):
    return (f"SELECT encode(lakehouse.claim_publication('{op}', '{authority}', '{table}', "
            f"'{relation}'::regclass, ARRAY[{ids}]::bigint[]), 'hex');")


def bound(seg, relation="events", table=TABLE):
    return (f"INSERT INTO {SEG} (seg_id, relid, payload, row_count, authority_id, table_uuid) "
            f"OVERRIDING SYSTEM VALUE VALUES ({seg}, '{relation}'::regclass, '[[20]]', "
            f"1, '{AUTH}', '{table}');")
