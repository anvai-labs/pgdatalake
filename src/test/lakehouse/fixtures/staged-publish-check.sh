#!/bin/sh
# Staged-publish acceptance (spec 15 §4, publisher append-extend pin):
# the coalescing publisher must turn N unpublished delta segments into
# ONE append snapshot — never a replacement. Pinned end-to-end against
# the LIVE fixture stack (Lakekeeper + SeaweedFS), the only way to prove
# remote emission:
#
#   1. create the remote Iceberg table via pyiceberg (empty)
#   2. stage TWO transactions (2 delta segments) against the live catalog
#   3. publish_staged -> must report 2 segments, ONE commit
#   4. ledger: both segments flipped, SAME published_snapshot_id
#   5. post-publish read-your-writes: rows now served from the remote
#      parquet (delta table empty), not from the delta segments
#   6. pyiceberg: exactly ONE snapshot, 5 rows
#
# Requires: docker fixtures up (make lakehouse-up), a postgres with the
# lakehouse extension reachable via psql. Defaults match the dev instance:
#   PGHOST/PGPORT/PGUSER/PGDATABASE  (localhost:52018 postgres/postgres)
#   PSQL    psql binary
#   PDI_VENV    host venv holding pyiceberg (default /tmp/pdi-venv); run
#               INSIDE the fixture docker network — the `smoke` warehouse's
#               storage profile carries the docker-internal S3 endpoint
#               (s3:9000), so host-side pyiceberg scans cannot reach it
#   PDI_NET     docker network (default: pgdatalake_default; auto-detected
#               via compose project label when docker is reachable)
# Re-runnable: cleans up its own objects first (pg table + remote table).
set -eu

PGHOST=${PGHOST:-localhost}
PGPORT=${PGPORT:-52018}
PGUSER=${PGUSER:-postgres}
PGDATABASE=${PGDATABASE:-postgres}
PSQL=${PSQL:-psql}
PDI_VENV=${PDI_VENV:-/tmp/pdi-venv}
# Makefile pins the compose project (-p pgdatalake), so the network name is
# deterministic; the label query handles custom COMPOSE_PROJECT_NAME setups.
PDI_NET=${PDI_NET:-$(docker network ls --filter label=com.docker.compose.project=pgdatalake --format '{{.Name}}' 2>/dev/null | head -1)}
PDI_NET=${PDI_NET:-pgdatalake_default}

# pyiceberg runner: image python + the host venv's site-packages via
# PYTHONPATH (version-matched wheels ride along; /venv/bin/python is a
# host symlink the image cannot exec)
[ -d "$PDI_VENV" ] || { echo "FAIL: PDI_VENV=$PDI_VENV does not exist (create the pyiceberg venv or point PDI_VENV at it)"; exit 1; }
PDI_SITE=$(ls -d "$PDI_VENV"/lib/python3.*/site-packages 2>/dev/null | head -1)
[ -n "$PDI_SITE" ] || { echo "FAIL: no lib/python3.*/site-packages under PDI_VENV=$PDI_VENV"; exit 1; }
# PYTHONPATH must be the CONTAINER path — the venv mounts at /venv, so the
# container sees /venv/lib/<py-minor>/site-packages, NOT the host path
# (a host PDI_SITE silently imports nothing: ModuleNotFoundError pyarrow)
PDI_PY_MINOR=$(basename "$(dirname "$PDI_SITE")")
PDI() {
    docker run --rm -i --network "$PDI_NET" \
        -v "$PDI_VENV":/venv \
        -e PYTHONPATH="/venv/lib/$PDI_PY_MINOR/site-packages" \
        python:3.12-slim python3 -
}

# one catalog-setup snippet, interpolated into every PDI block below
CAT_PY='from pyiceberg.catalog import load_catalog

cat = load_catalog("smoke", type="rest",
                   uri="http://lakekeeper:8181/catalog", warehouse="smoke",
                   **{"s3.endpoint": "http://s3:9000",
                      "s3.addressing-style": "path",
                      "s3.access-key-id": "lakehouse-test",
                      "s3.secret-access-key": "lakehouse-test-password"})
'
NS=pubcheck
TBL=staged_pub
REMTBL="$NS.$TBL"

echo "== staged-publish acceptance: $REMTBL =="
# in-container hostnames: lakekeeper:8181 / s3:9000 (docker-internal)
PDI <<PYEOF
import pyarrow as pa
from pyiceberg.exceptions import (NoSuchNamespaceError, NoSuchTableError,
                                  NamespaceAlreadyExistsError)
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType
$CAT_PY
try:
    cat.create_namespace("$NS")
except NamespaceAlreadyExistsError:
    pass
try:
    cat.drop_table("$REMTBL")
except (NoSuchTableError, NoSuchNamespaceError):
    pass
schema = Schema(NestedField(1, "id", LongType(), required=False),
                NestedField(2, "val", StringType(), required=False))
cat.create_table("$REMTBL", schema=schema,
                 properties={"format-version": "2"})
print("remote table created: $REMTBL (0 snapshots)")
PYEOF

psql_run() {
    "$PSQL" -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
        -v ON_ERROR_STOP=1 "$@"
}

# clean recreate: CASCADE drops any stale server/user-mapping OPTIONS, so
# edited credentials in this script always take effect on re-runs (the old
# idempotent-create guards silently kept the OLD options)
psql_run -q <<EOF
DROP TABLE IF EXISTS pub_$TBL CASCADE;
DROP FOREIGN DATA WRAPPER IF EXISTS pubcheck_wrapper CASCADE;
CREATE FOREIGN DATA WRAPPER pubcheck_wrapper
    HANDLER lakehouse.lakehouse_fdw_handler;
CREATE SERVER pubcheck_srv FOREIGN DATA WRAPPER pubcheck_wrapper
    OPTIONS (endpoint 'http://localhost:8181/catalog',
             warehouse 'smoke',
             s3_endpoint 'http://localhost:9000');
CREATE USER MAPPING FOR CURRENT_USER SERVER pubcheck_srv
    OPTIONS (s3_access_key 'lakehouse-test',
             s3_secret_key 'lakehouse-test-password');
CREATE TABLE pub_$TBL (id bigint, val text) USING lakehouse;
SELECT lakehouse.attach_table('pub_$TBL'::regclass, '$REMTBL', 'pubcheck_srv');
EOF

# --- publisher race guard ---------------------------------------------------
# the bgworker ("lakehouse publisher" in pg_stat_activity) loops
# publish_staged over every managed table; if a prior start_publisher() /
# preload left one running it can pre-publish or co-publish our segments and
# break every assertion below. This script wants the MANUAL path only.
PUBWORKERS=$(psql_run -tAq -c "SELECT count(*) FROM pg_stat_activity
    WHERE backend_type = 'bg worker' AND query LIKE '%publish%'")
[ "$PUBWORKERS" = "0" ] || { echo "FAIL: $PUBWORKERS lakehouse publisher bgworker(s) running — stop them (restart without lakehouse.publisher_enabled, or use a fresh instance) before running this acceptance"; exit 1; }
echo "publisher race guard: no bgworker running OK"

# --- stage TWO transactions: 2 delta segments, nothing remote yet ---------
psql_run -q <<EOF
SET lakehouse.write_mode = 'staged';
INSERT INTO pub_$TBL VALUES (1, 'one'), (2, 'two');
INSERT INTO pub_$TBL VALUES (3, 'three'), (4, 'four'), (5, 'five');
EOF

SEG=$(psql_run -tAq -c "SELECT count(*) FROM lakehouse.lakehouse_delta_segments
    WHERE relid = 'pub_$TBL'::regclass AND NOT published")
[ "$SEG" = "2" ] || { echo "FAIL: expected 2 unpublished segments, got $SEG"; exit 1; }
echo "pre-publish: 2 unpublished delta segments OK"

# --- publish: N segments -> ONE append commit ------------------------------
PUB=$(psql_run -tAq -c "SELECT lakehouse.publish_staged('pub_$TBL'::regclass)")
[ "$PUB" = "2" ] || { echo "FAIL: publish_staged reported $PUB segments, expected 2"; exit 1; }

LEDGER=$(psql_run -tAq -c "SELECT count(*) = 2 AND count(DISTINCT published_snapshot_id) = 1
    AND bool_and(published) AND bool_and(published_snapshot_id IS NOT NULL)
    FROM lakehouse.lakehouse_delta_segments
    WHERE relid = 'pub_$TBL'::regclass")
[ "$LEDGER" = "t" ] || { echo "FAIL: ledger flip wrong (want 2 rows, 1 snapshot id, all published): $LEDGER"; exit 1; }
echo "ledger: 2 segments flipped, SAME published_snapshot_id OK"

# --- post-publish read-your-writes: served from the REMOTE parquet --------
ROWS=$(psql_run -tAq -c "SELECT count(*) FROM pub_$TBL")
[ "$ROWS" = "5" ] || { echo "FAIL: post-publish scan returned $ROWS rows, expected 5"; exit 1; }
UNPUB=$(psql_run -tAq -c "SELECT count(*) FROM lakehouse.lakehouse_delta_segments
    WHERE relid = 'pub_$TBL'::regclass AND NOT published")
[ "$UNPUB" = "0" ] || { echo "FAIL: $UNPUB segments still unpublished"; exit 1; }
echo "post-publish scan: 5 rows from remote parquet, delta table drained OK"

# --- pyiceberg verdict: ONE snapshot carrying all 5 rows -------------------
PDI <<PYEOF
$CAT_PY
t = cat.load_table("$REMTBL")
snaps = t.metadata.snapshots
assert len(snaps) == 1, f"expected exactly ONE snapshot, got {len(snaps)}"
rows = t.scan().to_arrow().to_pydict()
ids = sorted(rows["id"])
assert ids == [1, 2, 3, 4, 5], f"remote rows wrong: {ids}"
print(f"remote: 1 snapshot, 5 rows OK (snapshot {snaps[0].snapshot_id})")
PYEOF

psql_run -q -c "DROP TABLE pub_$TBL CASCADE"
PDI <<PYEOF
from pyiceberg.exceptions import NoSuchTableError
$CAT_PY
try:
    cat.drop_table("$REMTBL")
except NoSuchTableError:
    print("cleanup: remote table already gone")
PYEOF

echo "staged-publish acceptance: ALL CHECKS PASSED"
