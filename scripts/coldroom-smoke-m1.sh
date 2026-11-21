#!/bin/bash
# M1 cold-room smoke ceremony (RULES §9) — proves milestone/m1 from a fresh
# clone + fresh fixture stack: build, upstream suite, lakehouse regress,
# live read path (PyIceberg fixtures), time-travel mechanics.
# Usage: COLDROOM=/tmp/coldroom-pg PYTHON_VENB=/path/to/venv bash scripts/coldroom-smoke-m1.sh
# (run from INSIDE the fresh clone root; expects `make` + docker + a pyiceberg venv
#  at PYTHON_VENB or build/venv — fixture generation needs pyiceberg+pyarrow)
set -x
COLDROOM="${COLDROOM:-/tmp/coldroom-pg}"
cd "$COLDROOM"

# 1. fresh fixture stack (tear down anything dev-shaped first)
docker compose -f src/test/lakehouse/docker-compose.yml -p pgdatalake down --volumes --remove-orphans >/dev/null 2>&1
make lakehouse-up || { echo "FAIL: lakehouse-up"; exit 1; }

# 2. build (meson-only tree, conda-gcc workaround; install: meson test runs
#    the INSTALLED binaries — the stale-tmp_install gotcha)
CC=/usr/bin/gcc ~/.local/bin/meson setup build -Ddatalake=enabled > /tmp/cr1-setup.log 2>&1 || { echo "FAIL: meson setup"; tail -5 /tmp/cr1-setup.log; exit 1; }
CC=/usr/bin/gcc ninja -C build > /tmp/cr1-ninja.log 2>&1 || { echo "FAIL: ninja"; tail -5 /tmp/cr1-ninja.log; exit 1; }
CC=/usr/bin/gcc meson install --destdir build/tmp_install > /dev/null 2>&1
echo "PASS: meson setup + ninja + install"

# 3. suites
~/.local/bin/meson test -C build --suite setup > /tmp/cr1-setup-suite.log 2>&1 || { echo "FAIL: setup suite"; exit 1; }
~/.local/bin/meson test -C build --suite regress > /tmp/cr1-regress.log 2>&1 || { echo "FAIL: upstream regress"; exit 1; }
~/.local/bin/meson test -C build --suite lakehouse > /tmp/cr1-lakehouse.log 2>&1 || { echo "FAIL: lakehouse regress"; exit 1; }
echo "PASS: setup + upstream regress + lakehouse regress (incl. time_travel)"

# 4. cargo unit tests
cargo test --manifest-path src/backend/access/lakehouse/core/pgdatalake-core/Cargo.toml > /tmp/cr1-cargo.log 2>&1 || { echo "FAIL: cargo test"; tail -20 /tmp/cr1-cargo.log; exit 1; }
echo "PASS: cargo tests"

# 5. live read path: generate PyIceberg fixtures and read them through the FDW.
# Fixture generation MUST run inside the compose network: lakekeeper serves a
# per-table s3.endpoint (http://s3:9000) that beats client config in pyiceberg
# (slice 20 gotcha). Uses the compose network + the pinned toolchain image with
# the host venv mounted at its absolute path (venv python symlinks into it).
run_py() {
  script="$1"; shift
  docker run --rm --network pgdatalake_default \
    -v "${PYTHON_VENB:-build/venv}":"${PYTHON_VENB:-build/venv}" \
    -v "${ANACONDA_DIR:-/home/vsingh/anaconda3}":"${ANACONDA_DIR:-/home/vsingh/anaconda3}" \
    -v "$PWD":/work -e LK_URI=http://lakekeeper:8181/catalog \
    --entrypoint "${PYTHON_VENB:-build/venv}/bin/python" \
    anvai-wsl-runner:ubuntu24-20261008 "/work/$script" "$@"
}
run_py scripts/gen_fixtures.py --uri http://lakekeeper:8181/catalog || { echo "FAIL: gen_fixtures"; exit 1; }
run_py scripts/gen_conformance_fixtures.py || { echo "FAIL: gen_conformance_fixtures"; exit 1; }
echo "PASS: fixture generation"

# 6. live SQL verification against a scratch cluster
PGBIN=build/tmp_install/usr/local/pgsql/bin
rm -rf /tmp/cr1-pgdata
LD_LIBRARY_PATH="$PGBIN/../lib:$PGBIN/../lib/x86_64-linux-gnu" $PGBIN/initdb -D /tmp/cr1-pgdata -U postgres --no-instructions > /dev/null 2>&1
cat >> /tmp/cr1-pgdata/postgresql.conf <<EOF
port = 52011
unix_socket_directories = '/tmp/cr1-pgdata'
dynamic_library_path = '$PWD/build/src/backend/access/lakehouse'
EOF
LD_LIBRARY_PATH="$PGBIN/../lib:$PGBIN/../lib/x86_64-linux-gnu" $PGBIN/pg_ctl -D /tmp/cr1-pgdata -l /tmp/cr1-pgdata/log start > /dev/null 2>&1 || { echo "FAIL: pg_ctl start"; exit 1; }
cleanup() { LD_LIBRARY_PATH="$PGBIN/../lib:$PGBIN/../lib/x86_64-linux-gnu" $PGBIN/pg_ctl -D /tmp/cr1-pgdata stop > /dev/null 2>&1; }
trap cleanup EXIT
LD_LIBRARY_PATH="$PGBIN/../lib:$PGBIN/../lib/x86_64-linux-gnu" $PGBIN/createdb -h /tmp/cr1-pgdata -p 52011 -U postgres lakehouse_verify
LD_LIBRARY_PATH="$PGBIN/../lib:$PGBIN/../lib/x86_64-linux-gnu" $PGBIN/psql -h /tmp/cr1-pgdata -p 52011 -U postgres -d lakehouse_verify -q > /tmp/cr1-live.txt 2>&1 <<'EOF' || { echo "FAIL: live read path"; cat /tmp/cr1-live.txt; exit 1; }
CREATE EXTENSION lakehouse;
CREATE FOREIGN DATA WRAPPER w HANDLER lakehouse.lakehouse_fdw_handler;
CREATE SERVER s FOREIGN DATA WRAPPER w OPTIONS
  (endpoint 'http://localhost:8181/catalog', warehouse 'smoke');
CREATE FOREIGN TABLE events (id bigint, "at" timestamptz) SERVER s OPTIONS
  (table 'analytics.events', s3_endpoint 'http://localhost:9000',
   s3_access_key 'lakehouse-test', s3_secret_key 'lakehouse-test-password');
-- read path: 3000 rows from 3 pyiceberg appends
DO $$ BEGIN
  IF (SELECT count(*) FROM events) <> 3000 THEN
    RAISE EXCEPTION 'live read path: expected 3000 rows';
  END IF;
END $$;
-- time travel mechanics: an unknown snapshot pin must fail cleanly (08000)
DO $$ BEGIN
  PERFORM count(*) FROM events FOR VERSION AS OF 999;
  RAISE EXCEPTION 'unknown snapshot pin did not error';
EXCEPTION WHEN OTHERS THEN
  IF SQLSTATE <> '08000' THEN RAISE EXCEPTION 'unexpected SQLSTATE %', SQLSTATE; END IF;
END $$;
-- conformance type matrix reads
CREATE FOREIGN TABLE types_all (
  b boolean, i int, l bigint, f real, d double precision,
  dec numeric(12,2), day date, t time, ts timestamp, tsz timestamptz,
  s text, u uuid, fx bytea, bin bytea, id int
) SERVER s OPTIONS
  (table 'analytics.types_all', s3_endpoint 'http://localhost:9000',
   s3_access_key 'lakehouse-test', s3_secret_key 'lakehouse-test-password');
DO $$ BEGIN
  IF (SELECT count(*) FROM types_all) <> 3 THEN
    RAISE EXCEPTION 'type matrix: expected 3 rows';
  END IF;
  IF (SELECT count(*) FROM types_all WHERE u IS NOT NULL AND u::text !~ '^[0-9a-f-]{36}$') <> 0 THEN
    RAISE EXCEPTION 'type matrix: uuid not canonical';
  END IF;
END $$;
EOF
echo "PASS: live read path + time travel mechanics + type matrix"

# 7. Trino interop (profile-gated; heavy — noted, does not block the gate)
echo "NOTE: Trino interop service available via: docker compose -p pgdatalake --profile trino up -d trino"

echo "COLDROOM M1 ALL GREEN"
