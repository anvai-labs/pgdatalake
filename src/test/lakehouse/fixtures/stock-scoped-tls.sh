#!/usr/bin/env bash
# Run the ordinary two-tenant scope fixture over authenticated PG-wire.
set -euo pipefail
umask 077
fixtures=$(cd "$(dirname "$0")" && pwd)
cluster=$(mktemp -d)
export PGHOST="$cluster" PGPORT="${SCOPED_TLS_PORT:-52594}" PGUSER=scope_admin PGDATABASE=postgres
export PGCONNECT_TIMEOUT=5 LC_ALL=C
unset PGHOSTADDR PGPASSWORD PGSERVICE PGSERVICEFILE
cleanup() {
    if [[ -e "$cluster/data/postmaster.pid" ]]; then
        pg_ctl -D "$cluster/data" -m fast -w stop || return 1
    fi
    rm -rf "$cluster"
}
trap cleanup EXIT
initdb -D "$cluster/data" --auth-local=trust --auth-host=scram-sha-256 \
    --encoding=UTF8 --no-locale -U "$PGUSER" >/dev/null
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj '/CN=localhost' \
    -addext 'subjectAltName=DNS:localhost' -keyout "$cluster/data/server.key" \
    -out "$cluster/data/server.crt" >/dev/null 2>&1
cat > "$cluster/data/pg_hba.conf" <<'HBA'
local   all   all                   trust
hostssl all   all   127.0.0.1/32    scram-sha-256
HBA
pg_ctl -D "$cluster/data" -l "$cluster/server.log" \
    -o "-k '$cluster' -p $PGPORT -c listen_addresses=127.0.0.1 -c ssl=on" -w start || { cat "$cluster/server.log" >&2; exit 1; }
python3 "$fixtures/scoped-read-check.py" --tls-root-cert "$cluster/data/server.crt"
