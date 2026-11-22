#!/bin/sh
# Requires the Lakekeeper/S3/Trino fixtures and a disposable PostgreSQL
# database with lakehouse installed or available. psql uses the PG* environment.
set -eu

name=cow_guard_$(date +%s)_$$
table=iceberg.$name.events
export TRINO_CONTAINER=${TRINO_CONTAINER:-pgdatalake-trino-1}
trino() {
    docker exec "$TRINO_CONTAINER" trino --output-format TSV --execute "$1"
}

trino "CREATE SCHEMA iceberg.$name"
trap 'trino "DROP TABLE IF EXISTS $table; DROP TABLE IF EXISTS iceberg.$name.clean; DROP SCHEMA iceberg.$name"' EXIT
trino "CREATE TABLE $table (id bigint) WITH (format_version = 2);
       INSERT INTO $table VALUES 10, 20, 30;
       DELETE FROM $table WHERE id = 20;
       CREATE TABLE iceberg.$name.clean (id bigint) WITH (format_version = 2);
       INSERT INTO iceberg.$name.clean VALUES 10, 20, 30"
# Prove that this fixture actually exercised a row-level position delete.
test "$(trino "SELECT count(*) FROM iceberg.$name.\"events\$files\" WHERE content = 1")" -ge 1
snapshot=$(trino "SELECT snapshot_id FROM iceberg.$name.\"events\$history\" ORDER BY made_current_at DESC LIMIT 1")

"${PSQL:-psql}" -X -v ON_ERROR_STOP=1 <<SQL
BEGIN;
CREATE EXTENSION IF NOT EXISTS lakehouse;
CREATE FOREIGN DATA WRAPPER $name HANDLER lakehouse.lakehouse_fdw_handler;
CREATE SERVER $name FOREIGN DATA WRAPPER $name OPTIONS
    (endpoint 'http://localhost:8181/catalog', warehouse 'smoke', s3_endpoint 'http://localhost:9000');
CREATE USER MAPPING FOR CURRENT_USER SERVER $name OPTIONS
    (s3_access_key 'lakehouse-test', s3_secret_key 'lakehouse-test-password');
CREATE TABLE $name (id bigint) USING lakehouse;
SELECT lakehouse.attach_table('$name'::regclass, '$name.events', '$name');
DO \$\$
BEGIN
    IF (SELECT array_agg(id ORDER BY id) FROM $name) IS DISTINCT FROM ARRAY[10,30]::bigint[] THEN
        RAISE EXCEPTION 'precondition failed: managed scan did not apply the existing position delete';
    END IF;
    BEGIN
        DELETE FROM $name WHERE id = 30;
        RAISE EXCEPTION 'DELETE accepted a delete-bearing rewrite';
    EXCEPTION WHEN feature_not_supported THEN
        IF SQLERRM NOT LIKE '%Iceberg delete%' THEN RAISE; END IF;
    END;
    BEGIN
        UPDATE $name SET id = 31 WHERE id = 30;
        RAISE EXCEPTION 'UPDATE accepted a delete-bearing rewrite';
    EXCEPTION WHEN feature_not_supported THEN
        IF SQLERRM NOT LIKE '%Iceberg delete%' THEN RAISE; END IF;
    END;
END \$\$;
CREATE TABLE ${name}_clean (id bigint) USING lakehouse;
SELECT lakehouse.attach_table('${name}_clean'::regclass, '$name.clean', '$name');
DELETE FROM ${name}_clean WHERE id = 30;
-- The source file is still live after the external writer adds a position delete.
\! docker exec "\$TRINO_CONTAINER" trino --execute "DELETE FROM iceberg.$name.clean WHERE id = 20"
\set ON_ERROR_STOP off
COMMIT;
\set commit_state :SQLSTATE
\set ON_ERROR_STOP on
SELECT :'commit_state' = '40001' AS rejected \gset
\if :rejected
\echo concurrent change rejected with SQLSTATE :commit_state
\else
DO \$\$ BEGIN RAISE EXCEPTION 'concurrent change did not cause a serialization failure'; END \$\$;
\endif
SQL
test "$(trino "SELECT snapshot_id FROM iceberg.$name.\"events\$history\" ORDER BY made_current_at DESC LIMIT 1")" = "$snapshot"
test "$(trino "SELECT array_join(array_agg(id ORDER BY id), ',') FROM $table")" = "10,30"
test "$(trino "SELECT array_join(array_agg(id ORDER BY id), ',') FROM iceberg.$name.clean")" = "10,30"
echo "COW guard passed: snapshot $snapshot retained rows 10,30; competing delete won on second table"
