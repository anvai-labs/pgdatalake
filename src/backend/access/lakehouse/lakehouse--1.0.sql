/* Format-generic lakehouse storage engine support, v1.0 (M0 stub surface).
 *
 * specs/lakehouse/RULES.md §4: lakehouse-visible functions live in the
 * `lakehouse` schema. The full SQL surface (catalog management, time travel,
 * maintenance) grows this file / adds versioned scripts from M1 on.
 */

CREATE SCHEMA IF NOT EXISTS lakehouse;

CREATE FUNCTION lakehouse.version() RETURNS text
AS 'MODULE_PATHNAME', 'pg_lakehouse_version'
LANGUAGE C STABLE PARALLEL SAFE;

-- M1 slice 9: scan planning over a REST-catalog table (FFI-backed SRF).
CREATE FUNCTION lakehouse.scan_files(
    endpoint      text,
    warehouse     text,
    token         text,
    namespace     text,
    table_name    text,
    s3_endpoint   text default null,
    s3_access_key text default null,
    s3_secret_key text default null)
RETURNS TABLE(file_path text, record_count bigint)
AS 'MODULE_PATHNAME', 'pg_lakehouse_scan_files'
LANGUAGE C VOLATILE STRICT;

-- M1 slice 11: the foreign-data wrapper handler (doc 01 D1: external
-- tables attach via the FDW and appear as ordinary pg_class rows).
CREATE FUNCTION lakehouse.lakehouse_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME', 'lakehouse_fdw_handler'
LANGUAGE C;
