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

-- M2 slice 26: the `USING lakehouse` TABLE access method (doc 01 D2,
-- doc 02 §3). Runtime CREATE ACCESS METHOD — no bootstrap-catalog edits.
CREATE FUNCTION lakehouse.lakehouse_tableam_handler(internal)
RETURNS table_am_handler
AS 'MODULE_PATHNAME', 'lakehouse_tableam_handler'
LANGUAGE C;

-- Managed-table identity rows (doc 07 §2's pg_lakehouse_table; the M2
-- plan's CREATE-time wiring lands in slice 27 — attach explicitly until
-- then). One row per managed relation: relid is the ordinary pg_class oid,
-- so one catalog, one visibility (00-goals #3).
CREATE TABLE lakehouse.managed_tables (
    relid        oid PRIMARY KEY,
    identity     text NOT NULL,          -- iceberg 'namespace.table'
    endpoint     text NOT NULL,
    warehouse    text,
    token        text,
    s3_endpoint  text,
    s3_access_key text,
    s3_secret_key text,
    snapshot_id  text                    -- time-travel pin (slice 18)
);

CREATE FUNCTION lakehouse.attach_table(target regclass,
                                       identity text,
                                       endpoint text,
                                       warehouse text DEFAULT NULL,
                                       token text DEFAULT NULL,
                                       s3_endpoint text DEFAULT NULL,
                                       s3_access_key text DEFAULT NULL,
                                       s3_secret_key text DEFAULT NULL,
                                       snapshot_id text DEFAULT NULL)
RETURNS void LANGUAGE sql AS $$
    INSERT INTO lakehouse.managed_tables AS mt
        (relid, identity, endpoint, warehouse, token,
         s3_endpoint, s3_access_key, s3_secret_key, snapshot_id)
    VALUES (target, identity, endpoint, warehouse, token,
            s3_endpoint, s3_access_key, s3_secret_key, snapshot_id)
    ON CONFLICT (relid) DO UPDATE
    SET identity = EXCLUDED.identity,
        endpoint = EXCLUDED.endpoint,
        warehouse = EXCLUDED.warehouse,
        token = EXCLUDED.token,
        s3_endpoint = EXCLUDED.s3_endpoint,
        s3_access_key = EXCLUDED.s3_access_key,
        s3_secret_key = EXCLUDED.s3_secret_key,
        snapshot_id = EXCLUDED.snapshot_id;
$$;

-- Managed-table hygiene: DROP TABLE removes the identity row (table AMs
-- get no drop hook, so a sql_drop event trigger does the GC).
CREATE FUNCTION lakehouse.managed_tables_drop_gc() RETURNS event_trigger
LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM lakehouse.managed_tables
    WHERE relid IN (
        SELECT objid::oid FROM pg_event_trigger_dropped_objects()
        WHERE object_type = 'table');
END $$;

CREATE EVENT TRIGGER lakehouse_drop_gc ON sql_drop
    EXECUTE FUNCTION lakehouse.managed_tables_drop_gc();

CREATE ACCESS METHOD lakehouse TYPE TABLE HANDLER lakehouse.lakehouse_tableam_handler;
