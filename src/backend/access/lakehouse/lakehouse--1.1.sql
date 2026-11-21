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
-- SECURITY (13-review blocker #6): scan_files takes a caller-chosen
-- endpoint and credentials — an arbitrary-egress primitive — so EXECUTE
-- is revoked from PUBLIC; superusers grant it explicitly.
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

REVOKE EXECUTE ON FUNCTION lakehouse.scan_files(text, text, text, text, text,
                                                text, text, text) FROM PUBLIC;

-- Option validator (13-review blocker #6, doc 09 authority model): each
-- option has exactly one legal home — transport on SERVERS, secrets on
-- USER MAPPINGS, relation identity/pin on FOREIGN TABLES. Unknown and
-- misplaced options are rejected at DDL (and again on use, so legacy
-- pre-validator definitions cannot silently use unmapped credentials).
CREATE FUNCTION lakehouse.lakehouse_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME', 'lakehouse_fdw_validator'
LANGUAGE C STRICT;

-- M1 slice 11: the foreign-data wrapper handler (doc 01 D1: external
-- tables attach via the FDW and appear as ordinary pg_class rows).
CREATE FUNCTION lakehouse.lakehouse_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME', 'lakehouse_fdw_handler'
LANGUAGE C;

-- the canonical wrapper: ships WITH the validator
CREATE FOREIGN DATA WRAPPER lakehouse_fdw
    HANDLER lakehouse.lakehouse_fdw_handler
    VALIDATOR lakehouse.lakehouse_fdw_validator;

-- M2 slice 26: the `USING lakehouse` TABLE access method (doc 01 D2,
-- doc 02 §3). Runtime CREATE ACCESS METHOD — no bootstrap-catalog edits.
CREATE FUNCTION lakehouse.lakehouse_tableam_handler(internal)
RETURNS table_am_handler
AS 'MODULE_PATHNAME', 'lakehouse_tableam_handler'
LANGUAGE C;

-- Managed-table identity rows (doc 07 §2's pg_lakehouse_table). One row
-- per managed relation: relid is the ordinary pg_class oid, so one
-- catalog, one visibility (00-goals #3).
-- SECURITY (doc 09, 13-review blocker #6): NO credential values here or
-- in WITH-options — pg_class.reloptions and this table are dumpable.
-- Endpoint/warehouse/s3-endpoint live on a lakehouse FOREIGN SERVER;
-- credentials (token, s3 keys) live in USER MAPPINGS for that server
-- (the postgres_fdw authority model: per-user, ACL-controlled, one
-- place to rotate).
CREATE TABLE lakehouse.managed_tables (
    relid        oid PRIMARY KEY,
    identity     text NOT NULL,          -- iceberg 'namespace.table'
    serverid     oid NOT NULL,           -- FDW server: endpoint + mappings
    snapshot_id  text                    -- time-travel pin (slice 18)
);

CREATE FUNCTION lakehouse.attach_table(target regclass,
                                       identity text,
                                       server text)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    srv oid;
BEGIN
    SELECT s.oid INTO srv FROM pg_foreign_server s
    WHERE s.srvname = server::name;
    IF srv IS NULL THEN
        RAISE EXCEPTION 'server "%" does not exist', server
            USING ERRCODE = '42704';
    END IF;
    INSERT INTO lakehouse.managed_tables AS mt
        (relid, identity, serverid, snapshot_id)
    VALUES (target, identity, srv, NULL)
    ON CONFLICT (relid) DO UPDATE
    SET identity = EXCLUDED.identity,
        serverid = EXCLUDED.serverid,
        snapshot_id = EXCLUDED.snapshot_id;
END $$;

-- Managed-table hygiene: DROP TABLE removes the identity row (table AMs
-- get no drop hook, so a sql_drop event trigger does the GC).
CREATE FUNCTION lakehouse.managed_tables_drop_gc() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lakehouse AS $$
BEGIN
    DELETE FROM lakehouse.managed_tables
    WHERE relid IN (
        SELECT objid::oid FROM pg_event_trigger_dropped_objects()
        WHERE object_type = 'table');
END $$;

CREATE EVENT TRIGGER lakehouse_drop_gc ON sql_drop
    EXECUTE FUNCTION lakehouse.managed_tables_drop_gc();

CREATE ACCESS METHOD lakehouse TYPE TABLE HANDLER lakehouse.lakehouse_tableam_handler;

-- Managed CREATE (slice 27c): create the remote Iceberg table for an
-- attached managed relation, schema taken from the relation's own
-- tupdesc. Flow: CREATE TABLE ... USING lakehouse -> attach_table ->
-- create_remote -> INSERT.
CREATE FUNCTION lakehouse.create_remote(target regclass)
RETURNS text
AS 'MODULE_PATHNAME', 'lakehouse_create_remote'
LANGUAGE C VOLATILE;

-- SECURITY (13-review blocker #6): create_remote issues external DDL —
-- remote creation requires owner or delegated management authority
-- (checked in the C entry), so EXECUTE is not PUBLIC by default.
REVOKE EXECUTE ON FUNCTION lakehouse.create_remote(regclass) FROM PUBLIC;


-- P3 (spec 15 §7.2, D1-rev): the staged-delta store IS a Postgres
-- table — durability, crash recovery, replication and basebackup are
-- inherited from the heap/WAL machinery; the publish ledger is the
-- row's own columns (published / published_snapshot_id). Payload is a
-- typed batch (the accumulator representation, never re-encoded).
CREATE TABLE lakehouse.lakehouse_delta_segments (
    seg_id                 bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    relid                  oid NOT NULL,
    payload                bytea NOT NULL,
    tombstones             jsonb,
    staged_at              timestamptz NOT NULL DEFAULT now(),
    published              boolean NOT NULL DEFAULT false,
    published_snapshot_id  bigint
);
CREATE INDEX lakehouse_delta_unpublished_idx
    ON lakehouse.lakehouse_delta_segments (relid, staged_at)
    WHERE NOT published;
REVOKE ALL ON lakehouse.lakehouse_delta_segments FROM PUBLIC;

-- P4 (spec 15 §4): publisher entry point — emit ALL unpublished delta
-- segments of a managed table as files in ONE append snapshot, then
-- flip each segment's ledger columns. Callable by the owner; the
-- bgworker wraps this (slice P4b).
CREATE FUNCTION lakehouse.publish_staged(target regclass)
RETURNS integer
AS 'MODULE_PATHNAME', 'pg_lakehouse_publish_staged'
LANGUAGE C VOLATILE;
