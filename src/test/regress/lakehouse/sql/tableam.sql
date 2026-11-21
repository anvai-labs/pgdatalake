-- TDD anchor for M2 slice 26: the `lakehouse` TABLE access method. Managed
-- tables are ordinary pg_class rows with relkind 'r' (one-catalog visibility,
-- 00-goals #3) created via core's native CREATE TABLE ... USING syntax — no
-- grammar or catalog-boilerplate changes. The AM is created at extension
-- install time by CREATE ACCESS METHOD (runtime pg_am row; the M2 plan's
-- pg_am.dat [core-allowlist] edit turned out unnecessary).
CREATE EXTENSION lakehouse;

-- canonical server + user mapping (credential authority, doc 09):
-- endpoints on the SERVER, secrets in USER MAPPINGS, never in table
-- options or the attach row (both are dumpable)
CREATE FOREIGN DATA WRAPPER lakehouse_w HANDLER lakehouse.lakehouse_fdw_handler;
CREATE SERVER lakehouse_srv FOREIGN DATA WRAPPER lakehouse_w
    OPTIONS (endpoint 'http://127.0.0.1:9', warehouse 'wh',
             s3_endpoint 'http://s3:9000');
CREATE USER MAPPING FOR CURRENT_USER SERVER lakehouse_srv
    OPTIONS (s3_access_key 'key', s3_secret_key 'secret');

-- the access method exists, type 't' (table), handler in the lakehouse schema
SELECT amname, amtype, p.oid::regprocedure AS handler
FROM pg_am a JOIN pg_proc p ON p.oid = a.amhandler
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE amname = 'lakehouse';

-- managed table round-trip: ordinary relation row, relam = lakehouse AM
CREATE TABLE lakehouse_mt (id bigint, name text) USING lakehouse;

SELECT c.relkind, a.amname
FROM pg_class c JOIN pg_am a ON a.oid = c.relam
WHERE c.relname = 'lakehouse_mt';

-- default table access method can select the AM per database (doc 02 §3)
SET default_table_access_method = 'lakehouse';
CREATE TABLE lakehouse_mt2 (id bigint);
SELECT c.relkind, a.amname
FROM pg_class c JOIN pg_am a ON a.oid = c.relam
WHERE c.relname = 'lakehouse_mt2';
SET default_table_access_method = DEFAULT;

-- scans: identity comes from lakehouse.managed_tables (attached by
-- lakehouse.attach_table; CREATE-time wiring lands with managed DDL in
-- slice 27). An un-attached managed table errors cleanly, not by crash.
DO $$
DECLARE
    msg text;
BEGIN
    PERFORM * FROM lakehouse_mt;
    RAISE EXCEPTION 'unattached managed scan did not error';
EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS msg = MESSAGE_TEXT;
    IF msg NOT LIKE '%no lakehouse identity%' THEN
        RAISE EXCEPTION 'unexpected error: %', msg;
    END IF;
END $$;

-- with an identity attached, scans delegate to the FDW session pipeline:
-- against an unreachable REST endpoint the scan fails fast with
-- connection_exception (08000), exactly like the foreign-table path.
SELECT lakehouse.attach_table('lakehouse_mt'::regclass,
                              'analytics.events', 'lakehouse_srv');
DO $$
DECLARE
    msg text;
    st text;
BEGIN
    PERFORM * FROM lakehouse_mt;
    RAISE EXCEPTION 'unreachable-endpoint scan did not error';
EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS msg = MESSAGE_TEXT, st = RETURNED_SQLSTATE;
    IF st <> '08000' THEN
        RAISE EXCEPTION 'unexpected SQLSTATE %', st;
    END IF;
END $$;

-- writes ARE live since 27b: staging requires identity (0A000), then a
-- reachable catalog (08000 at session open, same fail-fast as scans)
DO $$
DECLARE
    st text;
BEGIN
    INSERT INTO lakehouse_mt2 VALUES (1);   -- never attached
    RAISE EXCEPTION 'unattached insert did not error';
EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS st = RETURNED_SQLSTATE;
    IF st <> '0A000' THEN
        RAISE EXCEPTION 'unexpected SQLSTATE %', st;
    END IF;
END $$;

DO $$
DECLARE
    st text;
BEGIN
    INSERT INTO lakehouse_mt VALUES (1, 'x');   -- unreachable endpoint
    RAISE EXCEPTION 'unreachable insert did not error';
EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS st = RETURNED_SQLSTATE;
    IF st <> '08000' THEN
        RAISE EXCEPTION 'unexpected SQLSTATE %', st;
    END IF;
END $$;

-- attach is idempotent per table (re-attach replaces the identity row)
SELECT lakehouse.attach_table('lakehouse_mt'::regclass,
                              'analytics.events_v2', 'lakehouse_srv');
SELECT identity FROM lakehouse.managed_tables
WHERE relid = 'lakehouse_mt'::regclass;

-- the companion catalog keeps referential sanity after DROP
DROP TABLE lakehouse_mt;
SELECT count(*) AS orphan_rows FROM lakehouse.managed_tables;

-- managed CREATE (27c): create_remote requires an identity first
DO $$
DECLARE
    st text;
BEGIN
    SELECT lakehouse.create_remote('lakehouse_mt2'::regclass);
    RAISE EXCEPTION 'unattached create_remote did not error';
EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS st = RETURNED_SQLSTATE;
    IF st <> '0A000' THEN
        RAISE EXCEPTION 'unexpected SQLSTATE %', st;
    END IF;
END $$;

-- non-managed relations are rejected
DO $$
DECLARE
    st text;
BEGIN
    CREATE TABLE plain_heap (x int);
    SELECT lakehouse.create_remote('plain_heap'::regclass);
    RAISE EXCEPTION 'heap create_remote did not error';
EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS st = RETURNED_SQLSTATE;
    IF st <> '42809' THEN
        RAISE EXCEPTION 'unexpected SQLSTATE %', st;
    END IF;
END $$;

-- managed CREATE via WITH-options (slice 28): the WITH block IS the
-- identity — no attach_table needed. Offline anchors use an unreachable
-- endpoint; the live pure-SQL cycle ran against the fixture stack.
CREATE TABLE lakehouse_wt (id bigint, name text) USING lakehouse
    WITH (endpoint = 'http://127.0.0.1:9',
          warehouse = 'wh',
          table = 'analytics.events',
          s3_endpoint = 'http://s3:9000');
SELECT c.relkind, a.amname FROM pg_class c JOIN pg_am a ON a.oid = c.relam
WHERE c.relname = 'lakehouse_wt';
-- scan fails fast (08000) straight off the WITH-options identity
DO $$
DECLARE
    st text;
BEGIN
    PERFORM * FROM lakehouse_wt;
    RAISE EXCEPTION 'WITH-options scan did not error';
EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS st = RETURNED_SQLSTATE;
    IF st <> '08000' THEN
        RAISE EXCEPTION 'unexpected SQLSTATE %', st;
    END IF;
END $$;

-- unknown WITH keys are rejected loudly at CREATE
DO $$
DECLARE
    st text;
BEGIN
    CREATE TABLE lakehouse_bad (id bigint) USING lakehouse
        WITH (endpiont = 'http://typo');
    RAISE EXCEPTION 'typo option did not error';
EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS st = RETURNED_SQLSTATE;
    IF st <> '22023' THEN
        RAISE EXCEPTION 'unexpected SQLSTATE %', st;
    END IF;
END $$;

-- missing table option rejected
DO $$
DECLARE
    st text;
BEGIN
    CREATE TABLE lakehouse_bad2 (id bigint) USING lakehouse
        WITH (endpoint = 'http://127.0.0.1:9');
    RAISE EXCEPTION 'missing table option did not error';
EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS st = RETURNED_SQLSTATE;
    IF st <> '22023' THEN
        RAISE EXCEPTION 'unexpected SQLSTATE %', st;
    END IF;
END $$;

DROP TABLE lakehouse_wt;

-- slice 28: UPDATE/DELETE on managed tables (copy-on-write). The offline
-- surface: against an unreachable catalog the DML scan fails fast with
-- connection_exception (08000) — no silent success, no partial state.
-- The full live pipeline (tombstones, PRE-COMMIT rewrite, overwrite
-- commit, independent-reader agreement) is the slice-28 live acceptance.
DO $$
DECLARE
    st text;
BEGIN
    DELETE FROM lakehouse_mt WHERE id = 1;
    RAISE EXCEPTION 'unreachable managed DELETE did not error';
EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS st = RETURNED_SQLSTATE;
    IF st <> '08000' THEN
        RAISE EXCEPTION 'unexpected SQLSTATE %', st;
    END IF;
END $$;

DO $$
DECLARE
    st text;
BEGIN
    UPDATE lakehouse_mt SET id = 2 WHERE id = 1;
    RAISE EXCEPTION 'unreachable managed UPDATE did not error';
EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS st = RETURNED_SQLSTATE;
    IF st <> '08000' THEN
        RAISE EXCEPTION 'unexpected SQLSTATE %', st;
    END IF;
END $$;

-- rows inserted in the same transaction are not addressable: the DML
-- scan errors instead of silently mis-targeting (tombstones reference
-- base-snapshot files; staged rows have no remote identity yet)
DO $$
DECLARE
    st text;
BEGIN
    BEGIN
        INSERT INTO lakehouse_mt VALUES (1, 'x');
        DELETE FROM lakehouse_mt WHERE id = 1;
        RAISE EXCEPTION 'staged-row DELETE did not error';
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS st = RETURNED_SQLSTATE;
        IF st <> '08000' THEN
            RAISE EXCEPTION 'unexpected SQLSTATE %', st;
        END IF;
        RAISE;
    END;
EXCEPTION WHEN OTHERS THEN
    NULL; -- the INSERT's connection failure is the expected offline surface
END $$;
