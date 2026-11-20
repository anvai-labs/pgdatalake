-- TDD anchor for M2 slice 26: the `lakehouse` TABLE access method. Managed
-- tables are ordinary pg_class rows with relkind 'r' (one-catalog visibility,
-- 00-goals #3) created via core's native CREATE TABLE ... USING syntax — no
-- grammar or catalog-boilerplate changes. The AM is created at extension
-- install time by CREATE ACCESS METHOD (runtime pg_am row; the M2 plan's
-- pg_am.dat [core-allowlist] edit turned out unnecessary).
CREATE EXTENSION lakehouse;

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
                              'analytics.events',
                              endpoint => 'http://127.0.0.1:9',
                              s3_endpoint => 'http://s3:9000',
                              s3_access_key => 'key',
                              s3_secret_key => 'secret');
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
                              'analytics.events_v2',
                              endpoint => 'http://127.0.0.1:9');
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
