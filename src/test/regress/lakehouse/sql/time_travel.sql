-- TDD anchor for M1 slice 19: lakehouse time travel — SELECT ... FOR
-- TIMESTAMP/VERSION/TAG AS OF (specs/lakehouse/04-sql.md §3). The clause
-- parses in the locking-clause position (the only place this FOR is
-- unambiguous for LALR(1)) and pins every lakehouse foreign table of the
-- SELECT. Live pinning against fixtures is exercised by the smoke
-- transcript; here we verify the deterministic surface: parse-analysis
-- validation, error cases, plain-table behavior, and rule-storage
-- round-trip of the pin.

CREATE EXTENSION lakehouse;

CREATE FOREIGN DATA WRAPPER lakehouse_wrapper
    HANDLER lakehouse.lakehouse_fdw_handler;
CREATE SERVER smoke_server FOREIGN DATA WRAPPER lakehouse_wrapper
    OPTIONS (endpoint 'http://127.0.0.1:9', warehouse 'wh',
             s3_endpoint 'http://s3:9000');

-- credentials on the user mapping (13-review #6 authority model)
CREATE USER MAPPING FOR CURRENT_USER SERVER smoke_server
    OPTIONS (s3_access_key 'key', s3_secret_key 'secret');

CREATE FOREIGN TABLE lakehouse_ft (
    id bigint,
    name text
) SERVER smoke_server
    OPTIONS (table 'analytics.events');

CREATE TABLE local_t (id bigint);
INSERT INTO local_t VALUES (1), (2);

-- parse-analysis validation: constants only
SELECT count(*) FROM lakehouse_ft FOR TIMESTAMP AS OF now();
SELECT count(*) FROM lakehouse_ft FOR TIMESTAMP AS OF 'not-a-timestamp';
SELECT count(*) FROM lakehouse_ft FOR VERSION AS OF 'not-a-number';
SELECT count(*) FROM lakehouse_ft FOR VERSION AS OF 1.5;
SELECT count(*) FROM lakehouse_ft FOR TAG AS OF 42;

-- the clause lives in the locking-clause position: WHERE comes first
SELECT count(*) FROM lakehouse_ft WHERE id < 5 FOR TIMESTAMP AS OF '2026-01-01 00:00:00+00';

-- not applicable to a bare VALUES statement
VALUES (1), (2) FOR TIMESTAMP AS OF '2026-01-01 00:00:00+00';

-- the clause is a plain-select clause: a set-op continuation doesn't parse
SELECT id FROM lakehouse_ft FOR TIMESTAMP AS OF '2026-01-01 00:00:00+00'
UNION
SELECT id FROM lakehouse_ft;

-- plain tables are simply unpinned: no error, no effect
SELECT count(*) FROM local_t FOR TIMESTAMP AS OF '2026-01-01 00:00:00+00';

-- plain tables stay usable after a pin attempt in the same statement
SELECT count(*) FROM local_t FOR VERSION AS OF 12345;

-- FOR UPDATE and the lakehouse clause do not collide: FOR UPDATE still
-- parses (and errors on a foreign table the usual way), and the lakehouse
-- clause parses after WHERE
BEGIN;
SELECT id FROM local_t FOR UPDATE;
ROLLBACK;

-- the pin rides into stored rules: a view over a pinned foreign table
-- keeps the selector (observed through the plan-time error, which names
-- the selector — the endpoint is unreachable, so this is deterministic)
CREATE VIEW lakehouse_ft_v AS
    SELECT id FROM lakehouse_ft FOR VERSION AS OF 12345;
SELECT count(*) FROM lakehouse_ft_v;

CREATE VIEW lakehouse_ft_ts_v AS
    SELECT id FROM lakehouse_ft FOR TIMESTAMP AS OF '2026-01-01 00:00:00+00';
SELECT count(*) FROM lakehouse_ft_ts_v;

-- a view without the pin stays selector-free
CREATE VIEW lakehouse_ft_plain_v AS
    SELECT id FROM lakehouse_ft;
SELECT count(*) FROM lakehouse_ft_plain_v;

-- DEPARSE RECONSTRUCTION (spec 16 blocker 3): pg_get_viewdef is what a
-- logical dump stores, so it MUST reproduce the FOR ... AS OF clause —
-- a viewdef that drops the pin silently repoints the restored view to
-- the latest snapshot. The deparse must also round-trip: re-creating a
-- view from the emitted text yields the same pinned definition again
-- (fixed point).
SELECT pg_get_viewdef('lakehouse_ft_v'::regclass, true) AS version_viewdef;
SELECT pg_get_viewdef('lakehouse_ft_ts_v'::regclass, true) AS ts_viewdef;
SELECT pg_get_viewdef('lakehouse_ft_plain_v'::regclass, true) AS plain_viewdef;

CREATE VIEW lakehouse_ft_v_rt AS
    SELECT id FROM lakehouse_ft FOR VERSION AS OF 12345;
SELECT pg_get_viewdef('lakehouse_ft_v_rt'::regclass, true) =
       pg_get_viewdef('lakehouse_ft_v'::regclass, true) AS roundtrip_fixed_point;

-- timestamp viewdef must re-parse to the SAME instant (epoch ms round-trip;
-- the stored selector is an absolute value, timezone-independent)
CREATE VIEW lakehouse_ft_ts_rt AS SELECT id FROM lakehouse_ft
    FOR TIMESTAMP AS OF '2026-01-01 00:00:00+00';
SELECT pg_get_viewdef('lakehouse_ft_ts_rt'::regclass, true) =
       pg_get_viewdef('lakehouse_ft_ts_v'::regclass, true) AS ts_roundtrip_stable;

-- tag pins deparse too (ref: selector), with quote-safe emission
CREATE VIEW lakehouse_ft_tag_v AS
    SELECT id FROM lakehouse_ft FOR TAG AS OF 'it''s friday';
SELECT pg_get_viewdef('lakehouse_ft_tag_v'::regclass, true) AS tag_viewdef;
SELECT count(*) FROM lakehouse_ft_tag_v;

-- LOGICAL DUMP/RESTORE acceptance (spec 16 blocker 3): pg_dump stores
-- exactly the pg_get_viewdef text, so a piped dump|restore must land the
-- view with the pin intact. Piped (no temp files); the restore loads the
-- extension + wrapper + server + foreign table from the dump itself.
\! dropdb --if-exists regression_lakehouse_restore
\! createdb regression_lakehouse_restore
\! pg_dump regression_lakehouse | psql -d regression_lakehouse_restore 2>&1 | grep -c "^ERROR"
\! psql -d regression_lakehouse_restore -tAc "SELECT pg_get_viewdef('lakehouse_ft_v'::regclass, true)" | grep -c "FOR VERSION AS OF 12345"
\! psql -d regression_lakehouse_restore -tAc "SELECT pg_get_viewdef('lakehouse_ft_ts_v'::regclass, true)" | grep -c "FOR TIMESTAMP AS OF '2026-01-01 00:00:00+00'"
\! psql -d regression_lakehouse_restore -tAc "SELECT pg_get_viewdef('lakehouse_ft_tag_v'::regclass, true)" | grep -c "FOR TAG AS OF"
\! dropdb --if-exists regression_lakehouse_restore

DROP VIEW lakehouse_ft_v, lakehouse_ft_ts_v, lakehouse_ft_plain_v,
          lakehouse_ft_v_rt, lakehouse_ft_ts_rt, lakehouse_ft_tag_v;
DROP TABLE local_t;
DROP FOREIGN TABLE lakehouse_ft;
DROP USER MAPPING FOR CURRENT_USER SERVER smoke_server;
DROP SERVER smoke_server;
DROP FOREIGN DATA WRAPPER lakehouse_wrapper;
DROP EXTENSION lakehouse;
