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
    OPTIONS (endpoint 'http://127.0.0.1:9', warehouse 'wh');

CREATE FOREIGN TABLE lakehouse_ft (
    id bigint,
    name text
) SERVER smoke_server
    OPTIONS (table 'analytics.events',
             s3_endpoint 'http://s3:9000',
             s3_access_key 'key', s3_secret_key 'secret');

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

DROP VIEW lakehouse_ft_v, lakehouse_ft_ts_v, lakehouse_ft_plain_v;
DROP TABLE local_t;
DROP FOREIGN TABLE lakehouse_ft;
DROP SERVER smoke_server;
DROP FOREIGN DATA WRAPPER lakehouse_wrapper;
DROP EXTENSION lakehouse;
