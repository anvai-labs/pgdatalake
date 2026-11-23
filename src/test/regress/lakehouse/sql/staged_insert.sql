-- P3 core (spec 15 §3/§7.2 D1-rev): staged-INSERT mode. Durability is
-- LOCAL — the delta-segments table — so an unreachable catalog must NOT
-- block the commit (COW structurally cannot do this; this test runs
-- against a dead endpoint ON PURPOSE).
CREATE EXTENSION IF NOT EXISTS lakehouse;
SELECT lakehouse.version();

SET lakehouse.write_mode = 'staged';

CREATE TABLE stg (id bigint, name text) USING lakehouse;
CREATE FOREIGN DATA WRAPPER staged_wrapper HANDLER lakehouse.lakehouse_fdw_handler;
CREATE SERVER unreachable_srv FOREIGN DATA WRAPPER staged_wrapper
    OPTIONS (endpoint 'http://127.0.0.1:9', warehouse 'wh',
             s3_endpoint 'http://s3:9000');
CREATE USER MAPPING FOR CURRENT_USER SERVER unreachable_srv;
SELECT lakehouse.attach_table('stg'::regclass, 'analytics.staged_offline', 'unreachable_srv');

-- inserts stage locally; NO remote session is opened in staged mode
INSERT INTO stg SELECT g, 's-'||g FROM generate_series(1, 3) g;

-- read-your-writes: served from the delta segments (MVCC gives us
-- cross-transaction staged visibility for free)
SELECT id, name FROM stg ORDER BY id;

-- exactly ONE unpublished segment captured the batch
SELECT count(*) AS segments, bool_and(NOT published) AS all_unpublished
    FROM lakehouse.lakehouse_delta_segments
    WHERE relid = 'stg'::regclass;

-- payload is a JSON array of 3 row arrays
SELECT convert_from(payload,'UTF8') LIKE '[%' AS payload_is_json_array,
       length(convert_from(payload,'UTF8')) > 10 AS payload_has_content
    FROM lakehouse.lakehouse_delta_segments
    WHERE relid = 'stg'::regclass;

-- second transaction: more rows land as a SECOND segment; both visible
INSERT INTO stg VALUES (4, 's-4');
SELECT id, name FROM stg ORDER BY id;
SELECT count(*) AS segments
    FROM lakehouse.lakehouse_delta_segments
    WHERE relid = 'stg'::regclass;

-- R2 (spec 15): a TABLE-LEVEL write_mode sticks over the session GUC —
-- including when the reloption carries NO identity keys. CREATE-time
-- write_mode alone is legal per the reloption validator, and ALTER
-- TABLE SET is its post-create form; both must win over the session
-- GUC ('cow' here). Prove it the offline way: staged mode opens NO
-- remote session, so this INSERT succeeds against the DEAD endpoint
-- (in COW it would error with a connection failure).
RESET lakehouse.write_mode;
CREATE TABLE stg_r2 (id bigint, val text) USING lakehouse;
SELECT lakehouse.attach_table('stg_r2'::regclass, 'analytics.staged_r2', 'unreachable_srv');
ALTER TABLE stg_r2 SET (write_mode = 'staged');
INSERT INTO stg_r2 SELECT g, 'r-'||g FROM generate_series(1, 3) g;
SELECT count(*) AS segments, bool_and(NOT published) AS all_unpublished
    FROM lakehouse.lakehouse_delta_segments
    WHERE relid = 'stg_r2'::regclass;
SELECT id, val FROM stg_r2 ORDER BY id;

-- BULK CAPTURE (spec 16 blocker 2): one transaction inserting into MORE
-- than 64 staged relations must capture EVERY relation. The former
-- pre-commit collector (a fixed 64-slot entry array) silently dropped
-- entry 65 onward: rows committed in Postgres but never written to the
-- delta ledger — silent data loss on exactly the workload staging is
-- built for (bulk multi-table loads).
RESET lakehouse.write_mode;
SET lakehouse.write_mode = 'staged';
DO $bulk$
BEGIN
  FOR i IN 1..65 LOOP
    EXECUTE format('CREATE TABLE bulk_%s (id bigint, v text) USING lakehouse', i);
    EXECUTE format('SELECT lakehouse.attach_table(%L::regclass, %L, %L)',
                   'bulk_' || i, 'analytics.bulk_' || i, 'unreachable_srv');
  END LOOP;
END $bulk$;
BEGIN;
DO $bulk$
BEGIN
  FOR i IN 1..65 LOOP
    EXECUTE format('INSERT INTO bulk_%s VALUES (%s, %L)', i, i, 'b-' || i);
  END LOOP;
END $bulk$;
COMMIT;

-- ALL 65 relations must carry exactly one unpublished ledger segment
SELECT count(DISTINCT s.relid) AS bulk_rels_captured
FROM lakehouse.lakehouse_delta_segments s
JOIN pg_class c ON c.oid = s.relid
WHERE c.relname LIKE 'bulk%';
SELECT count(*) AS bulk_segments
FROM lakehouse.lakehouse_delta_segments s
JOIN pg_class c ON c.oid = s.relid
WHERE c.relname LIKE 'bulk%';

-- read-your-writes holds for the LAST created relation too
SELECT count(*) AS bulk_65_rows FROM bulk_65;

-- Force a parameterized inner scan to rewind both committed segments and
-- this transaction's own rows. Keep the duplicate in each result.
BEGIN;
INSERT INTO stg VALUES (2, 'own-duplicate');
SET LOCAL enable_material = off;
SET LOCAL enable_memoize = off;
DO $rewind$
DECLARE
    results text[];
BEGIN
    SELECT array_agg(exact::text ORDER BY lower_bound) INTO results
    FROM (VALUES (0), (2)) AS requested(lower_bound)
    CROSS JOIN LATERAL (
        SELECT array_agg(id ORDER BY id) AS exact
        FROM stg WHERE id >= requested.lower_bound
    ) AS scanned;

    IF results IS DISTINCT FROM ARRAY['{1,2,2,3,4}', '{2,2,3,4}'] THEN
        RAISE EXCEPTION 'staged scan lost rows on rescan: %', results;
    END IF;
END
$rewind$;
ROLLBACK;
