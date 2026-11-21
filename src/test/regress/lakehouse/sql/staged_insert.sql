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
