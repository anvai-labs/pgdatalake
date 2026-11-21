-- P3 core (spec 15 §3/§7.2 D1-rev): staged-INSERT mode. Durability is
-- LOCAL — the delta-segments table — so an unreachable catalog must NOT
-- block the commit (COW structurally cannot do this; this test runs
-- against a dead endpoint ON PURPOSE).
CREATE EXTENSION IF NOT EXISTS lakehouse;
SELECT lakehouse.version();

SET lakehouse.write_mode = 'staged';

CREATE TABLE stg (id bigint, name text) USING lakehouse;
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
SELECT payload::text LIKE '[%' AND payload::text LIKE '%]' AS payload_is_json_array,
       (length(payload::text) > 10) AS payload_has_content
    FROM lakehouse.lakehouse_delta_segments
    WHERE relid = 'stg'::regclass;

-- second transaction: more rows land as a SECOND segment; both visible
INSERT INTO stg VALUES (4, 's-4');
SELECT id, name FROM stg ORDER BY id;
SELECT count(*) AS segments
    FROM lakehouse.lakehouse_delta_segments
    WHERE relid = 'stg'::regclass;
