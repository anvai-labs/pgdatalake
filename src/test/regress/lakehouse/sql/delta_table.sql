CREATE EXTENSION IF NOT EXISTS lakehouse;
SELECT lakehouse.version();

-- P3 foundation (spec 15 §3.3, D1-rev): the staged-delta store is a
-- Postgres table owned by the extension. Columns ARE the publish
-- ledger; PUBLIC has no access (RULES §6 — payload carries table data).
SELECT count(*) AS empty_at_install
    FROM lakehouse.lakehouse_delta_segments;

SELECT attname, format_type(atttypid, atttypmod) AS typ
    FROM pg_attribute
    WHERE attrelid = 'lakehouse.lakehouse_delta_segments'::regclass
      AND attnum > 0 AND NOT attisdropped
    ORDER BY attnum;

-- PUBLIC may not touch it
SELECT has_table_privilege('public',
    'lakehouse.lakehouse_delta_segments', 'SELECT') AS public_select_denied;
