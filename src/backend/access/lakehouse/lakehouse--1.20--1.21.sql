\echo Use "ALTER EXTENSION lakehouse UPDATE TO '1.21'" to load this file. \quit

-- Foreground flush with a typed receipt (spec 17 §3 publish-and-wait):
-- drains the relation's unpublished staged segments synchronously (same
-- machinery, same failures as lakehouse.publish_staged) and returns the
-- verified snapshot id. state: 'no change' | 'published'. Serverless
-- flush-before-shutdown: call before stopping a writer.
CREATE FUNCTION lakehouse.publish_table(target regclass,
    OUT published_segments integer, OUT snapshot_id bigint, OUT state text)
AS 'MODULE_PATHNAME', 'pg_lakehouse_publish_table'
LANGUAGE C VOLATILE;
