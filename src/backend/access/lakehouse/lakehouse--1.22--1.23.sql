\echo Use "ALTER EXTENSION lakehouse UPDATE TO '1.23'" to load this file. \quit

-- ---------------------------------------------------------------------------
-- Spec 19 §4b (L0 physical stats collector): per-scan table statistics.
--
-- lakehouse.table_stats is INTERNAL (same posture as query_stats): created
-- by this script, REVOKE ALL FROM PUBLIC, written by the collector hooks as
-- the table owner. One row per completed foreign-scan executor, keyed by
-- (dbid, relid, endpoint, ns_table, snapshot_id). All values come from
-- structures the scan already parsed — no additional origin requests.
-- Column-level stats (value_counts composition, filterability tiers) are
-- the follow-up slice (new core FFI).
-- ---------------------------------------------------------------------------

CREATE TABLE lakehouse.table_stats (
    captured_at    timestamptz NOT NULL DEFAULT now(),
    dbid           oid NOT NULL,
    relid          oid NOT NULL,
    endpoint       text NOT NULL,
    ns_table       text NOT NULL,
    snapshot_id    bigint NOT NULL,
    row_count      bigint,
    files_total    integer,
    files_pruned   integer,
    scan_tasks     integer,
    total_bytes    bigint,
    has_deletes    boolean
);
CREATE INDEX lakehouse_table_stats_rel_idx
    ON lakehouse.table_stats (relid, snapshot_id);
REVOKE ALL ON lakehouse.table_stats FROM PUBLIC;
