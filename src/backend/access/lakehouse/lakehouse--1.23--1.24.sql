\echo Use "ALTER EXTENSION lakehouse UPDATE TO '1.24'" to load this file. \quit

-- ---------------------------------------------------------------------------
-- Spec 19 §4b (L0 physical stats collector, column half): composed
-- per-column manifest statistics with the derived filterability tier.
-- Same INTERNAL posture as table_stats (REVOKE ALL FROM PUBLIC,
-- owner-context writes). value_count_upper is the EXPLICIT upper bound
-- (value_counts summed across files — values repeat across files);
-- tier derivation per the fixed §4b rules (T2 <=100 distinct AND <=5%
-- of rows; T1 >100 with bounds; T3 boolean/temporal; T4 unfilterable;
-- T0 partition columns deferred until the spec FFI exists).
-- ---------------------------------------------------------------------------

CREATE TABLE lakehouse.column_stats (
    captured_at        timestamptz NOT NULL DEFAULT now(),
    dbid               oid NOT NULL,
    relid              oid NOT NULL,
    ns_table           text NOT NULL,
    snapshot_id        bigint NOT NULL,
    field_name         text NOT NULL,
    field_id           bigint NOT NULL,
    datatype_class     text NOT NULL,
    null_count         bigint NOT NULL,
    value_count_upper  bigint NOT NULL,
    has_bounds         boolean NOT NULL,
    metrics_covered    boolean NOT NULL,
    tier               text NOT NULL
);

-- NOTE: composition is over the PLANNED (post-pruning) file subset, so
-- tier is scan-dependent — the same (snapshot, field) can legitimately
-- record different tiers from a pruned vs a full scan.
CREATE INDEX lakehouse_column_stats_rel_idx
    ON lakehouse.column_stats (relid, snapshot_id);
REVOKE ALL ON lakehouse.column_stats FROM PUBLIC;
