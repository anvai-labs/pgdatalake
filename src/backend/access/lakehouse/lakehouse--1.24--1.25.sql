\echo Use "ALTER EXTENSION lakehouse UPDATE TO '1.25'" to load this file. \quit

-- ---------------------------------------------------------------------------
-- Spec 15 §6.3a measurement gate: expose the per-backend immutable-path
-- block cache's diagnostic counters as lakehouse.cache_stats(). One row
-- of BIGINTs (hits, misses, fetched_bytes, used_bytes, entries) read
-- from the Rust BlockCache — diagnostics ONLY, never a policy input and
-- never a second store (§6.3a: "the metadata/stats cache the reader
-- needs IS the block cache"). The R6 result-cache layer (§6.4) is gated
-- on these measurements.
-- ---------------------------------------------------------------------------

CREATE FUNCTION lakehouse.cache_stats(
    OUT hits          bigint,
    OUT misses        bigint,
    OUT fetched_bytes bigint,
    OUT used_bytes    bigint,
    OUT entries       bigint)
AS 'MODULE_PATHNAME', 'pg_lakehouse_cache_stats'
LANGUAGE C VOLATILE PARALLEL UNSAFE;

COMMENT ON FUNCTION lakehouse.cache_stats() IS
	'per-backend Iceberg block-cache diagnostics (spec 15 §6.3a measurement gate)';
