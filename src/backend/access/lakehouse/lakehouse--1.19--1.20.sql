\echo Use "ALTER EXTENSION lakehouse UPDATE TO '1.20'" to load this file. \quit

-- Metadata-only COUNT fast path for capture-bound tables (spec 03,
-- "managed COUNT"): the planner rewrite routes bound count(*) here
-- instead of the ledger-sum shortcut. The C function computes the cut's
-- two disjoint parts (published base from Parquet metadata + overlay
-- ledger row_count) without opening data files.
CREATE FUNCTION lakehouse.managed_count_cut(regclass) RETURNS bigint
AS 'MODULE_PATHNAME', 'lakehouse_managed_count_cut'
LANGUAGE C VOLATILE PARALLEL UNSAFE;
