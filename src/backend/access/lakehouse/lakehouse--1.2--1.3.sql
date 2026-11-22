\echo Use "CREATE EXTENSION lakehouse" to load this file. \quit

-- execution-time COUNT for managed tables (spec 16): VOLATILE so every
-- execution re-resolves permissions, RLS and the snapshot; INVOKER rights
CREATE OR REPLACE FUNCTION lakehouse.managed_count(relid oid)
    RETURNS bigint
    LANGUAGE c VOLATILE STRICT
AS 'MODULE_PATHNAME', 'lakehouse_managed_count';
