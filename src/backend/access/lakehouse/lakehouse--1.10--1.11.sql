\echo Use "ALTER EXTENSION lakehouse UPDATE TO '1.11'" to load this file. \quit

-- Launch one read-only worker using the caller's effective role and database.
-- Grant only to a dedicated login with read access to the managed ledger.
CREATE FUNCTION lakehouse.preflight_managed_publication_lookup(op uuid, generation bigint)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'pg_lakehouse_preflight_managed_lookup'
LANGUAGE c STRICT VOLATILE PARALLEL UNSAFE;

REVOKE ALL ON FUNCTION lakehouse.preflight_managed_publication_lookup(uuid, bigint)
FROM PUBLIC;

COMMENT ON FUNCTION lakehouse.preflight_managed_publication_lookup(uuid, bigint)
IS 'Read and verify one retained proposal in a native lookup worker; returns local evidence only';
