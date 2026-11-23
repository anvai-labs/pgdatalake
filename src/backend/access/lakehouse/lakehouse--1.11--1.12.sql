\echo Use "ALTER EXTENSION lakehouse UPDATE TO '1.12'" to load this file. \quit

-- The operator approves a dedicated role-specific user mapping separately
-- from publication send authority. Store no credential values here.
CREATE TABLE lakehouse.publication_lookup_grants (
    binding_id uuid NOT NULL REFERENCES lakehouse.managed_capture_bindings,
    worker_role oid NOT NULL,
    mapping_id oid NOT NULL,
    expires_at timestamptz NOT NULL CHECK (isfinite(expires_at)),
    PRIMARY KEY (binding_id, worker_role)
);
REVOKE ALL ON lakehouse.publication_lookup_grants FROM PUBLIC;

CREATE FUNCTION lakehouse.reconcile_managed_publication_lookup(op uuid, generation bigint)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'pg_lakehouse_reconcile_managed_lookup'
LANGUAGE c STRICT VOLATILE PARALLEL UNSAFE;

REVOKE ALL ON FUNCTION lakehouse.reconcile_managed_publication_lookup(uuid, bigint)
FROM PUBLIC;

COMMENT ON FUNCTION lakehouse.reconcile_managed_publication_lookup(uuid, bigint)
IS 'Verify an existing retained proposal with scoped read credentials and finalize on accepted proof';
