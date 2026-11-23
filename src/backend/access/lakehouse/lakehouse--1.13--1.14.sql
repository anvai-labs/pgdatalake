\echo Use "ALTER EXTENSION lakehouse UPDATE TO '1.14'" to load this file. \quit

-- An operator enrolls the exact role, mapping and independently approved
-- send target. Credential material stays in the foreign user mapping.
CREATE TABLE lakehouse.publication_native_send_grants (
    binding_id uuid NOT NULL REFERENCES lakehouse.managed_capture_bindings,
    worker_role oid NOT NULL,
    mapping_id oid NOT NULL,
    expires_at timestamptz NOT NULL CHECK (isfinite(expires_at)),
    approved_target jsonb NOT NULL CHECK (jsonb_typeof(approved_target) = 'object'
        AND octet_length(approved_target::text) <= 16384),
    PRIMARY KEY (binding_id, worker_role)
);
REVOKE ALL ON lakehouse.publication_native_send_grants FROM PUBLIC;

-- The diagnostic uses the same bounded ledger locks as a prospective
-- dispatcher. Give its session role no direct ledger mutation privileges.
CREATE FUNCTION lakehouse.load_native_send_preparation(op uuid, generation bigint)
RETURNS TABLE(job_json jsonb, approved_target jsonb, source_relid oid, mapping_id oid,
              server_id oid, expires_unix_seconds bigint,
              segment_ids bigint[], row_counts integer[], payloads bytea[])
LANGUAGE plpgsql VOLATILE STRICT SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    claim lakehouse.publication_claims%ROWTYPE;
    approval record;
    binding lakehouse.publication_binding_descriptor;
    target jsonb;
BEGIN
    SELECT c.* INTO claim FROM lakehouse.publication_claims c
    WHERE c.operation_id = op AND c.capture_binding_id IS NOT NULL;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'managed send requires an approved role-specific credential mapping'
            USING ERRCODE = '42501';
    END IF;
    SELECT g.approved_target, g.mapping_id, m.umserver,
           floor(extract(epoch FROM g.expires_at))::bigint AS expiry
    INTO approval
    FROM lakehouse.publication_native_send_grants g
    JOIN pg_catalog.pg_user_mapping m ON m.oid = g.mapping_id AND m.umuser = g.worker_role
    JOIN pg_catalog.pg_roles r ON r.oid = g.worker_role
    WHERE g.binding_id = claim.capture_binding_id AND r.rolname = session_user
      AND g.expires_at > clock_timestamp()
    FOR SHARE OF g;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'managed send requires an approved role-specific credential mapping'
            USING ERRCODE = '42501';
    END IF;
    SELECT d.* INTO binding FROM lakehouse.get_publication_send_binding(op, generation) d;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'managed send has no acquired operation' USING ERRCODE = '55000';
    END IF;
    target := jsonb_build_object(
        'binding', jsonb_build_object('authority_id', binding.authority_id, 'binding_id', binding.binding_id,
            'incarnation_id', binding.incarnation_id, 'binding_generation', binding.binding_generation),
        'table_uuid', binding.table_uuid, 'table_root', binding.storage_root,
        'schema_id', binding.schema_id, 'spec_id', binding.spec_id, 'ref_name', binding.ref_name,
        'catalog_endpoint', binding.catalog_endpoint, 'catalog_prefix', binding.catalog_prefix,
        'namespace', binding.namespace_components, 'table_name', binding.table_name,
        'object_endpoint', binding.object_endpoint, 's3_region', binding.s3_region);
    IF approval.approved_target IS DISTINCT FROM target THEN
        RAISE EXCEPTION 'managed send approval disagrees with the exact target' USING ERRCODE = '42501';
    END IF;
    RETURN QUERY SELECT jsonb_build_object(
        'target', target, 'operation_id', claim.operation_id, 'owner_generation', claim.owner_generation,
        'input_digest', encode(claim.input_digest, 'hex'), 'observation', jsonb_build_object(
            'mode', 'send', 'operation_id', op, 'binding_id', claim.capture_binding_id, 'owner_generation', generation)),
        approval.approved_target, claim.relid, approval.mapping_id, approval.umserver, approval.expiry,
        array_agg(m.segment_id ORDER BY m.segment_id), array_agg(m.row_count ORDER BY m.segment_id),
        array_agg(m.payload ORDER BY m.segment_id)
    FROM lakehouse.load_managed_publication_members(op, generation, 65536, 8388608) m;
END $$;
REVOKE ALL ON FUNCTION lakehouse.load_native_send_preparation(uuid, bigint) FROM PUBLIC;

CREATE FUNCTION lakehouse.prepare_managed_publication_send(op uuid, generation bigint)
RETURNS jsonb AS 'MODULE_PATHNAME', 'pg_lakehouse_prepare_managed_publication_send'
LANGUAGE c STRICT VOLATILE PARALLEL UNSAFE;
REVOKE ALL ON FUNCTION lakehouse.prepare_managed_publication_send(uuid, bigint) FROM PUBLIC;
COMMENT ON FUNCTION lakehouse.prepare_managed_publication_send(uuid, bigint)
IS 'Verify scoped preparation for an acquired operation and discard the process-local proposal';
