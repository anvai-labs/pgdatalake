ALTER EXTENSION lakehouse UPDATE TO '1.7';

CREATE FUNCTION capture_fixture.require_binding(target regclass)
RETURNS void LANGUAGE sql
RETURN lakehouse.require_managed_capture(target);

CREATE FUNCTION capture_fixture.enroll(
    target regclass, target_identity text, server_name text, incarnation uuid,
    binding uuid, generation bigint, authority uuid, remote_table uuid, root text
) RETURNS void LANGUAGE sql
RETURN lakehouse.enroll_managed_capture(
    target, target_identity, server_name, incarnation, binding, generation,
    authority, remote_table, root, 0, 0);

CREATE FUNCTION capture_fixture.revoke(target regclass, binding uuid, generation bigint)
RETURNS void LANGUAGE sql
RETURN lakehouse.revoke_managed_capture(target, binding, generation);

CREATE FUNCTION capture_fixture.provenance(segment_id bigint) RETURNS jsonb LANGUAGE sql AS $$
    SELECT jsonb_build_array(b.binding_id, b.incarnation_id, b.generation)
    FROM lakehouse.lakehouse_delta_segments s
    LEFT JOIN lakehouse.managed_capture_bindings b ON b.binding_id = s.capture_binding_id
    WHERE s.seg_id = segment_id
$$;
