\echo Use "ALTER EXTENSION lakehouse UPDATE TO '1.18'" to load this file. \quit

-- Read the durable attempt independently of the reader's data snapshot.
-- The caller supplies a certified head edge and a fresh SQL snapshot.
CREATE FUNCTION lakehouse.get_managed_read_pending_head(
    target regclass, binding uuid, child bigint, parent bigint)
RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    scope record;
    evidence jsonb;
BEGIN
    IF child IS NULL OR parent IS NULL OR child <= 0 OR parent <= 0 OR child = parent THEN
        RAISE EXCEPTION 'managed pending head requires distinct positive snapshot IDs'
            USING ERRCODE = '22023';
    END IF;
    SELECT * INTO scope FROM lakehouse.get_managed_read_scope(target, binding);
    IF NOT FOUND THEN
        RAISE EXCEPTION 'managed read anchor is unavailable or its scope changed'
            USING ERRCODE = '55000';
    END IF;

    -- Count all attempts in this lineage at the claimed pin before
    -- filtering by parent or finalization. Ambiguity proves nothing.
    WITH possible AS (
        SELECT a.operation_id, a.capture_binding_id, a.expected_parent, a.proposed_snapshot_id,
               a.proposal_digest, a.proposal_body, a.owner_generation, c.input_digest,
               c.owner_generation AS current_owner, c.verified_snapshot_id, b.schema_id, b.spec_id
        FROM lakehouse.publication_attempts a
        JOIN lakehouse.publication_claims c ON c.operation_id = a.operation_id
        JOIN lakehouse.managed_capture_bindings b ON b.binding_id = a.capture_binding_id
        WHERE a.proposed_snapshot_id = child AND a.capture_binding_id IS NOT NULL
          AND b.incarnation_id = scope.incarnation_id AND c.capture_binding_id = a.capture_binding_id
          AND c.relid = target AND (c.authority_id, c.table_uuid) = (scope.authority_id, scope.table_uuid)
        LIMIT 2
    )
    SELECT jsonb_build_object(
        'scope', jsonb_build_object('authority_id', scope.authority_id, 'table_uuid', scope.table_uuid,
            'ref_name', scope.ref_name, 'incarnation_id', scope.incarnation_id, 'storage_root', scope.storage_root),
        'schema_id', p.schema_id, 'spec_id', p.spec_id,
        'operation_id', p.operation_id, 'input_digest', encode(p.input_digest, 'hex'),
        'expected_parent', p.expected_parent::text, 'snapshot_id', p.proposed_snapshot_id::text,
        'proposal_digest', encode(p.proposal_digest, 'hex'), 'proposal_body', encode(p.proposal_body, 'hex'),
        'current_owner', p.current_owner, 'intent_owner', p.owner_generation, 'members', batch.members)
    INTO evidence
    FROM possible p
    CROSS JOIN LATERAL (
        SELECT count(*) AS n, bool_and(m.capture_binding_id IS NOT DISTINCT FROM p.capture_binding_id
            AND m.published IS FALSE AND m.published_snapshot_id IS NULL
            AND m.payload_digest IS NOT NULL AND octet_length(m.payload_digest) = 32
            AND m.row_count IS NOT NULL AND m.row_count > 0) AS held,
            jsonb_agg(jsonb_build_object('segment_id', m.seg_id::text, 'row_count', m.row_count,
                'payload_digest', encode(m.payload_digest, 'hex')) ORDER BY m.seg_id) AS members
        FROM (
            SELECT pm.seg_id, s.capture_binding_id, s.row_count, s.published,
                   s.published_snapshot_id, s.payload_digest
            FROM lakehouse.publication_members pm
            LEFT JOIN lakehouse.lakehouse_delta_segments s USING (seg_id)
            WHERE pm.operation_id = p.operation_id
            ORDER BY pm.seg_id LIMIT 1025
        ) m
    ) batch
    WHERE (SELECT count(*) FROM possible) = 1 AND p.expected_parent = parent
      AND p.verified_snapshot_id IS NULL AND octet_length(p.proposal_body) BETWEEN 1 AND 65536
      AND batch.n BETWEEN 1 AND 1024 AND batch.held IS TRUE;
    RETURN evidence;
END $$;

-- Hold the statement data snapshot while loading only the selected overlay.
-- This read uses one MVCC snapshot for scope, preflight and payloads. In
-- particular it must not switch to the latest ledger observation.
CREATE FUNCTION lakehouse.load_managed_read_overlay(
    target regclass, binding uuid, segments bigint[], max_rows bigint, max_bytes bigint)
RETURNS TABLE(segment_id bigint, row_count integer, payload_digest bytea, payload bytea)
LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    lineage uuid;
    requested integer;
    matched integer;
    total_rows bigint;
    total_bytes bigint;
BEGIN
    IF max_rows IS NULL OR max_rows NOT BETWEEN 1 AND 65536
       OR max_bytes IS NULL OR max_bytes NOT BETWEEN 1 AND 8388608 THEN
        RAISE EXCEPTION 'managed read overlay budgets exceed the supported limits'
            USING ERRCODE = '22023';
    END IF;
    requested := cardinality(segments);
    IF segments IS NULL OR requested > 16384 OR (requested > 0 AND
       (array_ndims(segments) <> 1 OR array_lower(segments, 1) <> 1
        OR EXISTS (
            SELECT FROM unnest(segments) WITH ORDINALITY AS x(id, pos)
            WHERE x.id IS NULL OR x.id <= 0 OR x.id <= segments[x.pos - 1]
        ))) THEN
        RAISE EXCEPTION 'managed read overlay requires bounded, strictly ascending segment IDs'
            USING ERRCODE = '22023';
    END IF;

    SELECT s.incarnation_id INTO lineage FROM lakehouse.get_managed_read_scope(target, binding) s;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'managed read anchor is unavailable or its scope changed'
            USING ERRCODE = '55000';
    END IF;
    IF requested = 0 THEN
        RETURN;
    END IF;

    SELECT count(*), sum(s.row_count), sum(octet_length(s.payload))
    INTO matched, total_rows, total_bytes
    FROM lakehouse.lakehouse_delta_segments s
    JOIN lakehouse.managed_capture_bindings b ON b.binding_id = s.capture_binding_id
    WHERE s.seg_id = ANY(segments) AND s.relid = target AND b.incarnation_id = lineage
      AND s.row_count > 0 AND octet_length(s.payload_digest) = 32;
    IF matched <> requested THEN
        RAISE EXCEPTION 'managed read overlay payload is unavailable in this snapshot'
            USING ERRCODE = '55000';
    END IF;
    IF total_rows > max_rows OR total_bytes > max_bytes THEN
        RAISE EXCEPTION 'managed read overlay exceeds its payload budget'
            USING ERRCODE = '54000';
    END IF;

    RETURN QUERY
        SELECT s.seg_id, s.row_count, s.payload_digest, s.payload
        FROM lakehouse.lakehouse_delta_segments s
        JOIN lakehouse.managed_capture_bindings b ON b.binding_id = s.capture_binding_id
        WHERE s.seg_id = ANY(segments) AND s.relid = target AND b.incarnation_id = lineage
        ORDER BY s.seg_id;
END $$;

REVOKE ALL ON FUNCTION lakehouse.get_managed_read_pending_head(regclass, uuid, bigint, bigint),
    lakehouse.load_managed_read_overlay(regclass, uuid, bigint[], bigint, bigint) FROM PUBLIC;
