\echo Use "ALTER EXTENSION lakehouse UPDATE TO '1.9'" to load this file. \quit

CREATE INDEX publication_open_managed_by_operation ON lakehouse.publication_claims (operation_id)
    WHERE verified_snapshot_id IS NULL AND capture_binding_id IS NOT NULL;

CREATE INDEX publication_pending_managed_by_segment ON lakehouse.lakehouse_delta_segments (seg_id)
    WHERE NOT published AND capture_binding_id IS NOT NULL;

-- Discovery provides scheduling hints from a single snapshot. It must never
-- grant a send, acquire a generation, take row locks or read request bodies.
-- Include held slots in a page so a caller can advance its cursor past them.
CREATE FUNCTION lakehouse.discover_managed_publication_claims(after_operation uuid, page_size integer)
RETURNS TABLE(operation_id uuid, owner_generation bigint, binding_id uuid, mode text)
LANGUAGE plpgsql STABLE SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    IF page_size IS NULL OR page_size NOT BETWEEN 1 AND 1024 THEN
        RAISE EXCEPTION 'publication discovery requires a page size between 1 and 1024'
            USING ERRCODE = '22023';
    END IF;

    RETURN QUERY
    WITH claim_page AS MATERIALIZED (
        SELECT c.operation_id, c.owner_generation, c.capture_binding_id, c.relid
        FROM lakehouse.publication_claims c
        WHERE c.verified_snapshot_id IS NULL AND c.capture_binding_id IS NOT NULL
          AND c.operation_id >= COALESCE(after_operation, '00000000-0000-0000-0000-000000000000'::uuid)
          AND c.operation_id IS DISTINCT FROM after_operation
        ORDER BY c.operation_id
        LIMIT page_size
    )
    SELECT c.operation_id, c.owner_generation, c.capture_binding_id,
           CASE WHEN a.operation_id IS NOT NULL THEN
                   CASE WHEN a.capture_binding_id = c.capture_binding_id AND a.body_retained
                        THEN 'lookup' ELSE 'hold' END
                WHEN EXISTS (
                    SELECT FROM lakehouse.managed_capture_state s
                    JOIN lakehouse.managed_capture_bindings b ON b.binding_id = s.active_binding_id
                    JOIN lakehouse.publication_send_grants g ON g.capture_binding_id = b.binding_id
                    WHERE s.relid = c.relid AND b.relid = c.relid
                      AND b.binding_id = c.capture_binding_id
                      AND (s.incarnation_id, s.generation) = (b.incarnation_id, b.generation)
                      AND NOT g.revoked
                ) THEN 'send'
                ELSE 'hold'
           END
    FROM claim_page c
    LEFT JOIN LATERAL (
        SELECT a.operation_id, a.capture_binding_id, a.proposal_body IS NOT NULL AS body_retained
        FROM lakehouse.publication_attempts a
        WHERE a.operation_id = c.operation_id
        LIMIT 1
    ) a ON true
    ORDER BY c.operation_id;
END $$;

-- Report the original payload size from the bytea metadata. Do not detoast,
-- return or parse the payload just to plan a bounded group under one binding.
CREATE FUNCTION lakehouse.discover_managed_publication_segments(after_segment bigint, page_size integer)
RETURNS TABLE(segment_id bigint, binding_id uuid, row_count integer, payload_bytes integer, eligible boolean)
LANGUAGE plpgsql STABLE SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    IF page_size IS NULL OR page_size NOT BETWEEN 1 AND 1024 THEN
        RAISE EXCEPTION 'publication discovery requires a page size between 1 and 1024'
            USING ERRCODE = '22023';
    END IF;

    RETURN QUERY
    WITH segment_page AS MATERIALIZED (
        SELECT s.seg_id, s.capture_binding_id, s.relid, s.authority_id, s.table_uuid,
               s.row_count, octet_length(s.payload) AS payload_bytes
        FROM lakehouse.lakehouse_delta_segments s
        WHERE NOT s.published AND s.capture_binding_id IS NOT NULL
          AND s.seg_id >= COALESCE(after_segment, '-9223372036854775808'::bigint)
          AND s.seg_id IS DISTINCT FROM after_segment
        ORDER BY s.seg_id
        LIMIT page_size
    )
    SELECT p.seg_id, p.capture_binding_id, p.row_count, p.payload_bytes,
           EXISTS (
               SELECT FROM lakehouse.managed_capture_state s
               JOIN lakehouse.managed_capture_bindings b ON b.binding_id = s.active_binding_id
               JOIN lakehouse.publication_send_grants g ON g.capture_binding_id = b.binding_id
               WHERE s.relid = p.relid AND b.relid = p.relid
                 AND b.binding_id = p.capture_binding_id
                 AND (s.incarnation_id, s.generation) = (b.incarnation_id, b.generation)
                 AND NOT g.revoked
           ) AND member.seg_id IS NULL AND reservation.operation_id IS NULL
    FROM segment_page p
    LEFT JOIN LATERAL (
        SELECT m.seg_id FROM lakehouse.publication_members m WHERE m.seg_id = p.seg_id LIMIT 1
    ) member ON true
    LEFT JOIN LATERAL (
        SELECT c.operation_id FROM lakehouse.publication_claims c
        WHERE c.authority_id = p.authority_id AND c.table_uuid = p.table_uuid
          AND c.verified_snapshot_id IS NULL
        LIMIT 1
    ) reservation ON true
    ORDER BY p.seg_id;
END $$;

REVOKE ALL ON FUNCTION lakehouse.discover_managed_publication_claims(uuid, integer),
                       lakehouse.discover_managed_publication_segments(bigint, integer) FROM PUBLIC;
