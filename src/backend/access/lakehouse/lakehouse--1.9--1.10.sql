\echo Use "ALTER EXTENSION lakehouse UPDATE TO '1.10'" to load this file. \quit

-- One snapshot gives the worker the operation, its immutable scope and any
-- retained intent. Lookup continues after the relation or its send grant retires.
CREATE FUNCTION lakehouse.load_managed_publication_job(op uuid)
RETURNS TABLE(operation_id uuid, authority_id uuid, table_uuid uuid, binding_id uuid,
              incarnation_id uuid, binding_generation bigint, owner_generation bigint,
              input_digest bytea, verified_snapshot_id bigint, attempt_owner_generation bigint,
              expected_parent bigint, proposed_snapshot_id bigint, proposal_digest bytea, proposal_body bytea)
LANGUAGE sql STABLE STRICT SET search_path = pg_catalog, pg_temp AS $$
    SELECT c.operation_id, c.authority_id, c.table_uuid, b.binding_id, b.incarnation_id,
           b.generation, c.owner_generation, c.input_digest, c.verified_snapshot_id,
           a.owner_generation, a.expected_parent, a.proposed_snapshot_id, a.proposal_digest, a.proposal_body
    FROM lakehouse.publication_claims c
    JOIN lakehouse.managed_capture_bindings b ON b.binding_id = c.capture_binding_id
    LEFT JOIN lakehouse.publication_attempts a USING (operation_id)
    WHERE c.operation_id = op
      AND (a.operation_id IS NULL OR a.capture_binding_id = b.binding_id)
$$;

-- The existing send fence locks the claim, grant and exact inputs. Measure
-- bytea lengths from TOAST metadata before returning any payload. Reconciliation
-- uses the retained proposal and never needs to load staged payloads.
CREATE FUNCTION lakehouse.load_managed_publication_members(
    op uuid, generation bigint, max_rows bigint, max_bytes bigint)
RETURNS TABLE(segment_id bigint, row_count integer, payload bytea)
LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    total_rows bigint;
    total_bytes bigint;
BEGIN
    IF max_rows IS NULL OR max_rows <= 0 OR max_bytes IS NULL OR max_bytes <= 0 THEN
        RAISE EXCEPTION 'managed publication member budgets must be positive'
            USING ERRCODE = '22023';
    END IF;
    PERFORM FROM lakehouse.get_publication_send_binding(op, generation);
    IF NOT FOUND THEN
        RAISE EXCEPTION 'publication send admission is missing' USING ERRCODE = '55000';
    END IF;

    SELECT sum(s.row_count), sum(octet_length(s.payload)) INTO total_rows, total_bytes
    FROM lakehouse.publication_members m
    JOIN lakehouse.lakehouse_delta_segments s USING (seg_id)
    WHERE m.operation_id = op;
    IF total_rows > max_rows OR total_bytes > max_bytes THEN
        RAISE EXCEPTION 'managed publication exceeds its member budget'
            USING ERRCODE = '54000';
    END IF;

    RETURN QUERY
        SELECT s.seg_id, s.row_count, s.payload
        FROM lakehouse.publication_members m
        JOIN lakehouse.lakehouse_delta_segments s USING (seg_id)
        WHERE m.operation_id = op ORDER BY s.seg_id;
END $$;

REVOKE ALL ON FUNCTION lakehouse.load_managed_publication_job(uuid),
                       lakehouse.load_managed_publication_members(uuid, bigint, bigint, bigint)
FROM PUBLIC;
