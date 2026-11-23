\echo Use "ALTER EXTENSION lakehouse UPDATE TO '1.15'" to load this file. \quit

-- The operator attests the independently checked retained main pin while
-- enrolling a new lineage. Old bindings receive no inferred anchor.
CREATE TABLE lakehouse.managed_read_anchors (
    binding_id uuid PRIMARY KEY REFERENCES lakehouse.managed_capture_bindings,
    snapshot_id bigint NOT NULL CHECK (snapshot_id > 0)
);
CREATE TRIGGER guard_managed_read_anchor
    BEFORE UPDATE OR DELETE ON lakehouse.managed_read_anchors
    FOR EACH ROW EXECUTE FUNCTION lakehouse.guard_publication_admission();
CREATE TRIGGER guard_managed_read_anchor_truncate
    BEFORE TRUNCATE ON lakehouse.managed_read_anchors
    FOR EACH STATEMENT EXECUTE FUNCTION lakehouse.guard_publication_admission();

CREATE FUNCTION lakehouse.enroll_managed_capture_with_anchor(
    target regclass, target_identity text, server_name text, incarnation_id uuid,
    binding_id uuid, authority_id uuid, table_uuid uuid, storage_root text,
    schema_id integer, spec_id integer, anchor_snapshot bigint)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    IF anchor_snapshot IS NULL OR anchor_snapshot <= 0 THEN
        RAISE EXCEPTION 'managed read enrollment requires a positive retained snapshot'
            USING ERRCODE = '22023';
    END IF;

    -- Enrollment serializes against capture and verifies READ COMMITTED.
    PERFORM lakehouse.enroll_managed_capture(target, target_identity, server_name,
        incarnation_id, binding_id, 1, authority_id, table_uuid, storage_root, schema_id, spec_id);
    IF EXISTS (SELECT FROM lakehouse.lakehouse_delta_segments s WHERE s.relid = target) THEN
        RAISE EXCEPTION 'managed read enrollment requires an empty source ledger'
            USING ERRCODE = '55000';
    END IF;
    INSERT INTO lakehouse.managed_read_anchors VALUES (binding_id, anchor_snapshot);
END $$;

-- An anchor follows only uninterrupted incarnations with the same source
-- shape and destination. A later generation cannot restore a broken chain.
CREATE FUNCTION lakehouse.get_managed_read_scope(target regclass, binding uuid)
RETURNS TABLE(incarnation_id uuid, authority_id uuid, table_uuid uuid,
              ref_name text, storage_root text, anchor_snapshot bigint)
LANGUAGE sql STABLE SET search_path = pg_catalog, pg_temp AS $$
    SELECT b.incarnation_id, b.authority_id, b.table_uuid, 'main'::text,
           b.storage_root, a.snapshot_id
    FROM lakehouse.managed_capture_state s
    JOIN lakehouse.managed_capture_bindings b ON b.binding_id = s.active_binding_id
    JOIN lakehouse.managed_capture_bindings initial ON initial.incarnation_id = b.incarnation_id
        AND initial.generation = 1
    JOIN lakehouse.managed_read_anchors a ON a.binding_id = initial.binding_id
    WHERE s.relid = target AND b.relid = target AND b.binding_id = binding
      AND (s.incarnation_id, s.generation) = (b.incarnation_id, b.generation)
      AND b.source_columns = lakehouse.capture_source_columns(target)
      AND NOT EXISTS (
          SELECT FROM lakehouse.managed_capture_bindings prior
          WHERE prior.incarnation_id = b.incarnation_id
            AND (prior.relid, prior.authority_id, prior.table_uuid, prior.storage_root,
                 prior.target_identity, prior.catalog_endpoint, prior.warehouse, prior.object_endpoint,
                 prior.schema_id, prior.spec_id, prior.source_columns) IS DISTINCT FROM
                (b.relid, b.authority_id, b.table_uuid, b.storage_root,
                 b.target_identity, b.catalog_endpoint, b.warehouse, b.object_endpoint,
                 b.schema_id, b.spec_id, b.source_columns));
$$;

CREATE INDEX managed_read_segments_by_relation
    ON lakehouse.lakehouse_delta_segments (relid, seg_id) WHERE capture_binding_id IS NOT NULL;
CREATE INDEX managed_read_attempts_by_snapshot
    ON lakehouse.publication_attempts (proposed_snapshot_id, capture_binding_id)
    WHERE capture_binding_id IS NOT NULL;

-- Read these IDs under the original PostgreSQL MVCC snapshot. The sealed
-- payload remains on the MVCC row even when a publication becomes visible.
CREATE FUNCTION lakehouse.get_managed_read_visible_segments(target regclass, binding uuid)
RETURNS TABLE(segment_id bigint, row_count integer, payload_digest bytea)
LANGUAGE plpgsql STABLE SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    lineage uuid;
    matched integer;
BEGIN
    SELECT s.incarnation_id INTO lineage FROM lakehouse.get_managed_read_scope(target, binding) s;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'managed read anchor is unavailable or its scope changed'
            USING ERRCODE = '55000';
    END IF;
    RETURN QUERY
        SELECT s.seg_id, s.row_count, s.payload_digest
        FROM lakehouse.lakehouse_delta_segments s
        JOIN lakehouse.managed_capture_bindings b ON b.binding_id = s.capture_binding_id
        WHERE s.relid = target AND s.capture_binding_id IS NOT NULL AND b.incarnation_id = lineage
        ORDER BY s.seg_id LIMIT 16385;
    GET DIAGNOSTICS matched = ROW_COUNT;
    IF matched > 16384 THEN
        RAISE EXCEPTION 'managed read visible segment limit exceeded' USING ERRCODE = '54000';
    END IF;
END $$;

-- Accept a separately certified newest-first chain. Read ledger facts from
-- a fresh snapshot, independently from the reader's MVCC-visible rows.
-- NULL additions marks an unknown edge and every descendant of that edge.
CREATE FUNCTION lakehouse.get_managed_read_additions(target regclass, binding uuid, pins bigint[])
RETURNS TABLE(snapshot_id bigint, additions bigint[])
LANGUAGE plpgsql STABLE SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    scope record;
    pin_count integer;
    operation_ids uuid[];
    receipt record;
    member_ids bigint[];
    member_digest bytea;
    all_members_match boolean;
    member_count integer := 0;
    complete boolean := true;
BEGIN
    SELECT * INTO scope FROM lakehouse.get_managed_read_scope(target, binding);
    IF NOT FOUND THEN
        RAISE EXCEPTION 'managed read anchor is unavailable or its scope changed'
            USING ERRCODE = '55000';
    END IF;

    pin_count := cardinality(pins);
    IF pins IS NULL OR pin_count NOT BETWEEN 1 AND 512 OR array_ndims(pins) <> 1
       OR array_lower(pins, 1) <> 1 OR pins[pin_count] IS DISTINCT FROM scope.anchor_snapshot
       OR EXISTS (SELECT FROM unnest(pins) AS p(pin) WHERE p.pin IS NULL OR p.pin <= 0)
       OR (SELECT count(DISTINCT pin) FROM unnest(pins) AS p(pin)) <> pin_count THEN
        RAISE EXCEPTION 'managed read pins must be distinct and end at the enrollment snapshot'
            USING ERRCODE = '22023';
    END IF;

    FOR pin_index IN REVERSE pin_count..1 LOOP
        snapshot_id := pins[pin_index];
        additions := NULL;
        IF pin_index = pin_count THEN
            additions := '{}'::bigint[];
        ELSIF complete THEN
            -- Detect ambiguity before filtering by verified state or parent.
            SELECT ARRAY(
                SELECT a.operation_id FROM lakehouse.publication_attempts a
                JOIN lakehouse.managed_capture_bindings b ON b.binding_id = a.capture_binding_id
                WHERE a.proposed_snapshot_id = pins[pin_index] AND a.capture_binding_id IS NOT NULL
                  AND b.incarnation_id = scope.incarnation_id
                LIMIT 2
            ) INTO operation_ids;
            complete := cardinality(operation_ids) = 1;
            IF complete THEN
                SELECT c.input_digest, c.capture_binding_id INTO receipt
                FROM lakehouse.publication_claims c
                JOIN lakehouse.publication_attempts a USING (operation_id)
                WHERE c.operation_id = operation_ids[1]
                  AND c.relid = target AND c.authority_id = scope.authority_id
                  AND c.table_uuid = scope.table_uuid AND c.capture_binding_id = a.capture_binding_id
                  AND c.verified_snapshot_id = a.proposed_snapshot_id
                  AND a.expected_parent = pins[pin_index + 1];
                complete := FOUND;
            END IF;
            IF complete THEN
                SELECT array_agg(m.seg_id ORDER BY m.seg_id),
                       sha256(string_agg(int8send(m.seg_id) || int4send(m.row_count) ||
                                         m.payload_digest, ''::bytea ORDER BY m.seg_id)),
                       bool_and(m.capture_binding_id IS NOT DISTINCT FROM receipt.capture_binding_id
                                AND m.published IS TRUE
                                AND m.published_snapshot_id IS NOT DISTINCT FROM pins[pin_index]
                                AND m.payload_digest IS NOT NULL AND m.row_count IS NOT NULL)
                INTO member_ids, member_digest, all_members_match
                FROM (
                    SELECT m.seg_id, s.row_count, s.payload_digest, s.capture_binding_id,
                           s.published, s.published_snapshot_id
                    FROM lakehouse.publication_members m
                    LEFT JOIN lakehouse.lakehouse_delta_segments s USING (seg_id)
                    WHERE m.operation_id = operation_ids[1]
                    ORDER BY m.seg_id LIMIT 1025
                ) m;
                complete := all_members_match IS TRUE AND cardinality(member_ids) BETWEEN 1 AND 1024
                    AND member_digest IS NOT DISTINCT FROM receipt.input_digest;
                IF complete THEN
                    member_count := member_count + cardinality(member_ids);
                    IF member_count > 16384 THEN
                        RAISE EXCEPTION 'managed read membership limit exceeded' USING ERRCODE = '54000';
                    END IF;
                    additions := member_ids;
                END IF;
            END IF;
        END IF;
        RETURN NEXT;
    END LOOP;
END $$;

REVOKE ALL ON lakehouse.managed_read_anchors FROM PUBLIC;
REVOKE ALL ON FUNCTION lakehouse.enroll_managed_capture_with_anchor(
    regclass, text, text, uuid, uuid, uuid, uuid, text, integer, integer, bigint),
    lakehouse.get_managed_read_scope(regclass, uuid),
    lakehouse.get_managed_read_visible_segments(regclass, uuid),
    lakehouse.get_managed_read_additions(regclass, uuid, bigint[]) FROM PUBLIC;
