\echo Use "ALTER EXTENSION lakehouse UPDATE TO '1.8'" to load this file. \quit

-- A grant belongs to one immutable capture generation. Revocation is
-- permanent for this binding ID, including if the same route is enrolled again.
CREATE TABLE lakehouse.publication_send_grants (
    capture_binding_id uuid PRIMARY KEY REFERENCES lakehouse.managed_capture_bindings,
    catalog_prefix text NOT NULL,
    s3_region text NOT NULL,
    revoked boolean NOT NULL DEFAULT false
);

-- Existing claims and intents retain NULL admission references. Migration
-- never infers authority to dispatch them from members or current enrollment.
ALTER TABLE lakehouse.publication_claims
    ADD COLUMN capture_binding_id uuid,
    ADD FOREIGN KEY (capture_binding_id, relid, authority_id, table_uuid)
        REFERENCES lakehouse.managed_capture_bindings
        (binding_id, relid, authority_id, table_uuid);
ALTER TABLE lakehouse.publication_attempts
    ADD COLUMN capture_binding_id uuid REFERENCES lakehouse.publication_send_grants;

CREATE FUNCTION lakehouse.guard_publication_admission() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    IF TG_TABLE_NAME = 'publication_send_grants' AND TG_OP = 'UPDATE' THEN
        IF NOT OLD.revoked AND NEW.revoked
           AND (OLD.capture_binding_id, OLD.catalog_prefix, OLD.s3_region)
               IS NOT DISTINCT FROM (NEW.capture_binding_id, NEW.catalog_prefix, NEW.s3_region) THEN
            RETURN NEW;
        END IF;
    END IF;
    RAISE EXCEPTION 'publication admission evidence is immutable' USING ERRCODE = '55000';
END $$;

CREATE TRIGGER guard_publication_binding
    BEFORE UPDATE OR DELETE ON lakehouse.managed_capture_bindings
    FOR EACH ROW EXECUTE FUNCTION lakehouse.guard_publication_admission();
CREATE TRIGGER guard_publication_grant
    BEFORE UPDATE OR DELETE ON lakehouse.publication_send_grants
    FOR EACH ROW EXECUTE FUNCTION lakehouse.guard_publication_admission();
CREATE TRIGGER guard_publication_grant_truncate
    BEFORE TRUNCATE ON lakehouse.publication_send_grants
    FOR EACH STATEMENT EXECUTE FUNCTION lakehouse.guard_publication_admission();
CREATE TRIGGER guard_publication_claim_binding
    BEFORE UPDATE OF operation_id, authority_id, table_uuid, relid, input_digest, capture_binding_id
    ON lakehouse.publication_claims
    FOR EACH ROW WHEN (OLD.capture_binding_id IS NOT NULL)
    EXECUTE FUNCTION lakehouse.guard_publication_admission();

CREATE FUNCTION lakehouse.lock_active_publication_binding(binding uuid, source_rel oid) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    -- The state row is updated by revoke and removed by DROP. FOR SHARE
    -- conflicts with either transition and rechecks the predicate on wake.
    PERFORM FROM lakehouse.managed_capture_state s
    JOIN lakehouse.managed_capture_bindings b ON b.binding_id = s.active_binding_id
    WHERE s.relid = source_rel AND b.relid = source_rel AND b.binding_id = binding
      AND (s.incarnation_id, s.generation) = (b.incarnation_id, b.generation)
    FOR SHARE OF s;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'publication capture binding is inactive' USING ERRCODE = '55000';
    END IF;
END $$;

CREATE FUNCTION lakehouse.lock_publication_send_grant(binding uuid, source_rel oid) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    PERFORM lakehouse.lock_active_publication_binding(binding, source_rel);
    PERFORM FROM lakehouse.publication_send_grants g
    WHERE g.capture_binding_id = binding AND NOT g.revoked
    FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'publication send grant is missing or revoked' USING ERRCODE = '55000';
    END IF;
END $$;

CREATE FUNCTION lakehouse.grant_publication_send(binding uuid, catalog_prefix text, s3_region text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    source lakehouse.managed_capture_bindings%ROWTYPE;
BEGIN
    IF catalog_prefix IS NULL
       OR (catalog_prefix <> '' AND catalog_prefix !~ '^[A-Za-z0-9_-]+(/[A-Za-z0-9_-]+)*$')
       OR s3_region IS NULL OR s3_region !~ '^[a-z0-9]+(-[a-z0-9]+)*$' THEN
        RAISE EXCEPTION 'publication requires an explicit REST prefix and object region'
            USING ERRCODE = '22023';
    END IF;
    SELECT * INTO source FROM lakehouse.managed_capture_bindings WHERE binding_id = binding;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'publication capture binding is unknown' USING ERRCODE = '55000';
    END IF;
    PERFORM lakehouse.lock_active_publication_binding(binding, source.relid);

    IF source.target_identity !~ '^[^.]+([.][^.]+)+$'
       OR source.catalog_endpoint !~ '^https?://[^/@?#[:space:]]+(/[^?#[:space:]]*)?$'
       OR source.object_endpoint IS NULL
       OR source.object_endpoint !~ '^https?://[^/@?#[:space:]]+/?$' THEN
        RAISE EXCEPTION 'publication requires explicit catalog and object routes'
            USING ERRCODE = '22023';
    END IF;
    INSERT INTO lakehouse.publication_send_grants (capture_binding_id, catalog_prefix, s3_region)
    VALUES (binding, catalog_prefix, s3_region)
    ON CONFLICT (capture_binding_id) DO NOTHING;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'publication binding already has a grant'
            USING ERRCODE = '55000';
    END IF;
END $$;

CREATE FUNCTION lakehouse.revoke_publication_send(binding uuid)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    UPDATE lakehouse.publication_send_grants
    SET revoked = true WHERE capture_binding_id = binding AND NOT revoked;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'publication send grant is missing or revoked'
            USING ERRCODE = '55000';
    END IF;
END $$;

CREATE TYPE lakehouse.publication_binding_descriptor AS (
    binding_id uuid,
    incarnation_id uuid,
    binding_generation bigint,
    authority_id uuid,
    table_uuid uuid,
    server_id oid,
    catalog_endpoint text,
    catalog_prefix text,
    namespace_components text[],
    table_name text,
    warehouse text,
    object_endpoint text,
    s3_region text,
    storage_root text,
    schema_id integer,
    spec_id integer,
    ref_name text
);

CREATE VIEW lakehouse.publication_binding_descriptors AS
SELECT b.binding_id, b.incarnation_id, b.generation AS binding_generation,
       b.authority_id, b.table_uuid, b.server_id, b.catalog_endpoint, g.catalog_prefix,
       route.parts[1:cardinality(route.parts) - 1] AS namespace_components,
       route.parts[cardinality(route.parts)] AS table_name, b.warehouse, b.object_endpoint,
       g.s3_region, b.storage_root, b.schema_id, b.spec_id, 'main'::text AS ref_name
FROM lakehouse.managed_capture_bindings b
JOIN lakehouse.publication_send_grants g ON g.capture_binding_id = b.binding_id
CROSS JOIN LATERAL (SELECT string_to_array(b.target_identity, '.') AS parts) route;

-- Preserve the existing digest and membership protocol for both claim adapters.
CREATE FUNCTION lakehouse.claim_publication_inputs(
    op uuid, authority uuid, remote_table uuid, source_rel oid, segments bigint[], binding uuid)
RETURNS bytea LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    stored lakehouse.publication_claims%ROWTYPE;
    ids bigint[];
    n integer;
    digest bytea;
BEGIN
    IF op IS NULL OR authority IS NULL OR remote_table IS NULL OR source_rel IS NULL
        OR segments IS NULL OR cardinality(segments) NOT BETWEEN 1 AND 1024
        OR array_ndims(segments) <> 1 THEN
        RAISE EXCEPTION 'claim requires a binding and 1 to 1024 segment IDs'
            USING ERRCODE = '22023';
    END IF;
    SELECT array_agg(DISTINCT id ORDER BY id) INTO ids FROM unnest(segments) id;
    IF cardinality(ids) <> cardinality(segments) OR array_position(ids, NULL) IS NOT NULL THEN
        RAISE EXCEPTION 'claim requires distinct non-null segment IDs' USING ERRCODE = '22023';
    END IF;

    SELECT * INTO stored FROM lakehouse.publication_claims WHERE operation_id = op;
    IF FOUND THEN
        IF (stored.authority_id, stored.table_uuid, stored.relid) IS DISTINCT FROM
           (authority, remote_table, source_rel) OR ids IS DISTINCT FROM
           (SELECT array_agg(seg_id ORDER BY seg_id) FROM lakehouse.publication_members
            WHERE operation_id = op) THEN
            RAISE EXCEPTION 'operation ID already identifies a different publication'
                USING ERRCODE = '22023';
        END IF;
        IF stored.capture_binding_id IS DISTINCT FROM binding THEN
            RAISE EXCEPTION 'publication claim has different admission provenance'
                USING ERRCODE = '55000';
        END IF;
        RETURN stored.input_digest;
    END IF;

    -- Capture sealed each payload. Only IDs, counts and hashes are read
    -- under the claim locks, at most 1024 fixed 44-byte digest inputs.
    SELECT count(*), sha256(string_agg(int8send(seg_id) || int4send(row_count) ||
                                       payload_digest, ''::bytea ORDER BY seg_id))
      INTO n, digest
      FROM (SELECT seg_id, row_count, payload_digest FROM lakehouse.lakehouse_delta_segments
            WHERE seg_id = ANY(ids) AND relid = source_rel AND authority_id = authority
              AND table_uuid = remote_table AND NOT published
              AND (binding IS NULL OR capture_binding_id = binding)
            ORDER BY seg_id FOR SHARE) AS eligible;
    IF n <> cardinality(ids) THEN
        RAISE EXCEPTION 'claim does not match the exact pending bound segments'
            USING ERRCODE = '55000';
    END IF;

    INSERT INTO lakehouse.publication_claims
        (operation_id, authority_id, table_uuid, relid, input_digest, capture_binding_id)
    VALUES (op, authority, remote_table, source_rel, digest, binding)
    ON CONFLICT DO NOTHING;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'publication operation or target already reserved'
            USING ERRCODE = '40001';
    END IF;
    INSERT INTO lakehouse.publication_members (seg_id, operation_id) SELECT id, op FROM unnest(ids) id;
    RETURN digest;
END $$;

CREATE OR REPLACE FUNCTION lakehouse.claim_publication(
    op uuid, authority uuid, remote_table uuid, source_rel oid, segments bigint[])
RETURNS bytea LANGUAGE sql VOLATILE SET search_path = pg_catalog, pg_temp AS $$
    SELECT lakehouse.claim_publication_inputs(op, authority, remote_table, source_rel, segments, NULL)
$$;

CREATE FUNCTION lakehouse.claim_managed_publication(op uuid, segments bigint[])
RETURNS bytea LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    bindings uuid[];
    found_count integer;
    source lakehouse.managed_capture_bindings%ROWTYPE;
    digest bytea;
BEGIN
    IF op IS NULL OR segments IS NULL OR cardinality(segments) NOT BETWEEN 1 AND 1024
       OR array_ndims(segments) <> 1 THEN
        RAISE EXCEPTION 'claim requires an operation and 1 to 1024 segment IDs'
            USING ERRCODE = '22023';
    END IF;
    IF array_position(segments, NULL) IS NOT NULL OR
       (SELECT count(DISTINCT id) FROM unnest(segments) id) <> cardinality(segments) THEN
        RAISE EXCEPTION 'claim requires distinct non-null segment IDs' USING ERRCODE = '22023';
    END IF;
    SELECT array_agg(DISTINCT capture_binding_id), count(*) INTO bindings, found_count
    FROM lakehouse.lakehouse_delta_segments WHERE seg_id = ANY(segments);
    IF found_count <> cardinality(segments) OR cardinality(bindings) <> 1 OR bindings[1] IS NULL THEN
        RAISE EXCEPTION 'publication requires exact members from one capture binding'
            USING ERRCODE = '55000';
    END IF;
    SELECT * INTO STRICT source FROM lakehouse.managed_capture_bindings WHERE binding_id = bindings[1];

    -- Stamp the new claim in its original INSERT. An old operation ID may
    -- identify an earlier unbound claim even when every member now has a binding.
    digest := lakehouse.claim_publication_inputs(op, source.authority_id, source.table_uuid,
                                                source.relid, segments, source.binding_id);
    PERFORM lakehouse.lock_publication_send_grant(source.binding_id, source.relid);
    RETURN digest;
END $$;

CREATE FUNCTION lakehouse.get_publication_send_binding(op uuid, generation bigint)
RETURNS SETOF lakehouse.publication_binding_descriptor
LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    claim lakehouse.publication_claims%ROWTYPE;
    found_count integer;
    exact_binding boolean;
    observed_hash bytea;
BEGIN
    SELECT * INTO claim FROM lakehouse.publication_claims WHERE operation_id = op FOR UPDATE;
    IF NOT FOUND OR generation IS DISTINCT FROM claim.owner_generation OR generation = 0 THEN
        RAISE EXCEPTION 'stale publication owner generation' USING ERRCODE = '40001';
    END IF;
    IF claim.capture_binding_id IS NULL OR claim.verified_snapshot_id IS NOT NULL OR
       EXISTS (SELECT FROM lakehouse.publication_attempts WHERE operation_id = op) THEN
        RAISE EXCEPTION 'publication does not have a new send admission'
            USING ERRCODE = '55000';
    END IF;
    PERFORM lakehouse.lock_publication_send_grant(claim.capture_binding_id, claim.relid);

    SELECT count(*),
           bool_and(capture_binding_id IS NOT DISTINCT FROM claim.capture_binding_id AND NOT published),
           sha256(string_agg(int8send(seg_id) || int4send(row_count) || payload_digest,
                             ''::bytea ORDER BY seg_id))
    INTO found_count, exact_binding, observed_hash
    FROM (
        SELECT s.seg_id, s.row_count, s.payload_digest, s.capture_binding_id, s.published
        FROM lakehouse.publication_members m
        JOIN lakehouse.lakehouse_delta_segments s USING (seg_id)
        WHERE m.operation_id = op
        ORDER BY m.seg_id LIMIT 1025 FOR SHARE OF m, s
    ) exact_members;
    IF found_count NOT BETWEEN 1 AND 1024 OR exact_binding IS DISTINCT FROM true
       OR observed_hash IS DISTINCT FROM claim.input_digest THEN
        RAISE EXCEPTION 'publication members do not match the admitted binding and digest'
            USING ERRCODE = '55000';
    END IF;
    RETURN QUERY SELECT d.* FROM lakehouse.publication_binding_descriptors d
        WHERE d.binding_id = claim.capture_binding_id;
END $$;

CREATE FUNCTION lakehouse.get_publication_lookup_binding(op uuid)
RETURNS SETOF lakehouse.publication_binding_descriptor
LANGUAGE sql STABLE SET search_path = pg_catalog, pg_temp AS $$
    SELECT d.* FROM lakehouse.publication_attempts a
    JOIN lakehouse.publication_claims c USING (operation_id)
    JOIN lakehouse.publication_binding_descriptors d
      ON d.binding_id = a.capture_binding_id AND d.binding_id = c.capture_binding_id
    WHERE a.operation_id = op
$$;

CREATE OR REPLACE PROCEDURE lakehouse.authorize_publication_send(
    IN op uuid, IN generation bigint, IN input_hash bytea, IN parent bigint,
    IN proposed bigint, IN proposal_hash bytea, IN body bytea, OUT intent_xid xid)
LANGUAGE plpgsql AS $$
DECLARE
    claim lakehouse.publication_claims%ROWTYPE;
BEGIN
    -- An attached SET clause prohibits transaction control in a procedure.
    SET LOCAL search_path TO pg_catalog, pg_temp;
    SET LOCAL synchronous_commit TO local;
    IF NOT current_setting('fsync')::boolean THEN
        RAISE EXCEPTION 'publication send requires fsync' USING ERRCODE = '55000';
    END IF;

    SELECT * INTO claim FROM lakehouse.publication_claims WHERE operation_id = op FOR UPDATE;
    IF NOT FOUND OR generation IS DISTINCT FROM claim.owner_generation OR generation = 0 THEN
        RAISE EXCEPTION 'stale publication owner generation' USING ERRCODE = '40001';
    END IF;
    IF input_hash IS DISTINCT FROM claim.input_digest OR proposed IS NULL OR proposed <= 0
        OR (parent IS NOT NULL AND (parent <= 0 OR parent = proposed))
        OR proposal_hash IS NULL OR octet_length(proposal_hash) <> 32 THEN
        RAISE EXCEPTION 'publication proposal does not match the claim or has an invalid identity'
            USING ERRCODE = '22023';
    END IF;
    IF claim.verified_snapshot_id IS NOT NULL OR
        EXISTS (SELECT FROM lakehouse.publication_attempts WHERE operation_id = op) THEN
        RAISE EXCEPTION 'publication already has a send intent'
            USING ERRCODE = '55000';
    END IF;

    IF body IS NULL OR octet_length(body) NOT BETWEEN 1 AND 65536 THEN
        RAISE EXCEPTION 'publication proposal must contain 1 to 65536 request bytes'
            USING ERRCODE = '22023';
    END IF;
    IF sha256(body) <> proposal_hash THEN
        RAISE EXCEPTION 'publication proposal bytes do not match the supplied digest'
            USING ERRCODE = '22023';
    END IF;

    -- The descriptor check holds the binding and grant locks to this COMMIT.
    PERFORM lakehouse.get_publication_send_binding(op, generation);
    IF NOT FOUND THEN
        RAISE EXCEPTION 'publication send admission is missing' USING ERRCODE = '55000';
    END IF;
    INSERT INTO lakehouse.publication_attempts
        (operation_id, owner_generation, expected_parent, proposed_snapshot_id, proposal_digest,
         proposal_body, capture_binding_id)
    VALUES (op, generation, parent, proposed, proposal_hash, body, claim.capture_binding_id);
    intent_xid := pg_current_xact_id()::xid;
    COMMIT;
END $$;

REVOKE ALL ON PROCEDURE lakehouse.authorize_publication_send(uuid, bigint, bytea, bigint, bigint, bytea, bytea)
    FROM PUBLIC;
REVOKE ALL ON lakehouse.publication_send_grants, lakehouse.publication_binding_descriptors FROM PUBLIC;
REVOKE ALL ON FUNCTION lakehouse.guard_publication_admission(),
    lakehouse.lock_active_publication_binding(uuid, oid),
    lakehouse.lock_publication_send_grant(uuid, oid),
    lakehouse.grant_publication_send(uuid, text, text),
    lakehouse.revoke_publication_send(uuid),
    lakehouse.claim_publication_inputs(uuid, uuid, uuid, oid, bigint[], uuid),
    lakehouse.claim_managed_publication(uuid, bigint[]),
    lakehouse.get_publication_send_binding(uuid, bigint),
    lakehouse.get_publication_lookup_binding(uuid) FROM PUBLIC;
