\echo Use "ALTER EXTENSION lakehouse UPDATE TO '1.19'" to load this file. \quit

-- Operator-approved Read admission (managed visibility, spec 03
-- "Admission before the native cut"): exact role-specific credential
-- mapping for one capture binding generation. Scope identity is derived
-- from the binding at admission time, never stored here.
CREATE TABLE lakehouse.managed_read_grants (
    binding_id uuid NOT NULL REFERENCES lakehouse.managed_capture_bindings,
    reader_role oid NOT NULL,
    mapping_id oid NOT NULL,
    catalog_prefix text NOT NULL DEFAULT '',
    s3_region text NOT NULL DEFAULT 'us-east-1',
    expires_at timestamptz NOT NULL CHECK (isfinite(expires_at)),
    revoked boolean NOT NULL DEFAULT false,
    PRIMARY KEY (binding_id, reader_role)
);
REVOKE ALL ON lakehouse.managed_read_grants FROM PUBLIC;
CREATE TRIGGER guard_managed_read_grant
    BEFORE UPDATE OR DELETE ON lakehouse.managed_read_grants
    FOR EACH ROW EXECUTE FUNCTION lakehouse.guard_publication_admission();
DO $$ BEGIN
    -- Revocation is a one-way flag transition, like the send grants.
    CREATE OR REPLACE FUNCTION lakehouse.guard_publication_admission() RETURNS trigger
    LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $fn$
    BEGIN
        IF TG_TABLE_NAME = 'publication_send_grants' AND TG_OP = 'UPDATE' THEN
            IF NOT OLD.revoked AND NEW.revoked
               AND (OLD.capture_binding_id, OLD.catalog_prefix, OLD.s3_region)
                   IS NOT DISTINCT FROM (NEW.capture_binding_id, NEW.catalog_prefix, NEW.s3_region) THEN
                RETURN NEW;
            END IF;
        END IF;
        IF TG_TABLE_NAME = 'managed_read_grants' AND TG_OP = 'UPDATE' THEN
            -- The row is the operator's CURRENT read decision, not sealed
            -- admission evidence: rotation may update the mapping, prefix,
            -- region, expiry and the revoked flag, but never the identity.
            IF (OLD.binding_id, OLD.reader_role) = (NEW.binding_id, NEW.reader_role) THEN
                RETURN NEW;
            END IF;
        END IF;
        RAISE EXCEPTION 'publication admission evidence is immutable' USING ERRCODE = '55000';
    END $fn$;
END $$;
CREATE TRIGGER guard_managed_read_grant_truncate
    BEFORE TRUNCATE ON lakehouse.managed_read_grants
    FOR EACH STATEMENT EXECUTE FUNCTION lakehouse.guard_publication_admission();

-- Grant one reader role: the mapping must be ROLE-SPECIFIC (rejects a
-- PUBLIC mapping structurally) on the dedicated publication-credential
-- wrapper and carry purpose=read. The C side re-validates
-- authoritatively at use time.
CREATE FUNCTION lakehouse.grant_managed_read(
    binding uuid, reader regrole, mapping oid,
    catalog_prefix text DEFAULT '', s3_region text DEFAULT 'us-east-1',
    ttl interval DEFAULT '15 minutes')
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF ttl <= interval '0' THEN
        RAISE EXCEPTION 'managed read grant ttl must be positive'
            USING ERRCODE = '22023';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_user_mapping m,
             pg_foreign_server s,
             pg_foreign_data_wrapper w,
             unnest(m.umoptions) opt
        WHERE m.oid = mapping
          AND m.umuser = reader
          AND s.oid = m.umserver
          AND w.oid = s.srvfdw
          AND w.fdwname = 'lakehouse_publication_credentials'
          AND opt = 'purpose=read')
    THEN
        RAISE EXCEPTION 'managed read grant requires a role-specific '
                        'mapping with purpose=read on the publication '
                        'credential server'
            USING ERRCODE = '42501';
    END IF;
    INSERT INTO lakehouse.managed_read_grants
        (binding_id, reader_role, mapping_id, catalog_prefix, s3_region, expires_at)
    VALUES (binding, reader, mapping, catalog_prefix, s3_region,
            clock_timestamp() + ttl)
    ON CONFLICT (binding_id, reader_role) DO UPDATE
        SET mapping_id = EXCLUDED.mapping_id,
            catalog_prefix = EXCLUDED.catalog_prefix,
            s3_region = EXCLUDED.s3_region,
            expires_at = EXCLUDED.expires_at,
            revoked = false;
END;
$$;

CREATE FUNCTION lakehouse.revoke_managed_read(binding uuid, reader regrole)
RETURNS void LANGUAGE sql AS $$
    UPDATE lakehouse.managed_read_grants SET revoked = true
    WHERE binding_id = binding AND reader_role = reader AND NOT revoked
$$;

-- Admission for one bound table under one checked role: one row or
-- nothing. SECURITY DEFINER so the read scope is resolved without
-- granting the reader any ledger privilege; the reader_role parameter is
-- the CHECKED role (executor passes GetUserId()/checkAsUser), so a
-- definer view cannot widen admission. Zero rows for unadmitted,
-- revoked, rotated, shape-drifted or expired bindings.
CREATE FUNCTION lakehouse.admit_managed_read(target regclass, reader regrole)
RETURNS TABLE(binding_id uuid, reader_role oid, mapping_id oid, server_id oid,
              expires_unix_seconds bigint,
              incarnation_id uuid, authority_id uuid, table_uuid uuid,
              ref_name text, storage_root text, anchor_snapshot bigint,
              schema_id integer, spec_id integer, target_identity text,
              catalog_endpoint text, catalog_prefix text, s3_region text,
              s3_endpoint text)
LANGUAGE sql VOLATILE STRICT SECURITY DEFINER
SET search_path = pg_catalog, pg_temp AS $$
    SELECT g.binding_id, g.reader_role, g.mapping_id, m.umserver,
           floor(extract(epoch FROM g.expires_at))::bigint,
           b.incarnation_id, b.authority_id, b.table_uuid, 'main'::text,
           b.storage_root, a.snapshot_id, b.schema_id, b.spec_id,
           b.target_identity, b.catalog_endpoint, g.catalog_prefix, g.s3_region,
           b.object_endpoint
    FROM lakehouse.managed_capture_state s
    JOIN lakehouse.managed_read_grants g
      ON g.binding_id = s.active_binding_id AND g.reader_role = reader::oid
     AND g.expires_at > clock_timestamp() AND NOT g.revoked
    JOIN pg_user_mapping m ON m.oid = g.mapping_id AND m.umuser = g.reader_role
    JOIN lakehouse.managed_capture_bindings b ON b.binding_id = g.binding_id
    JOIN lakehouse.managed_capture_bindings initial
      ON initial.incarnation_id = b.incarnation_id AND initial.generation = 1
    JOIN lakehouse.managed_read_anchors a ON a.binding_id = initial.binding_id
    WHERE s.relid = target
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
                 b.schema_id, b.spec_id, b.source_columns))
$$;
REVOKE ALL ON FUNCTION lakehouse.admit_managed_read(regclass, regrole),
    lakehouse.grant_managed_read(uuid, regrole, oid, text, text, interval),
    lakehouse.revoke_managed_read(uuid, regrole) FROM PUBLIC;

-- Retention of each lineage segment's sealed payload, read under the
-- CALLER's statement snapshot (the overlay is STABLE there). select_cut
-- refuses any cut whose overlay needs a reclaimed payload. Mirrors the
-- guarded shape of get_managed_read_visible_segments.
CREATE FUNCTION lakehouse.get_managed_read_segment_retention(target regclass, binding uuid)
RETURNS TABLE(segment_id bigint, payload_retained boolean)
LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path = pg_catalog, pg_temp AS $$
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
        SELECT s.seg_id, s.payload IS NOT NULL
        FROM lakehouse.lakehouse_delta_segments s
        JOIN lakehouse.managed_capture_bindings b ON b.binding_id = s.capture_binding_id
        WHERE s.relid = target AND s.capture_binding_id IS NOT NULL AND b.incarnation_id = lineage
        ORDER BY s.seg_id LIMIT 16385;
    GET DIAGNOSTICS matched = ROW_COUNT;
    IF matched > 16384 THEN
        RAISE EXCEPTION 'managed read visible segment limit exceeded' USING ERRCODE = '54000';
    END IF;
END $$;
REVOKE ALL ON FUNCTION lakehouse.get_managed_read_segment_retention(regclass, uuid) FROM PUBLIC;

-- Backend-local evidence of the last demand-time installed managed cut
-- ({"pin": …, "overlay": […]}), for the fixture drill and operator
-- debugging. NULL before the first bound scan of this backend.
CREATE FUNCTION lakehouse.managed_read_cut_evidence() RETURNS jsonb
AS 'MODULE_PATHNAME', 'lakehouse_managed_read_cut_evidence'
LANGUAGE C VOLATILE PARALLEL UNSAFE;
REVOKE ALL ON FUNCTION lakehouse.managed_read_cut_evidence() FROM PUBLIC;
