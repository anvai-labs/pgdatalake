\echo Use "ALTER EXTENSION lakehouse UPDATE TO '1.4'" to load this file. \quit

-- NULL binding identifies the original publisher's rows, including all rows
-- already present at upgrade. Only a verified capture may set the pair.
-- The first bound profile supports append on main.
ALTER TABLE lakehouse.lakehouse_delta_segments
    ADD COLUMN authority_id uuid,
    ADD COLUMN table_uuid uuid,
    ADD COLUMN payload_digest bytea,
    ADD CHECK ((authority_id IS NULL) = (table_uuid IS NULL)) NOT VALID,
    ADD CHECK (authority_id IS NULL OR
               (row_count IS NOT NULL AND row_count > 0 AND tombstones IS NULL
                AND payload_digest IS NOT NULL AND octet_length(payload_digest) = 32)) NOT VALID;

-- A pending legacy segment can already have a remote effect. Never promote
-- it in place. The hash in a receipt must continue to describe its inputs.
CREATE FUNCTION lakehouse.seal_publication_input() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        NEW.payload_digest := sha256(NEW.payload);
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'bound publication inputs are immutable'
        USING ERRCODE = '55000';
END $$;
CREATE TRIGGER seal_publication_input
    BEFORE INSERT ON lakehouse.lakehouse_delta_segments
    FOR EACH ROW WHEN (NEW.authority_id IS NOT NULL)
    EXECUTE FUNCTION lakehouse.seal_publication_input();
CREATE TRIGGER guard_publication_input
    BEFORE UPDATE OF seg_id, relid, payload, row_count, tombstones, authority_id, table_uuid, payload_digest
    ON lakehouse.lakehouse_delta_segments
    FOR EACH ROW WHEN (OLD.authority_id IS NOT NULL OR NEW.authority_id IS NOT NULL)
    EXECUTE FUNCTION lakehouse.seal_publication_input();

CREATE TABLE lakehouse.publication_claims (
    operation_id         uuid PRIMARY KEY,
    authority_id         uuid NOT NULL,
    table_uuid           uuid NOT NULL,
    relid                oid NOT NULL,
    input_digest         bytea NOT NULL CHECK (octet_length(input_digest) = 32),
    verified_snapshot_id bigint
);
CREATE UNIQUE INDEX publication_open_target ON lakehouse.publication_claims
    (authority_id, table_uuid) WHERE verified_snapshot_id IS NULL;

CREATE TABLE lakehouse.publication_members (
    seg_id       bigint PRIMARY KEY REFERENCES lakehouse.lakehouse_delta_segments,
    operation_id uuid NOT NULL REFERENCES lakehouse.publication_claims
);
CREATE INDEX publication_members_by_operation ON lakehouse.publication_members (operation_id, seg_id);
REVOKE ALL ON lakehouse.publication_claims, lakehouse.publication_members FROM PUBLIC;

-- Private ledger adapter. The caller supplies an operation ID before trying
-- the short local transaction, so a lost PG acknowledgment is recoverable.
CREATE FUNCTION lakehouse.claim_publication(op uuid, authority uuid, remote_table uuid,
                                           source_rel oid, segments bigint[])
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
            ORDER BY seg_id FOR SHARE) AS eligible;
    IF n <> cardinality(ids) THEN
        RAISE EXCEPTION 'claim does not match the exact pending bound segments'
            USING ERRCODE = '55000';
    END IF;

    INSERT INTO lakehouse.publication_claims
        (operation_id, authority_id, table_uuid, relid, input_digest)
    VALUES (op, authority, remote_table, source_rel, digest)
    ON CONFLICT DO NOTHING;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'publication operation or target already reserved'
            USING ERRCODE = '40001';
    END IF;
    INSERT INTO lakehouse.publication_members (seg_id, operation_id) SELECT id, op FROM unnest(ids) id;
    RETURN digest;
END $$;
REVOKE ALL ON FUNCTION lakehouse.claim_publication(uuid, uuid, uuid, oid, bigint[]) FROM PUBLIC;
