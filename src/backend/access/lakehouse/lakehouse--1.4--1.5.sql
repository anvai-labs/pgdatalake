\echo Use "ALTER EXTENSION lakehouse UPDATE TO '1.5'" to load this file. \quit

ALTER TABLE lakehouse.publication_claims
    ADD COLUMN owner_generation bigint NOT NULL DEFAULT 0 CHECK (owner_generation >= 0);

-- One attempt per operation in this profile. Its presence means the
-- request may have escaped. Keep the record even after verification.
CREATE TABLE lakehouse.publication_attempts (
    operation_id         uuid PRIMARY KEY REFERENCES lakehouse.publication_claims,
    owner_generation     bigint NOT NULL CHECK (owner_generation > 0),
    expected_parent      bigint,
    proposed_snapshot_id bigint NOT NULL CHECK (proposed_snapshot_id > 0),
    proposal_digest      bytea NOT NULL CHECK (octet_length(proposal_digest) = 32),
    CHECK (expected_parent IS NULL OR
           (expected_parent > 0 AND expected_parent <> proposed_snapshot_id))
);
REVOKE ALL ON lakehouse.publication_attempts FROM PUBLIC;

-- Scheduling decides when to transfer ownership. Every transition that
-- can change the operation compares the current token under a row lock.
CREATE FUNCTION lakehouse.advance_publication_generation(op uuid, generation bigint)
RETURNS bigint LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    next_generation bigint;
BEGIN
    UPDATE lakehouse.publication_claims
       SET owner_generation = owner_generation + 1
     WHERE operation_id = op AND owner_generation = generation
       AND verified_snapshot_id IS NULL
     RETURNING owner_generation INTO next_generation;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'publication owner generation could not be advanced'
            USING ERRCODE = '40001';
    END IF;
    RETURN next_generation;
END $$;

-- CALL this as its own top-level transaction. A successful reply follows
-- the synchronous LOCAL commit, and only that caller can dispatch. A lost
-- reply leaves the same lookup-only state as a lost catalog reply.
CREATE PROCEDURE lakehouse.authorize_publication_send(
    IN op uuid, IN generation bigint, IN input_hash bytea, IN parent bigint,
    IN proposed bigint, IN proposal_hash bytea, OUT intent_xid xid)
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

    INSERT INTO lakehouse.publication_attempts
        (operation_id, owner_generation, expected_parent, proposed_snapshot_id, proposal_digest)
    VALUES (op, generation, parent, proposed, proposal_hash);
    intent_xid := pg_current_xact_id()::xid;
    COMMIT;
END $$;

-- Caller must supply verified acceptance evidence from the scoped catalog
-- adapter. Match that evidence to the current generation and exact attempt
-- before retiring any segment.
CREATE FUNCTION lakehouse.finalize_publication(
    op uuid, generation bigint, authority uuid, remote_table uuid, input_hash bytea,
    snapshot bigint, proposal_hash bytea)
RETURNS integer LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    claim lakehouse.publication_claims%ROWTYPE;
    attempt lakehouse.publication_attempts%ROWTYPE;
    members integer;
    completed integer;
BEGIN
    SELECT * INTO claim FROM lakehouse.publication_claims WHERE operation_id = op FOR UPDATE;
    IF NOT FOUND OR generation IS DISTINCT FROM claim.owner_generation OR generation = 0 THEN
        RAISE EXCEPTION 'stale publication owner generation' USING ERRCODE = '40001';
    END IF;
    SELECT * INTO attempt FROM lakehouse.publication_attempts WHERE operation_id = op;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'publication has no send intent' USING ERRCODE = '55000';
    END IF;
    IF (authority, remote_table, input_hash, snapshot, proposal_hash) IS DISTINCT FROM
       (claim.authority_id, claim.table_uuid, claim.input_digest,
        attempt.proposed_snapshot_id, attempt.proposal_digest) THEN
        RAISE EXCEPTION 'publication evidence does not match the recorded attempt'
            USING ERRCODE = '22023';
    END IF;
    SELECT count(*) INTO members FROM lakehouse.publication_members WHERE operation_id = op;
    IF claim.verified_snapshot_id = snapshot THEN
        RETURN members;
    END IF;

    UPDATE lakehouse.lakehouse_delta_segments AS segment
       SET published = true, published_snapshot_id = snapshot
      FROM lakehouse.publication_members AS member
     WHERE member.operation_id = op AND member.seg_id = segment.seg_id
       AND segment.authority_id = authority AND segment.table_uuid = remote_table
       AND segment.relid = claim.relid AND NOT segment.published;
    GET DIAGNOSTICS completed = ROW_COUNT;
    IF members = 0 OR completed <> members THEN
        RAISE EXCEPTION 'publication members do not match pending ledger rows'
            USING ERRCODE = '55000';
    END IF;
    UPDATE lakehouse.publication_claims SET verified_snapshot_id = snapshot WHERE operation_id = op;
    RETURN members;
END $$;

REVOKE ALL ON FUNCTION lakehouse.advance_publication_generation(uuid, bigint) FROM PUBLIC;
REVOKE ALL ON PROCEDURE lakehouse.authorize_publication_send(uuid, bigint, bytea, bigint, bigint, bytea)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION lakehouse.finalize_publication(uuid, bigint, uuid, uuid, bytea, bigint, bytea)
    FROM PUBLIC;
