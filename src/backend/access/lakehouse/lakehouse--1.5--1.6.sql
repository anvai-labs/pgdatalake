\echo Use "ALTER EXTENSION lakehouse UPDATE TO '1.6'" to load this file. \quit

-- Old intents retained the digest only. Preserve that evidence and
-- require exact bounded bytes for every new authorization.
ALTER TABLE lakehouse.publication_attempts
    ADD COLUMN proposal_body bytea,
    ADD CONSTRAINT publication_proposal_body_size
        CHECK (proposal_body IS NOT NULL AND octet_length(proposal_body) BETWEEN 1 AND 65536)
        NOT VALID;

-- The existence of an intent fences replacement sends. Protect it even
-- from maintenance mistakes until a receipt-aware retention path exists.
CREATE FUNCTION lakehouse.guard_publication_attempt() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    RAISE EXCEPTION 'publication attempts are immutable' USING ERRCODE = '55000';
END $$;
CREATE TRIGGER guard_publication_attempt
    BEFORE UPDATE OR DELETE ON lakehouse.publication_attempts
    FOR EACH ROW EXECUTE FUNCTION lakehouse.guard_publication_attempt();
CREATE TRIGGER guard_publication_attempt_truncate
    BEFORE TRUNCATE ON lakehouse.publication_attempts
    FOR EACH STATEMENT EXECUTE FUNCTION lakehouse.guard_publication_attempt();
REVOKE ALL ON FUNCTION lakehouse.guard_publication_attempt() FROM PUBLIC;

DROP PROCEDURE lakehouse.authorize_publication_send(uuid, bigint, bytea, bigint, bigint, bytea);

-- CALL this as its own top-level transaction. A successful reply follows
-- the synchronous LOCAL commit, and only that caller can dispatch. A lost
-- reply leaves the same lookup-only state as a lost catalog reply.
CREATE PROCEDURE lakehouse.authorize_publication_send(
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

    INSERT INTO lakehouse.publication_attempts
        (operation_id, owner_generation, expected_parent, proposed_snapshot_id, proposal_digest, proposal_body)
    VALUES (op, generation, parent, proposed, proposal_hash, body);
    intent_xid := pg_current_xact_id()::xid;
    COMMIT;
END $$;

REVOKE ALL ON PROCEDURE lakehouse.authorize_publication_send(uuid, bigint, bytea, bigint, bigint, bytea, bytea)
    FROM PUBLIC;
