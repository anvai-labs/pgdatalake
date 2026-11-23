\echo Use "ALTER EXTENSION lakehouse UPDATE TO '1.13'" to load this file. \quit

CREATE FUNCTION lakehouse.hold_managed_publication_send(
    op uuid, generation bigint, input_hash bytea, parent bigint, proposed bigint,
    proposal_hash bytea, body bytea)
RETURNS jsonb AS 'MODULE_PATHNAME', 'pg_lakehouse_hold_managed_publication_send'
LANGUAGE c VOLATILE PARALLEL UNSAFE;

REVOKE ALL ON FUNCTION lakehouse.hold_managed_publication_send(uuid, bigint, bytea, bigint, bigint, bytea, bytea)
FROM PUBLIC;

COMMENT ON FUNCTION lakehouse.hold_managed_publication_send(uuid, bigint, bytea, bigint, bigint, bytea, bytea)
IS 'Retain one prepared managed proposal in a native top-level authorization transaction, then leave the operation lookup-only';
