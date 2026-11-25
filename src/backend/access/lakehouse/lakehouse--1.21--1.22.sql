\echo Use "ALTER EXTENSION lakehouse UPDATE TO '1.22'" to load this file. \quit

-- ---------------------------------------------------------------------------
-- Spec 19 §4 (L0 telemetry): extension-internal query stats.
--
-- lakehouse.query_stats is INTERNAL (same posture as the staged-delta
-- store): created by this script, REVOKE ALL FROM PUBLIC, written by the
-- capture hooks as the table owner. Tenant admins own it (the extension
-- owner); no role sees it unless the admin grants explicitly. Latest
-- records live here; the admin-only flush drains aged rows into the
-- tenant's managed Iceberg archive (object store) via publish_table.
-- ---------------------------------------------------------------------------

CREATE TABLE lakehouse.query_stats (
    captured_at    timestamptz NOT NULL DEFAULT now(),
    dbid           oid NOT NULL,
    userid         oid NOT NULL,
    queryid        bigint NOT NULL,
    rows           bigint,          -- DML rows; NULL for SELECT (v1)
    total_ms       double precision,
    foreign_scans  integer NOT NULL DEFAULT 0
);
CREATE INDEX lakehouse_query_stats_captured_idx
    ON lakehouse.query_stats (captured_at);
REVOKE ALL ON lakehouse.query_stats FROM PUBLIC;

-- Explicit, audited operator-share consent: a single-row ledger. Sharing
-- stats with the operator account NEVER happens implicitly —
-- share_query_stats_with_operator fails closed (55000) until an admin
-- grants here. The user trains on their own patterns via query_stats
-- directly; the operator trains on fleet data only across consented
-- tenants.
CREATE TABLE lakehouse.telemetry_consent (
    id              boolean PRIMARY KEY DEFAULT true CHECK (id),
    granted         boolean NOT NULL DEFAULT false,
    endpoint        text,
    CONSTRAINT telemetry_consent_endpoint CHECK (
        (NOT granted) OR (endpoint IS NOT NULL)),
    consent_version text NOT NULL DEFAULT 'v1',
    granted_by      name,
    granted_at      timestamptz,
    shared_at       timestamptz
);
REVOKE ALL ON lakehouse.telemetry_consent FROM PUBLIC;
INSERT INTO lakehouse.telemetry_consent (id) VALUES (true);

-- Admin consent gate (exec revoked from PUBLIC below): record who
-- consented, when, and to which operator endpoint.
CREATE FUNCTION lakehouse.grant_operator_telemetry(endpoint text)
RETURNS void
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, lakehouse
AS $$
    UPDATE lakehouse.telemetry_consent
    SET granted = true, endpoint = grant_operator_telemetry.endpoint,
        granted_by = session_user::text::name, granted_at = now()
    WHERE id;
$$;

CREATE FUNCTION lakehouse.revoke_operator_telemetry()
RETURNS void
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, lakehouse
AS $$
    UPDATE lakehouse.telemetry_consent
    SET granted = false, shared_at = NULL
    WHERE id;
$$;

-- Drain latest records into the tenant's managed Iceberg archive and
-- publish them to object storage. Requires the archive table to exist
-- (bootstrap: CREATE TABLE lakehouse.query_stats_archive (...) USING
-- lakehouse + lakehouse.attach_table) — without it the flush is a
-- documented no-op so the heap table keeps buffering under retention.
CREATE FUNCTION lakehouse.flush_query_stats(
    OUT archived integer, OUT snapshot_id bigint, OUT state text)
RETURNS record
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, lakehouse
AS $$
DECLARE
    v_archived bigint := 0;
    v_segs int;
    v_snap bigint;
    v_pubstate text;
BEGIN
    IF to_regclass('lakehouse.query_stats_archive') IS NULL THEN
        archived := 0; snapshot_id := NULL; state := 'no archive';
        RETURN;
    END IF;

    -- publish anything staged by a PREVIOUS call: the publisher cannot
    -- emit segments staged in its own (still uncommitted) transaction,
    -- so a row drained on call N publishes on call N+1
    SELECT pub.published_segments, pub.snapshot_id, pub.state
        INTO v_segs, v_snap, v_pubstate
        FROM lakehouse.publish_table('lakehouse.query_stats_archive') pub;

    WITH moved AS (
        DELETE FROM lakehouse.query_stats RETURNING *
    ), ins AS (
        INSERT INTO lakehouse.query_stats_archive
            (captured_at, dbid, userid, queryid, rows, total_ms, foreign_scans)
        -- naive archive column: normalize to UTC wall-clock (AT TIME
        -- ZONE on a timestamptz yields the UTC reading regardless of
        -- the session zone)
        SELECT captured_at AT TIME ZONE 'UTC', dbid::bigint,
               userid::bigint, queryid, rows, total_ms, foreign_scans
        FROM moved
        RETURNING 1
    )
    SELECT count(*) INTO v_archived FROM ins;

    IF v_pubstate = 'published' THEN
        archived := v_segs; snapshot_id := v_snap; state := 'published';
    ELSIF v_archived > 0 THEN
        archived := v_archived; snapshot_id := NULL; state := 'staged';
    ELSE
        archived := 0; snapshot_id := NULL; state := 'no change';
    END IF;
END;
$$;

-- Same drain, gated on the consent ledger. WITHOUT consent: fail closed.
-- WITH consent: drain + publish + stamp the ledger (the transport of the
-- published batch into the operator's own warehouse is the follow-up
-- slice per spec 19 §4).
CREATE FUNCTION lakehouse.share_query_stats_with_operator(
    OUT shared integer, OUT snapshot_id bigint, OUT state text)
RETURNS record
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, lakehouse
AS $$
DECLARE
    v_granted boolean;
    r record;
BEGIN
    SELECT granted INTO v_granted FROM lakehouse.telemetry_consent WHERE id;
    IF v_granted IS DISTINCT FROM true THEN
        RAISE EXCEPTION
            USING ERRCODE = '55000',
            MESSAGE = 'operator telemetry sharing is not consented',
            HINT = 'an admin must call lakehouse.grant_operator_telemetry(endpoint)';
    END IF;
    SELECT * INTO r FROM lakehouse.flush_query_stats();
    shared := r.archived;
    snapshot_id := r.snapshot_id;
    state := r.state;
    -- stamp the ledger only when rows actually moved to the archive
    IF r.archived > 0 THEN
        UPDATE lakehouse.telemetry_consent SET shared_at = now() WHERE id;
    END IF;
END;
$$;

REVOKE EXECUTE ON FUNCTION lakehouse.grant_operator_telemetry(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION lakehouse.revoke_operator_telemetry() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION lakehouse.flush_query_stats() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION lakehouse.share_query_stats_with_operator() FROM PUBLIC;
