\echo Use "ALTER EXTENSION lakehouse UPDATE TO '1.7'" to load this file. \quit

-- Version 1.7 is opt-in. The operator verifies the remote table and its
-- storage authority before enrollment. Foreground capture consumes only
-- local facts and never resolves or borrows the writer's remote credentials.
CREATE TABLE lakehouse.managed_capture_bindings (
    binding_id    uuid PRIMARY KEY,
    incarnation_id uuid NOT NULL,
    generation    bigint NOT NULL CHECK (generation > 0),
    relid         oid NOT NULL,
    authority_id  uuid NOT NULL,
    table_uuid    uuid NOT NULL,
    server_id     oid NOT NULL,
    target_identity text NOT NULL,
    catalog_endpoint text NOT NULL CHECK (catalog_endpoint <> ''),
    warehouse     text NOT NULL,
    object_endpoint text,
    storage_root  text NOT NULL,
    schema_id     integer NOT NULL CHECK (schema_id >= 0),
    spec_id       integer NOT NULL CHECK (spec_id >= 0),
    source_columns jsonb NOT NULL,
    UNIQUE (incarnation_id, generation),
    UNIQUE (binding_id, relid, authority_id, table_uuid)
);

-- Presence means bound-required. Absence of an active binding means writes
-- fail closed, including between revoke and replacement enrollment.
CREATE TABLE lakehouse.managed_capture_state (
    relid            oid PRIMARY KEY,
    incarnation_id   uuid UNIQUE,
    generation       bigint NOT NULL DEFAULT 0,
    active_binding_id uuid REFERENCES lakehouse.managed_capture_bindings,
    CHECK ((incarnation_id IS NULL AND generation = 0 AND active_binding_id IS NULL)
           OR (incarnation_id IS NOT NULL AND generation > 0))
);

ALTER TABLE lakehouse.lakehouse_delta_segments
    ADD COLUMN capture_binding_id uuid,
    ADD CHECK (capture_binding_id IS NULL OR authority_id IS NOT NULL),
    ADD FOREIGN KEY (capture_binding_id, relid, authority_id, table_uuid)
        REFERENCES lakehouse.managed_capture_bindings
        (binding_id, relid, authority_id, table_uuid);

CREATE TRIGGER guard_capture_binding_update
    BEFORE UPDATE OF capture_binding_id ON lakehouse.lakehouse_delta_segments
    FOR EACH ROW WHEN (OLD.capture_binding_id IS NOT NULL OR NEW.capture_binding_id IS NOT NULL)
    EXECUTE FUNCTION lakehouse.seal_publication_input();

CREATE FUNCTION lakehouse.check_capture_binding_insert() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    active_binding uuid;
BEGIN
    SELECT active_binding_id INTO active_binding FROM lakehouse.managed_capture_state
        WHERE relid = NEW.relid;
    IF (FOUND AND (active_binding IS NULL OR NEW.capture_binding_id IS DISTINCT FROM active_binding))
       OR (NOT FOUND AND NEW.capture_binding_id IS NOT NULL) THEN
        RAISE EXCEPTION 'managed capture binding changed or is inactive'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER check_capture_binding_insert
    BEFORE INSERT ON lakehouse.lakehouse_delta_segments
    FOR EACH ROW EXECUTE FUNCTION lakehouse.check_capture_binding_insert();

-- The first publication profile has a fixed local append shape. The operator
-- supplies the corresponding verified Iceberg schema and partition spec IDs.
CREATE FUNCTION lakehouse.capture_source_columns(target oid) RETURNS jsonb
LANGUAGE sql STABLE SET search_path = pg_catalog, pg_temp AS $$
    SELECT COALESCE((
        SELECT jsonb_agg(
            CASE WHEN a.attisdropped THEN jsonb_build_array(a.attnum, true)
                 ELSE jsonb_build_array(a.attnum, false, a.attname, a.atttypid, a.atttypmod,
                                        a.attcollation, a.attnotnull, a.attgenerated)
            END ORDER BY a.attnum)
        FROM pg_attribute a WHERE a.attrelid = target AND a.attnum > 0
    ), '[]'::jsonb)
    FROM pg_class c JOIN pg_am am ON am.oid = c.relam
    WHERE c.oid = target AND c.relkind = 'r' AND c.relpersistence = 'p'
      AND NOT c.relispartition AND am.amname = 'lakehouse'
      AND NOT EXISTS (
          SELECT 1 FROM pg_attribute a
          WHERE a.attrelid = target AND a.attnum > 0
            AND NOT a.attisdropped AND a.attgenerated <> ''
      );
$$;

CREATE FUNCTION lakehouse.capture_lock_managed_table(target regclass) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    relation_name text;
BEGIN
    SELECT format('%I.%I', n.nspname, c.relname)
    INTO relation_name
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_am a ON a.oid = c.relam
    WHERE c.oid = target AND c.relkind = 'r' AND c.relpersistence = 'p'
      AND NOT c.relispartition AND a.amname = 'lakehouse';
    IF relation_name IS NULL THEN
        RAISE EXCEPTION 'capture requires a permanent nonpartitioned lakehouse table'
            USING ERRCODE = '42809';
    END IF;

    EXECUTE format('LOCK TABLE ONLY %s IN ACCESS EXCLUSIVE MODE', relation_name);

    -- A DROP/CREATE or ALTER may have won while this operator waited.
    IF to_regclass(relation_name) IS DISTINCT FROM target OR NOT EXISTS (
        SELECT 1 FROM pg_class c JOIN pg_am a ON a.oid = c.relam
        WHERE c.oid = target AND c.relkind = 'r' AND c.relpersistence = 'p'
          AND NOT c.relispartition AND a.amname = 'lakehouse'
    ) THEN
        RAISE EXCEPTION 'managed capture table changed while acquiring its lock'
            USING ERRCODE = '55000';
    END IF;
END $$;

CREATE FUNCTION lakehouse.capture_check_legacy_write(target regclass) RETURNS void
AS 'MODULE_PATHNAME', 'lakehouse_capture_check_legacy_write'
LANGUAGE C STRICT;

CREATE FUNCTION lakehouse.require_managed_capture(target regclass) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    -- The operator must see all prior commits after acquiring the DDL lock.
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'managed capture transitions require READ COMMITTED'
            USING ERRCODE = '25001';
    END IF;
    PERFORM lakehouse.capture_lock_managed_table(target);
    PERFORM lakehouse.capture_check_legacy_write(target);
    IF EXISTS (
        SELECT 1 FROM lakehouse.lakehouse_delta_segments
        WHERE relid = target AND authority_id IS NULL AND NOT published
    ) THEN
        RAISE EXCEPTION 'reconcile pending legacy segments before requiring bound capture'
            USING ERRCODE = '55000';
    END IF;
    INSERT INTO lakehouse.managed_capture_state (relid) VALUES (target)
    ON CONFLICT ON CONSTRAINT managed_capture_state_pkey DO NOTHING;
END $$;

CREATE FUNCTION lakehouse.enroll_managed_capture(
    target regclass, target_identity text, server_name text, incarnation_id uuid,
    binding_id uuid, generation bigint, authority_id uuid, table_uuid uuid,
    storage_root text, schema_id integer, spec_id integer
) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    prior lakehouse.managed_capture_state%ROWTYPE;
    server_oid oid;
    server_options text[];
    source_shape jsonb;
BEGIN
    IF target_identity IS NULL OR target_identity !~ '^[^.]+[.].+$'
       OR server_name IS NULL OR server_name <> server_name::name::text
       OR incarnation_id IS NULL OR binding_id IS NULL OR generation IS NULL OR generation < 1
       OR authority_id IS NULL OR table_uuid IS NULL OR storage_root IS NULL
       OR storage_root !~ '^s3://[^/]+/.+$'
       OR schema_id IS NULL OR schema_id < 0 OR spec_id IS NULL OR spec_id < 0 THEN
        RAISE EXCEPTION 'enrollment requires a verified catalog and table binding'
            USING ERRCODE = '22023';
    END IF;
    PERFORM lakehouse.require_managed_capture(target);
    source_shape := lakehouse.capture_source_columns(target);
    IF source_shape IS NULL THEN
        RAISE EXCEPTION 'managed capture requires a fixed table without generated columns'
            USING ERRCODE = '0A000';
    END IF;
    SELECT * INTO STRICT prior FROM lakehouse.managed_capture_state WHERE relid = target;
    IF prior.active_binding_id IS NOT NULL
       OR generation - 1 IS DISTINCT FROM prior.generation
       OR (prior.incarnation_id IS NOT NULL AND incarnation_id <> prior.incarnation_id)
       OR (prior.incarnation_id IS NULL AND EXISTS (
           SELECT 1 FROM lakehouse.managed_capture_bindings b
           WHERE b.incarnation_id = enroll_managed_capture.incarnation_id)) THEN
        RAISE EXCEPTION 'revoke the active binding before enrolling the next generation'
            USING ERRCODE = '55000';
    END IF;

    SELECT s.oid, s.srvoptions INTO server_oid, server_options
    FROM pg_foreign_server s JOIN pg_foreign_data_wrapper w ON w.oid = s.srvfdw
    WHERE s.srvname = server_name::name AND w.fdwname = 'lakehouse_fdw';
    IF server_oid IS NULL THEN
        RAISE EXCEPTION 'lakehouse server "%" does not exist', server_name
            USING ERRCODE = '42704';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_options_to_table(server_options)
        WHERE option_name NOT IN ('endpoint', 'warehouse', 's3_endpoint')
    ) THEN
        RAISE EXCEPTION 'enrollment requires a publisher server'
            USING ERRCODE = '0A000';
    END IF;
    INSERT INTO lakehouse.managed_capture_bindings (
        binding_id, incarnation_id, generation, relid, authority_id, table_uuid,
        server_id, target_identity, catalog_endpoint, warehouse, object_endpoint,
        storage_root, schema_id, spec_id, source_columns
    ) VALUES (
        binding_id, incarnation_id, generation, target, authority_id, table_uuid,
        server_oid, target_identity,
        (SELECT option_value FROM pg_options_to_table(server_options) WHERE option_name = 'endpoint'),
        COALESCE((SELECT option_value FROM pg_options_to_table(server_options) WHERE option_name = 'warehouse'), ''),
        (SELECT option_value FROM pg_options_to_table(server_options) WHERE option_name = 's3_endpoint'),
        storage_root, schema_id, spec_id, source_shape
    );
    UPDATE lakehouse.managed_capture_state s
        SET incarnation_id = enroll_managed_capture.incarnation_id,
            generation = enroll_managed_capture.generation,
            active_binding_id = enroll_managed_capture.binding_id
        WHERE s.relid = target;
END $$;

CREATE FUNCTION lakehouse.revoke_managed_capture(target regclass, binding_id uuid, generation bigint)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'managed capture transitions require READ COMMITTED'
            USING ERRCODE = '25001';
    END IF;
    PERFORM lakehouse.capture_lock_managed_table(target);
    UPDATE lakehouse.managed_capture_state s SET active_binding_id = NULL
    WHERE s.relid = target AND s.active_binding_id = revoke_managed_capture.binding_id
          AND s.generation = revoke_managed_capture.generation;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'managed capture binding changed or is inactive'
            USING ERRCODE = '55000';
    END IF;
END $$;

-- Serialize the old attachment path with bound-required admission. Retained
-- bindings are independent of this mutable legacy catalog.
CREATE FUNCTION lakehouse.guard_capture_legacy_attach() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    -- CREATE/ALTER/DROP and capture admission both lock the target.
    -- Existing legacy attachments are still accepted for an unenrolled table.
    IF EXISTS (SELECT 1 FROM pg_class WHERE oid = NEW.relid AND relkind = 'r') THEN
        EXECUTE format('LOCK TABLE ONLY %s IN ACCESS SHARE MODE', NEW.relid::regclass);
    END IF;
    IF EXISTS (SELECT 1 FROM lakehouse.managed_capture_state WHERE relid = NEW.relid) THEN
        RAISE EXCEPTION 'bound capture requires explicit enrollment'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER guard_capture_legacy_attach
    BEFORE INSERT OR UPDATE ON lakehouse.managed_tables
    FOR EACH ROW EXECUTE FUNCTION lakehouse.guard_capture_legacy_attach();

CREATE OR REPLACE FUNCTION lakehouse.managed_tables_drop_gc() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    DELETE FROM lakehouse.managed_capture_state
    WHERE relid IN (
        SELECT objid FROM pg_event_trigger_dropped_objects() WHERE object_type = 'table');
    DELETE FROM lakehouse.managed_tables
    WHERE relid IN (
        SELECT objid FROM pg_event_trigger_dropped_objects() WHERE object_type = 'table');
END $$;

REVOKE ALL ON lakehouse.managed_capture_state, lakehouse.managed_capture_bindings FROM PUBLIC;
REVOKE ALL ON FUNCTION lakehouse.capture_source_columns(oid),
    lakehouse.check_capture_binding_insert(),
    lakehouse.capture_check_legacy_write(regclass),
    lakehouse.capture_lock_managed_table(regclass),
    lakehouse.require_managed_capture(regclass),
    lakehouse.enroll_managed_capture(regclass, text, text, uuid, uuid, bigint, uuid, uuid, text, integer, integer),
    lakehouse.revoke_managed_capture(regclass, uuid, bigint),
    lakehouse.guard_capture_legacy_attach() FROM PUBLIC;
