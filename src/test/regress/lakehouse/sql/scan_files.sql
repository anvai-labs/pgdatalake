-- TDD anchor for M1 slice 9: lakehouse.scan_files() plans a scan over a
-- REST catalog table and returns the data-file task list through the FFI
-- (specs/lakehouse/06-execution.md; the one-catalog principle lives in
-- 00-goals #3). Fixture-dependent happy paths are exercised by the cargo
-- example against `make lakehouse-up`; regress covers the SQL surface
-- deterministically: error class surfacing and STRICT null handling.
CREATE EXTENSION lakehouse;

-- unreachable endpoint -> connection exception, endpoint named in message
DO $$
DECLARE
    msg text;
    detail text;
BEGIN
    BEGIN
        PERFORM * FROM lakehouse.scan_files(
            'http://127.0.0.1:9', 'warehouse', '', 'ns', 'tbl',
            'http://s3:9000', 'key', 'secret');
    EXCEPTION WHEN OTHERS THEN
        msg := SQLSTATE;
        detail := SQLERRM;
    END;
    RAISE NOTICE 'caught sqlstate=%', msg;
    RAISE NOTICE 'endpoint_named=%', (position('127.0.0.1:9' in detail) > 0);
END $$;

-- STRICT: any NULL argument -> zero rows (SRF strict semantics)
SELECT lakehouse.scan_files(NULL, 'warehouse', '', 'ns', 'tbl', '', '', '');

-- function lives in the lakehouse schema with the documented signature
SELECT p.oid::regprocedure
FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = 'lakehouse' AND p.proname = 'scan_files';

DROP EXTENSION lakehouse;
