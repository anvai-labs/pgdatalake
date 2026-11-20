-- 13-review blocker #6 (FDW side): bind credentials, destinations and
-- SQL privileges to the same principal (doc 09 authority model).
-- TDD anchors:
-- * the wrapper validator rejects unknown AND misplaced options at DDL
-- * storage credentials resolve ONLY from the user mapping — server- or
--   table-level bearer secrets are rejected at DDL and validated again
--   on use (legacy definitions)
-- * scan_files (caller-chosen endpoint + credentials) is not
--   PUBLIC-executable
-- * the drop-gc event trigger does not interfere with unrelated DDL
CREATE EXTENSION lakehouse;

-- canonical wrapper ships WITH the validator
CREATE FOREIGN DATA WRAPPER auth_w HANDLER lakehouse.lakehouse_fdw_handler
    VALIDATOR lakehouse.lakehouse_fdw_validator;

-- valid server: transport only, no secrets
CREATE SERVER auth_srv FOREIGN DATA WRAPPER auth_w
    OPTIONS (endpoint 'http://127.0.0.1:9', warehouse 'wh',
             s3_endpoint 'http://s3:9000');

-- credentials live in the USER MAPPING (per-user, ACL-controlled)
CREATE USER MAPPING FOR CURRENT_USER SERVER auth_srv
    OPTIONS (token 'auth-test-token-never-in-dumps',
             s3_access_key 'ak-test', s3_secret_key 'sk-test');

-- unknown option name on a server: rejected
CREATE SERVER bad_unknown FOREIGN DATA WRAPPER auth_w
    OPTIONS (endpoint 'http://127.0.0.1:9', endpooint 'typo');

-- misplaced: a SERVER is not a credential home
CREATE SERVER bad_cred FOREIGN DATA WRAPPER auth_w
    OPTIONS (endpoint 'http://127.0.0.1:9', s3_access_key 'k');

-- misplaced: a table-level storage endpoint could repurpose the
-- server's credential against a new destination — rejected
CREATE FOREIGN TABLE bad_ft (id bigint) SERVER auth_srv
    OPTIONS (table 'ns.tbl', s3_endpoint 'http://attacker:9000');

-- misplaced: a MAPPING is not a destination home
CREATE USER MAPPING FOR PUBLIC SERVER auth_srv
    OPTIONS (endpoint 'http://elsewhere:8181');

-- valid foreign table: identity + snapshot pin only
CREATE FOREIGN TABLE auth_ft (id bigint, name text) SERVER auth_srv
    OPTIONS (table 'analytics.events');

-- the extension ships its own canonical wrapper WITH the validator
SELECT fdwname, fdwvalidator::regprocedure
FROM pg_foreign_data_wrapper WHERE fdwname = 'lakehouse_fdw';

-- no secrets in catalog output: the server row shows transport only
SELECT srvoptions FROM pg_foreign_server WHERE srvname = 'auth_srv';

-- on-use validation: a LEGACY table carrying table-level credentials
-- (pre-validator era) errors instead of silently using them
CREATE FOREIGN DATA WRAPPER legacy_w HANDLER lakehouse.lakehouse_fdw_handler;
CREATE SERVER legacy_srv FOREIGN DATA WRAPPER legacy_w
    OPTIONS (endpoint 'http://127.0.0.1:9', warehouse 'wh',
             s3_endpoint 'http://s3:9000');
CREATE USER MAPPING FOR CURRENT_USER SERVER legacy_srv OPTIONS (token 't');
CREATE FOREIGN TABLE legacy_ft (id bigint) SERVER legacy_srv
    OPTIONS (table 'ns.tbl', s3_access_key 'ak', s3_secret_key 'sk');
SELECT count(*) FROM legacy_ft;

-- scan_files takes a caller-chosen endpoint + credentials: not
-- PUBLIC-executable
CREATE ROLE auth_reader NOLOGIN;
GRANT USAGE ON SCHEMA lakehouse TO auth_reader;
SET ROLE auth_reader;
SELECT count(*) FROM lakehouse.scan_files('http://127.0.0.1:9', 'wh',
                                          'tok', 'ns', 'tbl');
RESET ROLE;

-- the drop-gc event trigger must not interfere with unrelated DDL by
-- an ordinary owner
CREATE ROLE auth_dropper NOLOGIN;
GRANT USAGE, CREATE ON SCHEMA public TO auth_dropper;
SET ROLE auth_dropper;
CREATE TABLE unrelated_t (a int);
INSERT INTO unrelated_t VALUES (1);
DROP TABLE unrelated_t;
RESET ROLE;

-- cleanup
DROP FOREIGN TABLE auth_ft;
DROP FOREIGN TABLE legacy_ft;
DROP USER MAPPING FOR CURRENT_USER SERVER auth_srv;
DROP USER MAPPING FOR CURRENT_USER SERVER legacy_srv;
DROP SERVER legacy_srv;
DROP SERVER auth_srv;
DROP FOREIGN DATA WRAPPER legacy_w;
DROP FOREIGN DATA WRAPPER auth_w;
REVOKE USAGE ON SCHEMA lakehouse FROM auth_reader;
REVOKE USAGE, CREATE ON SCHEMA public FROM auth_dropper;
DROP ROLE auth_reader;
DROP ROLE auth_dropper;
DROP EXTENSION lakehouse;
