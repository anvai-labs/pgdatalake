\echo Use "ALTER EXTENSION lakehouse UPDATE TO '1.17'" to load this file. \quit

-- Secret-only operator servers have no FDW scan handler. Keep these options
-- separate from ordinary FDW mappings and their access-key-only transport.
CREATE FUNCTION lakehouse.validate_publication_credential_options(text[], oid)
RETURNS void AS 'MODULE_PATHNAME', 'pg_lakehouse_validate_publication_credentials'
LANGUAGE c STRICT;
REVOKE ALL ON FUNCTION lakehouse.validate_publication_credential_options(text[], oid) FROM PUBLIC;

CREATE FOREIGN DATA WRAPPER lakehouse_publication_credentials
    NO HANDLER VALIDATOR lakehouse.validate_publication_credential_options;
REVOKE ALL ON FOREIGN DATA WRAPPER lakehouse_publication_credentials FROM PUBLIC;
