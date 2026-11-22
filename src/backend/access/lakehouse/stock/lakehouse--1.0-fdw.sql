CREATE FUNCTION lakehouse.version() RETURNS text
AS 'MODULE_PATHNAME', 'pg_lakehouse_version'
LANGUAGE C STABLE PARALLEL SAFE;

CREATE FUNCTION lakehouse.lakehouse_fdw_validator(text[], oid) RETURNS void
AS 'MODULE_PATHNAME', 'lakehouse_fdw_validator'
LANGUAGE C STRICT;

CREATE FUNCTION lakehouse.lakehouse_fdw_handler() RETURNS fdw_handler
AS 'MODULE_PATHNAME', 'lakehouse_fdw_handler'
LANGUAGE C;

CREATE FOREIGN DATA WRAPPER lakehouse_fdw
    HANDLER lakehouse.lakehouse_fdw_handler
    VALIDATOR lakehouse.lakehouse_fdw_validator;
