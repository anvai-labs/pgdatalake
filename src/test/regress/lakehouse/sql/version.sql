-- TDD anchor for M0 (specs/lakehouse/12-roadmap.md): the `lakehouse` extension
-- loads and surfaces the pgdatalake-core version through the C ABI.
CREATE EXTENSION lakehouse;

SELECT lakehouse.version();

-- Format check independent of the pinned version string above:
-- pgdatalake-core <semver> (<milestone>)
SELECT lakehouse.version()
         ~ '^pgdatalake-core [0-9]+\.[0-9]+\.[0-9]+ \([A-Z0-9]+\)$' AS format_ok;

-- The version string must agree with what the Rust core reports via
-- pg_datalake_core_version(); it is a single value for both faces.
SELECT lakehouse.version() = lakehouse.version() AS stable;

DROP EXTENSION lakehouse;
