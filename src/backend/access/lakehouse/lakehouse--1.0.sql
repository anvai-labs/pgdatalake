/* Format-generic lakehouse storage engine support, v1.0 (M0 stub surface).
 *
 * specs/lakehouse/RULES.md §4: lakehouse-visible functions live in the
 * `lakehouse` schema. The full SQL surface (catalog management, time travel,
 * maintenance) grows this file / adds versioned scripts from M1 on.
 */

CREATE SCHEMA IF NOT EXISTS lakehouse;

CREATE FUNCTION lakehouse.version() RETURNS text
AS 'MODULE_PATHNAME', 'pg_lakehouse_version'
LANGUAGE C STABLE PARALLEL SAFE;
