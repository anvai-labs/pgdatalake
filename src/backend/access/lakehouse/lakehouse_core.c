/*-------------------------------------------------------------------------
 *
 * lakehouse_core.c
 *		C glue for pgdatalake-core, the Rust core of the format-generic
 *		lakehouse storage engine (specs/lakehouse/01-architecture.md,
 *		decision D2). Iceberg is the first FORMAT layer inside the core.
 *
 * M0 scope: SQL exposure of the core version. The full TableAM
 * implementation grows alongside this file from M2 on (iceberg.c,
 * scan.c, insert.c, ...; see doc 01 §2 for the planned layout).
 *
 * Portions Copyright (c) 2026, Anvai Labs
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "pg_datalake_core.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

/*
 * lakehouse.version()
 *
 * Returns the pgdatalake-core version string (see core/pgdatalake-core/src/ffi.rs
 * for the documented format `pgdatalake-core <semver> (<milestone>)`). The Rust
 * core owns the string; it is a static, so no lifetime management is needed.
 */
PG_FUNCTION_INFO_V1(pg_lakehouse_version);

Datum
pg_lakehouse_version(PG_FUNCTION_ARGS)
{
	const char *core_version = pg_datalake_core_version();

	PG_RETURN_TEXT_P(cstring_to_text(core_version));
}
