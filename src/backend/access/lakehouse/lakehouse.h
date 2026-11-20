/*-------------------------------------------------------------------------
 *
 * lakehouse.h
 *		Internal declarations shared between the lakehouse FDW (fdw.c)
 *		and the lakehouse TABLE access method (tableam.c). Both surfaces
 *		delegate to the same FFI session pipeline — one scan engine, two
 *		attach surfaces (doc 01 D1/D2).
 *
 * Portions Copyright (c) 2026, Anvai Labs
 *-------------------------------------------------------------------------
 */
#ifndef LAKEHOUSE_H
#define LAKEHOUSE_H

#include "postgres.h"

#include "pg_datalake_core.h"

/* options for one table (foreign table: server opts + table opts, table
 * wins; managed table: lakehouse.managed_tables identity row) */
typedef struct LakehouseOptions
{
	char	   *endpoint;
	char	   *warehouse;
	char	   *token;
	char	   *namespace;		/* levels before the last '.' of the table opt */
	char	   *table;			/* last '.'-level of the table option */
	char	   *s3_endpoint;
	char	   *s3_access_key;
	char	   *s3_secret_key;
	char	   *predicate_json; /* translated quals (slice 15) */
	char	   *snapshot_id;	/* time travel pin (slice 18); NULL = current */
}			LakehouseOptions;

/* Canonical RTE selector string ("ts:<ms>" / "id:<n>" / "ref:<name>") ->
 * selector JSON for the FFI (slice 19). */
extern char *lakehouse_BuildSelectorJson(const char *sel);

/* Plan a scan session (FFI pg_datalake_scan_new with selector priority:
 * RTE pin > snapshot_id option > current). Errors out with
 * ERRCODE_CONNECTION_EXCEPTION on failure — the FDW and the AM share the
 * same fail-fast contract (doc 03 §5). */
extern ScanSession *lakehouse_OpenSession(LakehouseOptions *opts,
										  const char *rte_selector);

#endif							/* LAKEHOUSE_H */
