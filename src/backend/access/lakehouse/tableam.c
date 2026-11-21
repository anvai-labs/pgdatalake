/*-------------------------------------------------------------------------
 *
 * tableam.c
 *		`USING lakehouse` TABLE access method (specs/lakehouse/02-sql-
 *		surface.md §3, doc 01 D2). M2 slice 26 skeleton: managed tables
 *		are ordinary pg_class rows (relkind 'r') whose scans DELEGATE to
 *		the FDW session pipeline (lakehouse_OpenSession + the
 *		pg_datalake_rows_* machinery — one scan engine, two attach
 *		surfaces). Table identity lives in lakehouse.managed_tables
 *		(attach_table); CREATE-time wiring of the identity row lands with
 *		managed DDL (slice 27).
 *
 * Not yet implemented (each ereports a clean ERRCODE_FEATURE_NOT_
 * SUPPORTED naming its slice): INSERT (27), UPDATE/DELETE/TRUNCATE (28),
 * indexes, parallel scans, TOAST, CLUSTER/TABLESPACE moves, ANALYZE
 * sampling. Writing callbacks exist as stubs so the executor's worst
 * path is a loud error, never a NULL deref.
 *
 * Portions Copyright (c) 2026, Anvai Labs
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/multixact.h"
#include "access/relation.h"
#include "access/sdir.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "access/heaptoast.h"
#include "utils/memutils.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "nodes/miscnodes.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "foreign/foreign.h"
#include "commands/defrem.h"
#include "catalog/pg_am_d.h"
#include "catalog/storage_xlog.h"
#include "catalog/storage.h"
#include "commands/vacuum.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "storage/smgr.h"
#include "utils/lsyscache.h"
#include "access/reloptions.h"
#include "utils/guc.h"
#include "executor/spi.h"
#include "utils/rel.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include "lakehouse.h"

#include "utils/json.h"
#include "utils/numeric.h"
#include "utils/typcache.h"
#include "varatt.h"

#include "batch_fill.h"

/* ---------------- identity catalog lookup ---------------- */

/*
 * Resolve lakehouse.managed_tables' oid (extension table, so not a
 * compile-time oid; cached and re-resolved if the extension is recreated).
 */
static Oid
get_managed_tables_relid(void)
{
	static Oid	cached_relid = InvalidOid;

	if (OidIsValid(cached_relid) &&
		SearchSysCacheExists1(RELOID, ObjectIdGetDatum(cached_relid)))
		return cached_relid;

	{
		Oid			ns = get_namespace_oid("lakehouse", false);
		Oid			relid = get_relname_relid("managed_tables", ns);

		if (!OidIsValid(relid))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("lakehouse.managed_tables is missing — "
							"reinstall the lakehouse extension")));
		cached_relid = relid;
		return relid;
	}
}

/*
 * Load the identity row for `relid` into `opts`. Returns false when the
 * table is un-attached (the caller reports a clean feature error).
 */
static bool
ExtractManagedIdentity(Oid relid, LakehouseOptions *opts)
{
	/* merge mode: caller's opts (possibly from WITH-options) keeps its
	 * identity fields; only NULL fields get filled from the attach row */
	Relation	mt;
	TableScanDesc scan;
	TupleTableSlot *slot;
	bool		found = false;
	char	   *identity = NULL;
	Oid			serverid = InvalidOid;

	mt = table_open(get_managed_tables_relid(), AccessShareLock);
	scan = table_beginscan(mt, GetActiveSnapshot(), 0, NULL, 0);
	/* the slot type must be the scanned table's AM's own (heap scans
	 * refuse minimal/virtual slots — on-disk tuple storage) */
	slot = MakeSingleTupleTableSlot(RelationGetDescr(mt),
									table_slot_callbacks(mt));

	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		bool		isnull;
		Datum		v = slot_getattr(slot, 1, &isnull);

		if (!isnull && DatumGetObjectId(v) == relid)
		{
			int			i;

			for (i = 2; i <= 4; i++)
			{
				v = slot_getattr(slot, i, &isnull);
				if (isnull)
					continue;
				switch (i)
				{
					case 2:
						identity = text_to_cstring(DatumGetTextPP(v));
						break;
					case 3:
						serverid = DatumGetObjectId(v);
						break;
					case 4:
						opts->snapshot_id =
							text_to_cstring(DatumGetTextPP(v));
						break;
				}
			}
			found = true;
			break;
		}
	}

	ExecDropSingleTupleTableSlot(slot);
	table_endscan(scan);
	table_close(mt, AccessShareLock);

	if (found && serverid != InvalidOid)
	{
		/*
		 * Authority model (doc 09, 13-review blocker #6): endpoint and
		 * storage scope come from the SERVER; credentials come from the
		 * USER MAPPING of the effective user — never from table options
		 * or this table (both are dumpable).
		 */
		ForeignServer *server = GetForeignServer(serverid);
		ListCell   *lc;

		if (opts->endpoint == NULL || opts->warehouse == NULL ||
			opts->s3_endpoint == NULL)
		{
			foreach(lc, server->options)
			{
				DefElem    *def = (DefElem *) lfirst(lc);

				if (strcmp(def->defname, "endpoint") == 0 &&
					opts->endpoint == NULL)
					opts->endpoint = defGetString(def);
				else if (strcmp(def->defname, "warehouse") == 0 &&
						 opts->warehouse == NULL)
					opts->warehouse = defGetString(def);
				else if (strcmp(def->defname, "s3_endpoint") == 0 &&
						 opts->s3_endpoint == NULL)
					opts->s3_endpoint = defGetString(def);
			}
		}
		{
			UserMapping *um = GetUserMapping(GetUserId(), serverid);

			foreach(lc, um->options)
			{
				DefElem    *def = (DefElem *) lfirst(lc);

				if (strcmp(def->defname, "token") == 0 &&
					opts->token == NULL)
					opts->token = defGetString(def);
				else if (strcmp(def->defname, "s3_access_key") == 0 &&
						 opts->s3_access_key == NULL)
					opts->s3_access_key = defGetString(def);
				else if (strcmp(def->defname, "s3_secret_key") == 0 &&
						 opts->s3_secret_key == NULL)
					opts->s3_secret_key = defGetString(def);
			}
		}
	}

	if (found && identity != NULL)
	{
		if (opts->table != NULL && identity != NULL)
		{
			/* caller already has an identity (WITH-options): merge done */
			return opts->endpoint != NULL;
		}
		{
			/* namespace = before the last '.', table = after (doc 03 §3) */
			const char *last_dot = strrchr(identity, '.');

			if (last_dot != NULL)
			{
				opts->namespace = pnstrdup(identity, last_dot - identity);
				opts->table = pstrdup(last_dot + 1);
			}
			else
			{
				opts->namespace = pstrdup("");
				opts->table = pstrdup(identity);
			}
		}
	}
	return found;
}

/* ---------------- managed-write state (M2 slice 27b) ---------------- */

/*
 * Per-transaction, per-relation write state. Table AMs are stateless
 * between callbacks, so INSERTs accumulate here; the xact callback
 * commits at PRE-COMMIT (doc 07 §2 step 2-3) and frees at abort. The
 * hash lives for exactly one transaction — it is destroyed in the
 * callback, so an entry is always current-txn (the own-writes overlay
 * relies on that).
 */
/*
 * COW UPDATE/DELETE state (M2 slice 28). A deleted/updated row is a
 * TOMBSTONE: (base-snapshot file, 0-based row index). Affected files are
 * REWRITTEN at PRE-COMMIT (surviving rows pushed as new files, the
 * original committed as a DELETED manifest entry — copy-on-write, doc 07
 * §3). Rows are never modified in place; the file's bytes are history.
 */
typedef struct LakehouseTombFile
{
	char	   *path;			/* base-snapshot file URI (mcxt-owned) */
	uint64	   *rows;			/* 0-based row indexes, sorted ascending */
	SubTransactionId *row_subids; /* owning subxact per tombstone */
	int			nrows;
	int			rows_cap;
	int64		record_count;	/* identity of the DELETED manifest entry */
	int64		file_size_in_bytes;
}			LakehouseTombFile;

typedef struct LakehouseWriteEntry
{
	Oid			relid;
	WriteSession *ws;			/* FFI write session; NULL until 1st insert */
	MemoryContext mcxt;			/* all entry allocations live here */
	/* own-writes overlay: staged tuples, copied datum-wise into mcxt */
	Datum	   *staged_values;
	bool	   *staged_isnull;
	SubTransactionId *staged_subids; /* owning subxact per staged row */
	int			staged_rows;
	int			natts;
	TupleDesc	tupdesc;
	/* identity, resolved at first insert */
	LakehouseOptions opts;
	bool		have_identity;
	/* COW tombstones (slice 28): linear scan over AFFECTED FILES (small:
	 * files one statement touches), sorted rows + bsearch within a file */
	LakehouseTombFile **tomb_files;
	int			n_tomb_files;
	int			tomb_cap;
	/* input-function plumbing for the PRE-COMMIT rewrite pass (reads a
	 * base file, re-encodes surviving rows exactly like staged ones) */
	FmgrInfo   *in_funcs;
	Oid		   *typioparams;
	bool		have_in_funcs;
}			LakehouseWriteEntry;

/* TID encoding (slice 28): block = task_idx * MAX_OFFSET + (rowidx >> 11),
 * offset = (rowidx & 0x7FF) + 1 — offset stays within MaxOffsetNumber,
 * rowidx up to ~4M per task, task_idx up to ~1M. Row indexes beyond the
 * encoding range error loudly at DML time rather than mis-targeting. */
#define LAKEHOUSE_TID_FANOUT MaxOffsetNumber

/*
 * Scan↔DML registry: the scan that produced a tid, per relid. DELETE/
 * UPDATE call tuple_delete/UPDATE with only (rel, tid) — the executor's
 * target scan is alive at that moment and its pinned snapshot's task
 * list maps tid → (file, rowidx). One entry per relid; concurrent scans
 * of the same rel (self-join) plan the SAME snapshot and task list, so
 * resolution is identical whichever scan is registered.
 */
typedef struct LakehouseTableScanDescData LakehouseTableScanDescData;

typedef struct LakehouseScanRegistryEntry
{
	Oid			relid;
	LakehouseTableScanDescData *scan;
	SubTransactionId subid;
}			LakehouseScanRegistryEntry;

static HTAB *scan_registry = NULL;

/* lakehouse GUCs (defined + registered in _PG_init, spec 15 P2) */
extern int		lakehouse_write_mode;	/* enum: 1 = cow, 2 = staged */
extern int		lakehouse_target_file_size;
extern int		lakehouse_publish_max_lag;
extern int		lakehouse_cache_max_bytes;
#define LAKEHOUSE_WRITE_MODE_COW		1
#define LAKEHOUSE_WRITE_MODE_STAGED		2

static void staged_capture_segment(LakehouseWriteEntry *entry);


static HTAB *write_entries = NULL;

static void append_cell_json(StringInfoData *buf, Form_pg_attribute attr,
								 Datum v, bool isnull);
static LakehouseTombFile *tomb_file_get(LakehouseWriteEntry *entry,
										const char *path, bool create);
static void tomb_file_add_row(LakehouseWriteEntry *entry,
							  LakehouseTombFile *tf, uint64 rowidx);
static bool tomb_file_has_row(const LakehouseTombFile *tf, uint64 rowidx);
static void open_write_session(LakehouseWriteEntry *entry, TupleDesc tupdesc);
static char *build_projection_json(TupleDesc tupdesc);
static void ensure_in_funcs(LakehouseWriteEntry *entry);

static void
lakehouse_xact_callback(XactEvent event, void *arg)
{
	HASH_SEQ_STATUS seq;
	LakehouseWriteEntry *entry;

	if (write_entries == NULL)
		return;

	/*
	 * Two-phase commit is rejected with pending managed writes BEFORE
	 * prepare (13-review blocker #3): a prepared transaction's remote
	 * commit decision must survive crashes, which this AM's recovery
	 * protocol does not yet support.
	 */
	if (event == XACT_EVENT_PRE_PREPARE && write_entries != NULL &&
		hash_get_num_entries(write_entries) > 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("two-phase commit is not supported with managed lakehouse writes"),
				 errdetail("ROLLBACK or COMMIT the transaction before PREPARE.")));

	if (event == XACT_EVENT_PRE_COMMIT || event == XACT_EVENT_PARALLEL_PRE_COMMIT)
	{
		/*
		 * doc 07 §2 steps 2-3: flush + build commit + commitTable with
		 * bounded retry. An error here aborts the transaction (the ABORT
		 * callback then frees the sessions) — no partial commit ever
		 * exists remotely (Iceberg's core guarantee).
		 */
		hash_seq_init(&seq, write_entries);
		while ((entry = hash_seq_search(&seq)) != NULL)
		{
			/* staged mode (D1-rev): the delta row IS the commit —
			 * durability from the heap+WAL machinery, no remote I/O */
			if (lakehouse_write_mode == LAKEHOUSE_WRITE_MODE_STAGED &&
				entry->staged_rows > 0)
			{
				staged_capture_segment(entry);
				continue;
			}
			if (entry->ws == NULL || (entry->staged_rows == 0 &&
									  entry->n_tomb_files == 0))
				continue;
			{
				char		errbuf[1024];
				char	   *loc;
				TupleDesc	tupdesc = entry->tupdesc;
				StringInfoData row;
				int			i;

				/*
				 * COW rewrite pass (slice 28): each tombstoned file is
				 * re-read from the BASE snapshot; surviving rows are
				 * converted (input functions) and re-encoded exactly like
				 * staged rows. The original files ride to the commit as
				 * DELETED entries. Reading the CURRENT snapshot here would
				 * be a lost-update race — a concurrent writer may have
				 * already replaced these files; the commit's liveness
				 * check catches that and aborts.
				 */
				for (i = 0; i < entry->n_tomb_files; i++)
				{
					LakehouseTombFile *tf = entry->tomb_files[i];
					char	   *proj = build_projection_json(tupdesc);
					RowsHandle *rows;
					uint64		src_row = 0;
					int			survivors = 0;

					ensure_in_funcs(entry);
					errbuf[0] = '\0';
					rows = pg_datalake_rows_open_path(entry->ws,
													  tf->path,
													  proj,
													  errbuf, sizeof(errbuf));
					if (rows == NULL)
						ereport(ERROR,
								(errcode(ERRCODE_IO_ERROR),
								 errmsg("lakehouse: COW rewrite could not open %s: %s",
										tf->path, errbuf)));
					for (;;)
					{
						char		nst_err[1024];
						int			nst;

						nst_err[0] = '\0';
						nst = pg_datalake_rows_next(rows, nst_err,
													sizeof(nst_err));
						if (nst == 1)
							break;
						if (nst == 2)
							ereport(ERROR,
									(errcode(ERRCODE_DATA_EXCEPTION),
									 errmsg("lakehouse: COW rewrite read failed in %s: %s",
											tf->path,
											nst_err[0] != '\0' ? nst_err : "unknown error")));
						if (!tomb_file_has_row(tf, src_row))
						{
							int			k;
							bool		first_col = true;

							initStringInfo(&row);
							appendStringInfoChar(&row, '[');
							for (k = 0; k < tupdesc->natts; k++)
							{
								int			pcol;
								Form_pg_attribute attr = TupleDescAttr(tupdesc, k);

								if (attr->attisdropped)
									continue;
								/* projection column = ordinal among visible attrs */
								pcol = k;
								for (int vis = 0; vis < k; vis++)
									if (TupleDescAttr(tupdesc, vis)->attisdropped)
										pcol--;
								if (pg_datalake_rows_col_null(rows, pcol))
								{
									if (!first_col)
										appendStringInfoChar(&row, ',');
									appendStringInfoString(&row, "null");
								}
								else
								{
									Datum		v = InputFunctionCall(&entry->in_funcs[k],
											pg_datalake_rows_col_text(rows, pcol),
											entry->typioparams[k],
											attr->atttypmod);

									if (!first_col)
										appendStringInfoChar(&row, ',');
									append_cell_json(&row, attr, v, false);
								}
								first_col = false;
							}
							appendStringInfoChar(&row, ']');
							errbuf[0] = '\0';
							if (!pg_datalake_write_push(entry->ws, row.data,
														errbuf, sizeof(errbuf)))
								ereport(ERROR,
										(errcode(ERRCODE_DATA_EXCEPTION),
										 errmsg("lakehouse: COW rewrite could not re-stage surviving row of %s: %s",
												tf->path, errbuf)));
							pfree(row.data);
							survivors++;
						}
						src_row++;
					}
					pg_datalake_rows_close(rows);
					elog(LOG, "lakehouse COW rewrite: %s -> %d surviving rows "
						 "(%lld tombstoned)", tf->path, survivors,
						 (long long) tf->nrows);
				}

				/* push the SURVIVING staged rows (post subxact pruning)
				 * into the accumulator now, then flush + commit */
				for (i = 0; i < entry->staged_rows; i++)
				{
					int			k;

					initStringInfo(&row);
					appendStringInfoChar(&row, '[');
					for (k = 0; k < tupdesc->natts; k++)
					{
						/* dropped attributes are omitted from the schema
						 * (build_schema_json) and from the row too — the
						 * cell count must match the column count
						 * (13-review #8b) */
						if (TupleDescAttr(tupdesc, k)->attisdropped)
							continue;
						if (k > 0)
							appendStringInfoChar(&row, ',');
						append_cell_json(&row,
										 TupleDescAttr(tupdesc, k),
										 entry->staged_values[(size_t) i * entry->natts + k],
										 entry->staged_isnull[(size_t) i * entry->natts + k]);
					}
					appendStringInfoChar(&row, ']');

					errbuf[0] = '\0';
					if (!pg_datalake_write_push(entry->ws, row.data,
												errbuf, sizeof(errbuf)))
						ereport(ERROR,
								(errcode(ERRCODE_DATA_EXCEPTION),
								 errmsg("lakehouse: could not stage row: %s",
										errbuf)));
					pfree(row.data);
				}

				errbuf[0] = '\0';
				if (entry->n_tomb_files > 0)
				{
					/* overwrite commit (slice 28): staged/rewritten files
					 * ADDED, the tombstoned originals DELETED entries */
					StringInfoData del;
					int			j;

					initStringInfo(&del);
					appendStringInfoChar(&del, '[');
					for (j = 0; j < entry->n_tomb_files; j++)
					{
						LakehouseTombFile *tf = entry->tomb_files[j];

						if (j > 0)
							appendStringInfoChar(&del, ',');
						appendStringInfoChar(&del, '[');
						escape_json(&del, tf->path); /* emits its own quotes */
						appendStringInfo(&del, ", %lld, %lld]",
										 (long long) tf->record_count,
										 (long long) tf->file_size_in_bytes);
					}
					appendStringInfoChar(&del, ']');
					loc = pg_datalake_write_commit_overwrite(entry->ws, del.data,
															 3, errbuf,
															 sizeof(errbuf));
					pfree(del.data);
				}
				else
					loc = pg_datalake_write_commit(entry->ws, 3, errbuf, sizeof(errbuf));
				if (loc == NULL)
					ereport(ERROR,
							(errcode(ERRCODE_CONNECTION_EXCEPTION),
							 errmsg("lakehouse: managed commit failed for %s: %s",
									get_rel_name(entry->relid) ? get_rel_name(entry->relid) : "?",
									errbuf)));
				pg_datalake_write_commit_result_free(loc);
			}
		}
	}

	/* teardown: free FFI sessions + the hash. PRE_COMMIT's commit errors
	 * leave the state intact so the ABORT callback reaps it. */
	if (event == XACT_EVENT_PRE_COMMIT || event == XACT_EVENT_ABORT ||
		event == XACT_EVENT_PARALLEL_ABORT ||
		event == XACT_EVENT_COMMIT || event == XACT_EVENT_PREPARE ||
		event == XACT_EVENT_PARALLEL_COMMIT ||
		event == XACT_EVENT_PARALLEL_PRE_COMMIT)
	{
		hash_seq_init(&seq, write_entries);
		while ((entry = hash_seq_search(&seq)) != NULL)
		{
			if (entry->ws != NULL)
				pg_datalake_write_free(entry->ws);
			MemoryContextDelete(entry->mcxt);
		}
		hash_destroy(write_entries);
		write_entries = NULL;
	}
}

/*
 * Map a Postgres attribute to its Iceberg type name (doc 05 mapping);
 * unsupported types are a clean CREATE-time-style rejection.
 */
static const char *
iceberg_type_name(Form_pg_attribute attr)
{
	switch (attr->atttypid)
	{
		case BOOLOID:
			return "boolean";
		case INT2OID:
		case INT4OID:
			return "int";
		case INT8OID:
			return "long";
		case FLOAT4OID:
			return "float";
		case FLOAT8OID:
			return "double";
		case TEXTOID:
		case VARCHAROID:
		case NAMEOID:
		case BPCHAROID:
			return "string";
		case DATEOID:
			return "date";
		case TIMEOID:
			return "time";
		case TIMESTAMPOID:
			return "timestamp";
		case TIMESTAMPTZOID:
			return "timestamptz";
		case NUMERICOID:
			{
				int32		m = attr->atttypmod - VARHDRSZ;
				int			precision = (m >> 16) & 0xffff;
				int			scale = (((m) & 0x7ff) ^ 1024) - 1024;
				static char buf[NAMEDATALEN];

				if (scale < 0)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("lakehouse: negative-scale numeric is not supported on managed tables"),
							 errdetail("Iceberg decimal requires scale >= 0.")));
				snprintf(buf, sizeof(buf), "decimal(%d,%d)", precision, scale);
				return buf;
			}
		case UUIDOID:
			return "uuid";
		case BYTEAOID:
			return "binary";
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("lakehouse: column \"%s\" has type %s, which is not supported on managed tables yet",
							NameStr(attr->attname), format_type_be(attr->atttypid)),
					 errdetail("Managed-table primitive coverage follows doc 05's mapping; nested types land with their own slices.")));
			return "";			/* unreachable */
	}
}

/* projection for pg_datalake_rows_open_path: counted JSON array of the
 * relation's visible attribute names (same shape as the scan projection). */
static char *
build_projection_json(TupleDesc tupdesc)
{
	StringInfoData proj;
	bool		first_col = true;
	int			i;

	initStringInfo(&proj);
	appendStringInfoChar(&proj, '[');
	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		if (attr->attisdropped)
			continue;
		if (!first_col)
			appendStringInfoChar(&proj, ',');
		escape_json(&proj, NameStr(attr->attname)); /* emits its own quotes */
		first_col = false;
	}
	appendStringInfoChar(&proj, ']');
	return proj.data;
}

/* input-function plumbing for the PRE-COMMIT rewrite pass: surviving rows
 * read from a base file are converted to datums and re-encoded exactly
 * like staged rows (one code path for every cell type). */
static void
ensure_in_funcs(LakehouseWriteEntry *entry)
{
	TupleDesc	tupdesc = entry->tupdesc;
	int			i;

	if (entry->have_in_funcs)
		return;
	{
		MemoryContext old = MemoryContextSwitchTo(entry->mcxt);

		entry->in_funcs = (FmgrInfo *)
			palloc0(tupdesc->natts * sizeof(FmgrInfo));
		entry->typioparams = (Oid *)
			palloc0(tupdesc->natts * sizeof(Oid));
		for (i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

			if (attr->attisdropped)
				continue;
			getTypeInputInfo(attr->atttypid,
							 &entry->in_funcs[i].fn_oid,
							 &entry->typioparams[i]);
			fmgr_info(entry->in_funcs[i].fn_oid, &entry->in_funcs[i]);
		}
		MemoryContextSwitchTo(old);
	}
	entry->have_in_funcs = true;
}

/* schema_json for pg_datalake_write_new, from the relation's tupdesc */
static char *
build_schema_json(TupleDesc tupdesc)
{
	StringInfoData buf;
	bool		first = true;
	int			i;

	initStringInfo(&buf);
	appendStringInfoString(&buf, "[");
	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		if (attr->attisdropped)
			continue;
		if (!first)
			appendStringInfoChar(&buf, ',');
		appendStringInfo(&buf, "{\"name\": ");
		escape_json(&buf, NameStr(attr->attname));
		appendStringInfo(&buf, ", \"type\": \"%s\"}", iceberg_type_name(attr));
		first = false;
	}
	appendStringInfoChar(&buf, ']');
	return buf.data;
}

/* one cell of the typed row-JSON (values the FFI parses, slice 27b-i) */
static void
append_cell_json(StringInfoData *buf, Form_pg_attribute attr, Datum v,
				 bool isnull)
{
	if (isnull)
	{
		appendStringInfoString(buf, "null");
		return;
	}
	switch (attr->atttypid)
	{
		case BOOLOID:
			appendStringInfoString(buf, DatumGetBool(v) ? "true" : "false");
			break;
		case INT2OID:
			appendStringInfo(buf, "%d", (int) DatumGetInt16(v));
			break;
		case INT4OID:
			appendStringInfo(buf, "%d", DatumGetInt32(v));
			break;
		case INT8OID:
			appendStringInfo(buf, "%lld", (long long) DatumGetInt64(v));
			break;
		case OIDOID:
			appendStringInfo(buf, "%u", DatumGetObjectId(v));
			break;
		case FLOAT4OID:
		case FLOAT8OID:
			{
				char	   *out = DatumGetCString(attr->atttypid == FLOAT4OID
					? DirectFunctionCall1(float4out, v)
					: DirectFunctionCall1(float8out, v));

				if (strstr(out, "NaN") != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("lakehouse: NaN is not supported in managed-table floats yet")));
				appendStringInfoString(buf, out);
				break;
			}
		case TEXTOID:
		case VARCHAROID:
			escape_json(buf, DatumGetCString(DirectFunctionCall1(textout, v)));
			break;
		case NAMEOID:
			/* name Datums are NameData*, NOT varlena text — routing them
			 * through textout read garbage (13-review #8b) */
			escape_json(buf, DatumGetCString(DirectFunctionCall1(nameout, v)));
			break;
		case BPCHAROID:
			/* padded-blank semantics live in bpcharout */
			escape_json(buf, DatumGetCString(DirectFunctionCall1(bpcharout, v)));
			break;
		case DATEOID:
			/* PG date = days since 2000-01-01; Iceberg = days since epoch */
			appendStringInfo(buf, "{\"d\": %d}",
							 DatumGetInt32(v) +
							 (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE));
			break;
		case TIMEOID:
			/* micros since midnight — same epoch, no conversion */
			appendStringInfo(buf, "{\"t\": %lld}", (long long) DatumGetInt64(v));
			break;
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			/* PG micros since 2000-01-01 -> unix micros */
			appendStringInfo(buf, "{\"ts\": %lld}",
							 (long long) (DatumGetInt64(v) + 946684800000000LL));
			break;
		case NUMERICOID:
			appendStringInfoString(buf, "{\"dec\": ");
			escape_json(buf, DatumGetCString(DirectFunctionCall1(numeric_out, v)));
			appendStringInfoChar(buf, '}');
			break;
		case UUIDOID:
			appendStringInfoString(buf, "{\"u\": ");
			escape_json(buf, DatumGetCString(DirectFunctionCall1(uuid_out, v)));
			appendStringInfoChar(buf, '}');
			break;
		case BYTEAOID:
		{
			/* hex-encode DIRECTLY: byteaout follows the bytea_output GUC
			 * (escape vs hex), which would silently corrupt stored bytes
			 * under bytea_output=escape (13-review #8b) */
			bytea	   *b = DatumGetByteaPP(v);
			const unsigned char *raw = (const unsigned char *) VARDATA_ANY(b);
			int			blen = VARSIZE_ANY_EXHDR(b);
			int			j;

			appendStringInfoString(buf, "{\"b\": \"\\\\x");
			for (j = 0; j < blen; j++)
				appendStringInfo(buf, "%02x", raw[j]);
			appendStringInfoString(buf, "\"}");
			break;
		}
		default:
			/* build_schema_json rejected it before we ever got a row */
			elog(ERROR, "lakehouse: unexpected column type in managed insert");
	}
}

/* ---------------- WITH-options (managed CREATE, slice 28) ---------------- */

/*
 * rd_options layout (GPT-6 review finding #2): the bytea MUST carry a
 * valid StdRdOptions prefix — autovacuum casts extractRelOptions()
 * results to StdRdOptions* and copies sizeof(StdRdOptions), so a bare
 * NUL-packed string layout gets read past. Our identity rides AFTER the
 * standard struct as a NUL-packed blob (see LAKEHOUSE_RLOPT_BLOB), and
 * standard keys (fillfactor, autovacuum, ...) parse via
 * default_reloptions like on any heap table.
 */
typedef struct LakehouseRelOptions
{
	int32		vl_len_;		/* varlena header */
	StdRdOptions std;			/* standard prefix core reads safely */
	int32		blob_len;		/* bytes of identity blob following */
	char		blob[FLEXIBLE_ARRAY_MEMBER]; /* "table\0endpoint\0warehouse\0s3_endpoint\0" */
}			LakehouseRelOptions;

static const char *lakehouse_relopt_keys[] = {
	"table", "endpoint", "warehouse", "s3_endpoint",
};
#define LAKEHOUSE_NUM_RELOPTS 4

/* credentials (token, s3_access_key, s3_secret_key) are deliberately NOT
 * table options: they live in the attach row / secure settings (doc 09)
 * and must never enter pg_class.reloptions, which pg_dump exposes. */

/*
 * [core-allowlist consumer] parse the raw WITH (...) list: our keys go
 * into the identity blob, everything else validates via
 * default_reloptions (fillfactor and friends keep working), unknown
 * keys error loudly.
 */
static bytea *
lakehouse_relation_reloptions(Datum reloptions, bool validate)
{
	char	   *values[LAKEHOUSE_NUM_RELOPTS] = {0};
	bytea	   *std;
	StringInfoData blob;
	int32		std_len;
	 LakehouseRelOptions *out;
	int			i;

	if (DatumGetPointer(reloptions) == NULL)
		return NULL;

	{
		ArrayType  *array = DatumGetArrayTypeP(reloptions);
		Datum	   *elems;
		int			nelems;

		deconstruct_array(array, TEXTOID, -1, false, TYPALIGN_INT,
						  &elems, NULL, &nelems);
		for (i = 0; i < nelems; i++)
		{
			char	   *raw = TextDatumGetCString(elems[i]);
			char	   *eq = strchr(raw, '=');
			char	   *key = raw;
			char	   *val = "";
			bool		known = false;

			if (eq != NULL)
			{
				*eq = '\0';
				val = eq + 1;
			}
			for (int k = 0; k < LAKEHOUSE_NUM_RELOPTS; k++)
			{
				if (strcmp(key, lakehouse_relopt_keys[k]) == 0)
				{
					values[k] = val;
					known = true;
					break;
				}
			}
			if (!known)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("lakehouse: unrecognized option \"%s\"", key),
						 errhint("Known options: table, endpoint, warehouse, "
								 "s3_endpoint (credentials come from the attach "
								 "row or secure settings, never table options).")));
		}
	}

	if (values[0] == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("lakehouse: the \"table\" option is required"),
				 errhint("WITH (table = 'namespace.table_name', endpoint = '...')")));

	/* standard keys (with ours stripped) -> StdRdOptions */
	{
		ArrayType  *array = DatumGetArrayTypeP(reloptions);
		Datum	   *elems;
		int			nelems;
		Datum	   *keep;
		int			nkeep = 0;

		deconstruct_array(array, TEXTOID, -1, false, TYPALIGN_INT,
						  &elems, NULL, &nelems);
		keep = (Datum *) palloc(nelems * sizeof(Datum));
		for (i = 0; i < nelems; i++)
		{
			char	   *raw = TextDatumGetCString(elems[i]);
			char	   *eq = strchr(raw, '=');
			bool		ours = false;

			if (eq != NULL)
				*eq = '\0';
			for (int k = 0; k < LAKEHOUSE_NUM_RELOPTS; k++)
				if (strcmp(raw, lakehouse_relopt_keys[k]) == 0)
					ours = true;
			if (!ours)
				keep[nkeep++] = elems[i];
		}
		if (nkeep > 0)
		{
			array = construct_array_builtin(keep, nkeep, TEXTOID);
			std = default_reloptions(PointerGetDatum(array), validate,
									 RELOPT_KIND_HEAP);
		}
		else
			std = NULL;
	}

	/* identity blob: table, endpoint, warehouse, s3_endpoint (NUL-packed) */
	initStringInfo(&blob);
	for (i = 0; i < LAKEHOUSE_NUM_RELOPTS; i++)
	{
		if (values[i] != NULL)
			appendStringInfoString(&blob, values[i]);
		appendStringInfoChar(&blob, '\0');
	}

	std_len = std != NULL ? VARSIZE(std) : offsetof(StdRdOptions, autovacuum) + sizeof(AutoVacOpts);
	out = (LakehouseRelOptions *)
		palloc0(VARHDRSZ + offsetof(LakehouseRelOptions, blob) + blob.len);
	SET_VARSIZE(out, VARHDRSZ + offsetof(LakehouseRelOptions, blob) + blob.len);
	if (std != NULL)
		memcpy(&out->std, VARDATA(std), Min(std_len, sizeof(StdRdOptions)));
	out->blob_len = blob.len;
	memcpy(out->blob, blob.data, blob.len);
	return (bytea *) out;
}

/*
 * Decode rd_options into opts (identity + endpoint fields). Returns
 * false when the relation carries no WITH-options. Credentials are NOT
 * here by design — the caller merges them from the attach row.
 */
static bool
decode_lakehouse_reloptions(Relation rel, LakehouseOptions *opts)
{
	bytea	   *raw = rel->rd_options;
	LakehouseRelOptions *ro;
	char	   *p;
	int			i;

	if (raw == NULL)
		return false;

	ro = (LakehouseRelOptions *) raw;
	if (VARSIZE(raw) < offsetof(LakehouseRelOptions, blob) + ro->blob_len)
		return false;			/* foreign/legacy layout: ignore safely */

	memset(opts, 0, sizeof(*opts));
	p = ro->blob;
	for (i = 0; i < LAKEHOUSE_NUM_RELOPTS; i++)
	{
		char	  **field = NULL;
		char	   *start = p;

		while (*p != '\0')
			p++;
		switch (i)
		{
			case 0:
				field = &opts->table;
				break;
			case 1:
				field = &opts->endpoint;
				break;
			case 2:
				field = &opts->warehouse;
				break;
			case 3:
				field = &opts->s3_endpoint;
				break;
		}
		if (p > start)
			*field = pnstrdup(start, p - start);
		p++;					/* skip the NUL */
	}

	/* split identity into namespace.table (doc 03 §3) */
	if (opts->table != NULL)
	{
		const char *last_dot = strrchr(opts->table, '.');

		if (last_dot != NULL)
		{
			char	   *full = opts->table;

			opts->namespace = pnstrdup(full, last_dot - full);
			opts->table = pstrdup(last_dot + 1);
		}
		else
			opts->namespace = pstrdup("");
	}
	return opts->table != NULL;
}

static LakehouseWriteEntry *
get_write_entry(Relation rel)
{
	LakehouseWriteEntry *entry;
	bool		found;
	Oid			relid = RelationGetRelid(rel);

	if (write_entries == NULL)
	{
		HASHCTL		ctl;

		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(LakehouseWriteEntry);
		write_entries = hash_create("lakehouse managed write state", 8, &ctl,
									HASH_ELEM | HASH_BLOBS);
	}
	entry = (LakehouseWriteEntry *) hash_search(write_entries, &relid,
												HASH_ENTER, &found);
	if (!found)
	{
		entry->ws = NULL;
		entry->mcxt = AllocSetContextCreate(TopTransactionContext,
											"lakehouse managed write",
											ALLOCSET_DEFAULT_SIZES);
		entry->staged_values = NULL;
		entry->staged_isnull = NULL;
		entry->staged_subids = NULL;
		entry->staged_rows = 0;
		entry->natts = 0;
		entry->tupdesc = NULL;
		entry->tomb_files = NULL;
		entry->n_tomb_files = 0;
		entry->tomb_cap = 0;
		entry->in_funcs = NULL;
		entry->typioparams = NULL;
		entry->have_in_funcs = false;
		memset(&entry->opts, 0, sizeof(entry->opts));
		/* WITH-options identity wins; attach row still supplies the
		 * credentials (they never live in reloptions — doc 09) */
		entry->have_identity = decode_lakehouse_reloptions(rel, &entry->opts);
		if (!ExtractManagedIdentity(relid, &entry->opts))
			entry->have_identity = entry->have_identity;
		else if (!entry->have_identity)
			entry->have_identity = true;
		if (!entry->have_identity)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("managed table %s has no lakehouse identity",
							RelationGetRelationName(rel)),
					 errdetail("Managed DDL wiring arrives with the next slice; "
							   "for now attach one explicitly."),
					 errhint("SELECT lakehouse.attach_table('%s'::regclass, "
							 "'namespace.table_name', endpoint => '...');",
							 RelationGetRelationName(rel))));
		/*
		 * EVERYTHING the PRE-COMMIT callback touches must live in the
		 * txn-scoped mcxt — the executor's per-query context (where this
		 * code first runs, inside a statement) is RESET at ExecutorEnd.
		 * tupdesc/opts allocated in the calling context were freed memory
		 * by the time PRE-COMMIT read them (intermittent garbage attribute
		 * names -> garbage palloc sizes / crashes, caught by valgrind as
		 * reads of an ExecutorEnd-freed block).
		 */
		{
			MemoryContext old = MemoryContextSwitchTo(entry->mcxt);

			entry->tupdesc = CreateTupleDescCopyConstr(RelationGetDescr(rel));
			entry->natts = entry->tupdesc->natts;
			/* re-copy the identity strings into the txn mcxt (decode
			 * helpers pstrdup into the caller's context) */
			if (entry->opts.endpoint != NULL)
				entry->opts.endpoint = pstrdup(entry->opts.endpoint);
			if (entry->opts.warehouse != NULL)
				entry->opts.warehouse = pstrdup(entry->opts.warehouse);
			if (entry->opts.token != NULL)
				entry->opts.token = pstrdup(entry->opts.token);
			if (entry->opts.namespace != NULL)
				entry->opts.namespace = pstrdup(entry->opts.namespace);
			if (entry->opts.table != NULL)
				entry->opts.table = pstrdup(entry->opts.table);
			if (entry->opts.s3_endpoint != NULL)
				entry->opts.s3_endpoint = pstrdup(entry->opts.s3_endpoint);
			if (entry->opts.s3_access_key != NULL)
				entry->opts.s3_access_key = pstrdup(entry->opts.s3_access_key);
			if (entry->opts.s3_secret_key != NULL)
				entry->opts.s3_secret_key = pstrdup(entry->opts.s3_secret_key);
			MemoryContextSwitchTo(old);
		}
	}
	return entry;
}

/* ---------------- COW tombstone plumbing (slice 28) ---------------- */

/* Find (or lazily create) the tombstone file record for `path`. */
static LakehouseTombFile *
tomb_file_get(LakehouseWriteEntry *entry, const char *path, bool create)
{
	MemoryContext old = MemoryContextSwitchTo(entry->mcxt);
	LakehouseTombFile *tf = NULL;
	int			i;

	for (i = 0; i < entry->n_tomb_files; i++)
	{
		if (strcmp(entry->tomb_files[i]->path, path) == 0)
		{
			tf = entry->tomb_files[i];
			break;
		}
	}
	if (tf == NULL && create)
	{
		if (entry->tomb_cap == 0)
		{
			entry->tomb_cap = 8;
			entry->tomb_files = (LakehouseTombFile **)
				palloc(entry->tomb_cap * sizeof(LakehouseTombFile *));
		}
		else if (entry->n_tomb_files == entry->tomb_cap)
		{
			entry->tomb_cap *= 2;
			entry->tomb_files = (LakehouseTombFile **)
				repalloc(entry->tomb_files,
						 entry->tomb_cap * sizeof(LakehouseTombFile *));
		}
		tf = (LakehouseTombFile *) palloc(sizeof(LakehouseTombFile));
		tf->path = pstrdup(path);
		tf->rows = NULL;
		tf->row_subids = NULL;
		tf->nrows = 0;
		tf->rows_cap = 0;
		tf->record_count = 0;
		tf->file_size_in_bytes = 0;
		entry->tomb_files[entry->n_tomb_files++] = tf;
	}
	MemoryContextSwitchTo(old);
	return tf;
}

/* Record a tombstoned row (sorted insert; duplicates are idempotent). */
static void
tomb_file_add_row(LakehouseWriteEntry *entry, LakehouseTombFile *tf,
				  uint64 rowidx)
{
	MemoryContext old = MemoryContextSwitchTo(entry->mcxt);
	int			lo = 0;
	int			hi = tf->nrows;

	while (lo < hi)
	{
		int			mid = (lo + hi) / 2;

		if (tf->rows[mid] < rowidx)
			lo = mid + 1;
		else
			hi = mid;
	}
	if (lo < tf->nrows && tf->rows[lo] == rowidx)
	{
		MemoryContextSwitchTo(old);
		return;
	}
	if (tf->nrows == tf->rows_cap)
	{
		tf->rows_cap = tf->rows_cap == 0 ? 16 : tf->rows_cap * 2;
		tf->rows = tf->rows == NULL
			? (uint64 *) palloc(tf->rows_cap * sizeof(uint64))
			: (uint64 *) repalloc(tf->rows, tf->rows_cap * sizeof(uint64));
		tf->row_subids = tf->row_subids == NULL
			? (SubTransactionId *) palloc(tf->rows_cap * sizeof(SubTransactionId))
			: (SubTransactionId *) repalloc(tf->row_subids,
											tf->rows_cap * sizeof(SubTransactionId));
	}
	memmove(&tf->rows[lo + 1], &tf->rows[lo],
			(tf->nrows - lo) * sizeof(uint64));
	memmove(&tf->row_subids[lo + 1], &tf->row_subids[lo],
			(tf->nrows - lo) * sizeof(SubTransactionId));
	tf->rows[lo] = rowidx;
	tf->row_subids[lo] = GetCurrentSubTransactionId();
	tf->nrows++;
	MemoryContextSwitchTo(old);
}

static bool
tomb_file_has_row(const LakehouseTombFile *tf, uint64 rowidx)
{
	int			lo = 0;
	int			hi = tf->nrows;

	while (lo < hi)
	{
		int			mid = (lo + hi) / 2;

		if (tf->rows[mid] < rowidx)
			lo = mid + 1;
		else
			hi = mid;
	}
	return lo < tf->nrows && tf->rows[lo] == rowidx;
}

/* Record the file identity carried into the DELETED manifest entry. */
static void
tomb_file_note_meta(LakehouseTombFile *tf, int64 record_count,
					int64 file_size_in_bytes)
{
	tf->record_count = record_count;
	tf->file_size_in_bytes = file_size_in_bytes;
}

/* Decode a tid produced by the managed scan into (task, rowidx). */
static void
decode_lakehouse_tid(ItemPointer tid, uint64 *task_idx, uint64 *rowidx)
{
	BlockNumber blk = ItemPointerGetBlockNumber(tid);
	OffsetNumber off = ItemPointerGetOffsetNumber(tid);

	*task_idx = (uint64) blk / LAKEHOUSE_TID_FANOUT;
	*rowidx = ((uint64) (blk % LAKEHOUSE_TID_FANOUT) << 11) | (uint64) (off - 1);
}

/* The live managed scan for `relid`, or NULL (error reported by caller). */
static LakehouseTableScanDescData *
lookup_scan_for_dml(Oid relid)
{
	if (scan_registry == NULL)
		return NULL;
	{
		bool		found;
		LakehouseScanRegistryEntry *re = (LakehouseScanRegistryEntry *)
			hash_search(scan_registry, &relid, HASH_FIND, &found);

		return found ? re->scan : NULL;
	}
}

/* Register/unregister the target scan (scan_begin/scan_end). */
static void
register_scan_for_dml(Oid relid, LakehouseTableScanDescData *scan)
{
	if (scan == NULL)
		return;
	if (scan_registry == NULL)
	{
		HASHCTL		ctl;

		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(LakehouseScanRegistryEntry);
		scan_registry = hash_create("lakehouse managed scan registry", 8,
									&ctl, HASH_ELEM | HASH_BLOBS);
	}
	{
		bool		found;
		LakehouseScanRegistryEntry *re = (LakehouseScanRegistryEntry *)
			hash_search(scan_registry, &relid, HASH_ENTER, &found);

		re->scan = scan;
		re->subid = GetCurrentSubTransactionId();
	}
}

static void
unregister_scan_for_dml(Oid relid, LakehouseTableScanDescData *scan)
{
	if (scan_registry == NULL)
		return;
	{
		bool		found;
		LakehouseScanRegistryEntry *re = (LakehouseScanRegistryEntry *)
			hash_search(scan_registry, &relid, HASH_FIND, &found);

		if (found && re->scan == scan)
			hash_search(scan_registry, &relid, HASH_REMOVE, NULL);
	}
}

/* Drop registry entries created above the aborted subtransaction —
 * their scan descs die with the aborted executor context. */
static void
prune_scan_registry_above(SubTransactionId subid)
{
	if (scan_registry == NULL)
		return;
	{
		HASH_SEQ_STATUS seq;
		LakehouseScanRegistryEntry *re;

		hash_seq_init(&seq, scan_registry);
		while ((re = hash_seq_search(&seq)) != NULL)
		{
			if (re->subid >= subid)
			{
				Oid			relid = re->relid;

				hash_seq_term(&seq);
				hash_search(scan_registry, &relid, HASH_REMOVE, NULL);
				hash_seq_init(&seq, scan_registry);
			}
		}
	}
}

/* ---------------- scan state ---------------- */

typedef struct LakehouseTableScanDescData
{
	TableScanDescData rs_base;
	LakehouseOptions opts;
	ScanSession *session;
	RowsHandle *rows;
	uint64		current_task;
	uint64		task_count;
	char	   *projection_csv;
	int			natts;
	FmgrInfo   *in_funcs;
	Oid		   *typioparams;
	int		   *slot_index;
	/* own-writes overlay: staged rows of THIS txn (entry == NULL when the
	 * relation has no open write state) */
	LakehouseWriteEntry *entry;
	uint64		staged_pos;
	/* COW tids (slice 28): 0-based row index within the current task, and
	 * the task's file path (session-owned CString pointer, stable) for
	 * tombstone filtering. M3-R1: the identity is the file-absolute row
	 * index r from the batch live list (equal to the live-row ordinal for
	 * managed tables, which carry no delete files). */
	uint64		row_in_task;
	const char *cur_task_path;
	/* M3-R1 typed batch cursor (no per-row renderer on this path) */
	PgDatalakeRowBatch batch;
	bool		batch_valid;
	uint64		batch_cursor;
	LakehouseBatchMap *maps;	/* per visible attribute */
	/* staged-delta read side (P3 core, D1-rev): committed unpublished
	 * segments of this table, loaded once per scan via SPI */
	DeltaRowsHandle **delta_rows;
	int			n_delta;
	int			delta_pos;
	bool		delta_loaded;
}			LakehouseTableScanDescData;
typedef LakehouseTableScanDescData *LakehouseTableScanDesc;

static void load_delta_segments(LakehouseTableScanDesc scan, TupleDesc tupdesc);

static void
lakehouse_scan_common_setup(LakehouseTableScanDesc scan, Relation rel,
							const char *rte_selector)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	StringInfoData proj;
	bool		first_col = true;
	int			i;

	scan->natts = tupdesc->natts;
	scan->in_funcs = (FmgrInfo *) palloc0(tupdesc->natts * sizeof(FmgrInfo));
	scan->typioparams = (Oid *) palloc0(tupdesc->natts * sizeof(Oid));
	scan->slot_index = (int *) palloc0(tupdesc->natts * sizeof(int));
	scan->maps = (LakehouseBatchMap *) palloc0(tupdesc->natts * sizeof(LakehouseBatchMap));
	scan->batch_valid = false;

	/* projection: visible attribute names, in attribute order — a counted
	 * JSON ARRAY, not comma-separated: column names may contain commas
	 * (13-review #8b) */
	initStringInfo(&proj);
	appendStringInfoChar(&proj, '[');
	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		if (attr->attisdropped)
		{
			scan->slot_index[i] = -1;
			continue;
		}
		if (!first_col)
			appendStringInfoChar(&proj, ',');
		escape_json(&proj, NameStr(attr->attname)); /* emits its own quotes */
		first_col = false;
		getTypeInputInfo(attr->atttypid,
						 &scan->in_funcs[i].fn_oid,
						 &scan->typioparams[i]);
		fmgr_info(scan->in_funcs[i].fn_oid, &scan->in_funcs[i]);
	}
	appendStringInfoChar(&proj, ']');
	{
		int			proj_col = 0;

		for (i = 0; i < tupdesc->natts; i++)
			if (scan->slot_index[i] >= 0)
				scan->slot_index[i] = proj_col++;
	}
	scan->projection_csv = proj.data;

	/* staged mode (P3, D2): the scan source is the delta table — no
	 * remote session; zero tasks means the file loop never runs and the
	 * delta serve path (below) is the whole scan */
	if (lakehouse_write_mode == LAKEHOUSE_WRITE_MODE_STAGED)
	{
		scan->session = NULL;
		scan->task_count = 0;
		return;
	}
	scan->session = lakehouse_OpenSession(&scan->opts, rte_selector);
	scan->task_count = pg_datalake_scan_task_count(scan->session);
	scan->rows = NULL;
	scan->current_task = 0;
}

static const TupleTableSlotOps *
lakehouse_slot_callbacks(Relation rel)
{
	return &TTSOpsVirtual;
}

static TableScanDesc
lakehouse_scan_begin(Relation rel, Snapshot snapshot,
					 int nkeys, ScanKey key,
					 ParallelTableScanDesc pscan, uint32 flags)
{
	LakehouseTableScanDesc scan;

	if (pscan != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("lakehouse: parallel managed scans are not supported yet "
						"(planned for M3)")));
	if (nkeys > 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("lakehouse: scan keys are not supported on managed tables yet "
						"(quals are pushed as predicates in slice 15 parity, M2)")));

	scan = (LakehouseTableScanDesc) palloc0(sizeof(LakehouseTableScanDescData));

	RelationIncrementReferenceCount(rel);
	scan->rs_base.rs_rd = rel;
	if (flags & SO_TEMP_SNAPSHOT)
		scan->rs_base.rs_snapshot = RegisterSnapshot(snapshot);
	else
		scan->rs_base.rs_snapshot = snapshot;
	scan->rs_base.rs_nkeys = nkeys;
	scan->rs_base.rs_key = key;
	scan->rs_base.rs_flags = flags;
	scan->rs_base.rs_parallel = pscan;
	if (flags & SO_SCAN_INSTRUMENT)
		scan->rs_base.rs_instrument = palloc0(sizeof(TableScanInstrumentation));
	else
		scan->rs_base.rs_instrument = NULL;

	{
		bool		with_identity = decode_lakehouse_reloptions(rel, &scan->opts);
		bool		attach_identity =
			ExtractManagedIdentity(RelationGetRelid(rel), &scan->opts);

		if (!with_identity && !attach_identity)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("managed table %s has no lakehouse identity",
						RelationGetRelationName(rel)),
				 errdetail("Managed DDL wiring arrives with M2 slice 27; "
						   "for now attach one explicitly."),
				 errhint("SELECT lakehouse.attach_table('%s'::regclass, "
						 "'namespace.table_name', endpoint => '...');",
						 RelationGetRelationName(rel))));
	}

	lakehouse_scan_common_setup(scan, rel, NULL);
	scan->row_in_task = 0;
	scan->cur_task_path = NULL;
	if (write_entries != NULL)
	{
		bool		found;

		scan->entry = (LakehouseWriteEntry *)
			hash_search(write_entries, &rel->rd_id, HASH_FIND, &found);
		if (!found)
			scan->entry = NULL;
	}
	register_scan_for_dml(RelationGetRelid(rel), scan);
	return (TableScanDesc) scan;
}

static void
lakehouse_scan_end(TableScanDesc bscan)
{
	LakehouseTableScanDesc scan = (LakehouseTableScanDesc) bscan;

	if (scan->rows != NULL)
	{
		pg_datalake_rows_close(scan->rows);
		scan->rows = NULL;
	}
	if (scan->session != NULL)
	{
		pg_datalake_scan_free(scan->session);
		scan->session = NULL;
	}
	unregister_scan_for_dml(RelationGetRelid(scan->rs_base.rs_rd), scan);
	if (scan->rs_base.rs_flags & SO_TEMP_SNAPSHOT)
		UnregisterSnapshot(scan->rs_base.rs_snapshot);
	RelationDecrementReferenceCount(scan->rs_base.rs_rd);
}

static void
lakehouse_scan_rescan(TableScanDesc bscan, ScanKey key, bool set_params,
					  bool allow_strat, bool allow_sync,
					  bool allow_pagemode)
{
	LakehouseTableScanDesc scan = (LakehouseTableScanDesc) bscan;

	/* rewind-only, mirroring the FDW fix from the M1 gate (retro #2):
	 * the session stays pinned to the snapshot from scan_begin */
	if (scan->rows != NULL)
	{
		pg_datalake_rows_close(scan->rows);
		scan->rows = NULL;
	}
}

static bool
lakehouse_scan_getnextslot(TableScanDesc bscan, ScanDirection direction,
						   TupleTableSlot *slot)
{
	LakehouseTableScanDesc scan = (LakehouseTableScanDesc) bscan;
	Relation	rel = scan->rs_base.rs_rd;
	TupleDesc	tupdesc = RelationGetDescr(rel);
	int			i;

	/* ScanDirection ignored: files iterate in stored order only */

	for (;;)
	{
		if (scan->rows == NULL)
		{
			char		errbuf[1024];
			const char *file;

			/* own-writes overlay (doc 07 §2): after the snapshot's files
			 * drain, yield the rows this transaction staged but has not
			 * committed yet. This runs on EVERY exhausted call (not once):
			 * a tombstone skip can drain the last task mid-loop, and the
			 * staged rows must still be served. */
			if (scan->current_task >= scan->task_count)
			{
				LakehouseWriteEntry *entry = scan->entry;

				while (entry != NULL &&
					   scan->staged_pos < entry->staged_rows)
				{
					int			k;
					Datum	   *row = entry->staged_values +
						(size_t) scan->staged_pos * entry->natts;
					bool	   *nulls = entry->staged_isnull +
						(size_t) scan->staged_pos * entry->natts;

					scan->staged_pos++;
					ExecClearTuple(slot);
					ItemPointerSetInvalid(&slot->tts_tid);
					for (k = 0; k < tupdesc->natts; k++)
					{
						slot->tts_values[k] = row[k];
						slot->tts_isnull[k] = nulls[k];
					}
					ExecStoreVirtualTuple(slot);
					return true;
				}

				/* staged mode: committed unpublished delta segments
				 * next (MVCC visibility via the SPI snapshot) */
				if (lakehouse_write_mode == LAKEHOUSE_WRITE_MODE_STAGED)
				{
					if (!scan->delta_loaded)
						load_delta_segments(scan, tupdesc);
					char		delta_err[1024];

					while (scan->delta_pos < scan->n_delta)
					{
						DeltaRowsHandle *h = scan->delta_rows[scan->delta_pos];

						if (pg_datalake_delta_rows_next(h, delta_err,
														sizeof(delta_err)) == 0)
						{
							int			k2;

							ExecClearTuple(slot);
							ItemPointerSetInvalid(&slot->tts_tid);
							for (k2 = 0; k2 < tupdesc->natts; k2++)
							{
								int			pcol = scan->slot_index[k2];

								if (pcol < 0 ||
									pg_datalake_delta_rows_col_null(h, pcol))
								{
									slot->tts_values[k2] = PointerGetDatum(NULL);
									slot->tts_isnull[k2] = true;
									continue;
								}
								slot->tts_values[k2] =
									InputFunctionCall(&scan->in_funcs[k2],
										pg_datalake_delta_rows_col_text(h, pcol),
										scan->typioparams[k2],
										TupleDescAttr(tupdesc, k2)->atttypmod);
								slot->tts_isnull[k2] = false;
							}
							ExecStoreVirtualTuple(slot);
							return true;
						}
						scan->delta_pos++;
					}
				}
				return false;
			}

			errbuf[0] = '\0';
			file = pg_datalake_scan_task_file(scan->session,
											  scan->current_task);
			scan->rows = pg_datalake_rows_open(scan->session,
											   scan->current_task,
											   scan->projection_csv,
											   errbuf,
											   sizeof(errbuf));
			if (scan->rows == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_IO_ERROR),
						 errmsg("lakehouse: could not open task %llu (%s): %s",
								(unsigned long long) scan->current_task,
								file, errbuf)));
			scan->row_in_task = 0;
			scan->cur_task_path = file;
			scan->batch_valid = false;
		}

		/*
		 * M3-R1: batch cursor — one pg_datalake_rows_batch_next call per
		 * arrow batch; Datums are built directly (batch_fill.c). The
		 * explicit ROW/EOF/ERROR tri-state carries over: EOF advances the
		 * task, ERROR aborts the scan (13-review #8 — a caught panic is
		 * never mistaken for EOF).
		 */
		{
			char		next_err[1024];
			int			nst;

			if (!scan->batch_valid || scan->batch_cursor >= scan->batch.live_count)
			{
				next_err[0] = '\0';
				nst = pg_datalake_rows_batch_next(scan->rows, &scan->batch,
												  next_err, sizeof(next_err));
				if (nst == 2)
					ereport(ERROR,
							(errcode(ERRCODE_DATA_EXCEPTION),
							 errmsg("lakehouse: row read failed in task %llu: %s",
									(unsigned long long) scan->current_task,
									next_err[0] != '\0' ? next_err : "unknown error")));
				if (nst == 1)
				{
					pg_datalake_rows_close(scan->rows);
					scan->rows = NULL;
					scan->batch_valid = false;
					scan->current_task++;
					continue;
				}
				scan->batch_valid = true;
				scan->batch_cursor = 0;
				for (i = 0; i < tupdesc->natts; i++)
				{
					int			pcol = scan->slot_index[i];

					if (pcol < 0)
						continue;
					lakehouse_batch_map_column(TupleDescAttr(tupdesc, i),
											   &scan->batch.cols[pcol],
											   scan->batch.rows,
											   &scan->in_funcs[i],
											   scan->typioparams[i],
											   &scan->maps[i]);
				}
			}

			{
				/* lr: batch-LOCAL index into the typed buffers; r:
				 * file-ABSOLUTE row index — the tombstone/TID identity
				 * (slice 28). Conflating the two silently retargeted
				 * tombstones at the wrong rows in every batch after the
				 * first (caught by the M3-R1 live DML verification). */
				uint32		lr = scan->batch.live_idx[scan->batch_cursor];
				uint64		r = scan->batch.start_abs + lr;

				/* COW read-your-deletes: rows tombstoned earlier in THIS
				 * transaction are filtered here, exactly like the rewrite
				 * pass filters them at PRE-COMMIT. r is the file-absolute
				 * row index (the tombstone/TID identity, slice 28). */
				if (scan->entry != NULL && scan->entry->n_tomb_files > 0 &&
					scan->cur_task_path != NULL)
				{
					LakehouseTombFile *tf = tomb_file_get(scan->entry,
														  scan->cur_task_path,
														  false);

					if (tf != NULL && tomb_file_has_row(tf, r))
					{
						scan->batch_cursor++;
						continue;
					}
				}
				ExecClearTuple(slot);
				/* COW tid (slice 28): (task, rowidx) — the identity
				 * tuple_delete/tuple_update decode back into the base
				 * snapshot's file + row position */
				ItemPointerSetBlockNumber(&slot->tts_tid,
										  (BlockNumber) (scan->current_task *
												LAKEHOUSE_TID_FANOUT +
												(r >> 11)));
				ItemPointerSetOffsetNumber(&slot->tts_tid,
										   (OffsetNumber) ((r & 0x7FF) + 1));
				for (i = 0; i < tupdesc->natts; i++)
				{
					int			pcol = scan->slot_index[i];

					if (pcol < 0 || scan->batch.cols[pcol].nulls[lr])
					{
						slot->tts_values[i] = PointerGetDatum(NULL);
						slot->tts_isnull[i] = true;
						continue;
					}
					if (scan->maps[i].target != LAKE_FAST_NONE)
					{
						slot->tts_values[i] =
							lakehouse_batch_col_datum(&scan->maps[i],
													  &scan->batch.cols[pcol], lr,
													  TupleDescAttr(tupdesc, i)->attname.data);
					}
					else
					{
						char		cell_err[1024];
						const char *text;

						cell_err[0] = '\0';
						text = pg_datalake_rows_batch_cell_text(scan->rows,
																pcol, lr,
																cell_err,
																sizeof(cell_err));
						if (text == NULL)
							ereport(ERROR,
									(errcode(ERRCODE_DATA_EXCEPTION),
									 errmsg("lakehouse: row read failed in task %llu: %s",
											(unsigned long long) scan->current_task,
											cell_err[0] != '\0' ? cell_err : "unknown error")));
						slot->tts_values[i] =
							InputFunctionCall(&scan->in_funcs[i], text,
											  scan->typioparams[i],
											  TupleDescAttr(tupdesc, i)->atttypmod);
					}
					slot->tts_isnull[i] = false;
				}
				ExecStoreVirtualTuple(slot);
				scan->batch_cursor++;
				scan->row_in_task = r;
				return true;
			}
		}
	}
}

/* ---------------- writes: loud stubs until the managed-write slices -------- */

#define LAKEHOUSE_UNSUPPORTED(what, slice) \
	do { \
		ereport(ERROR, \
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
				 errmsg("lakehouse: %s is not supported on managed tables yet", \
						what), \
				 errdetail("Managed %s arrives with M2 slice " slice ".", \
						   what))); \
	} while (0)


/* Open the FFI write session lazily (first insert or first tombstone):
 * validates identity/credentials early, fail-fast per doc 03 §5. */
static void
open_write_session(LakehouseWriteEntry *entry, TupleDesc tupdesc)
{
	char		errbuf[1024];

	if (entry->ws != NULL)
		return;
	errbuf[0] = '\0';
	entry->ws = pg_datalake_write_new(entry->opts.endpoint,
									  entry->opts.warehouse
									  ? entry->opts.warehouse : "",
									  entry->opts.token ? entry->opts.token : "",
									  entry->opts.namespace,
									  entry->opts.table,
									  entry->opts.s3_endpoint,
									  entry->opts.s3_access_key,
									  entry->opts.s3_secret_key,
									  build_schema_json(tupdesc),
									  errbuf, sizeof(errbuf));
	if (entry->ws == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg("lakehouse: could not open write session: %s",
						errbuf)));
	if (!pg_datalake_write_set_target_file_size(
			entry->ws,
			(int64) lakehouse_target_file_size * 1024,
			errbuf,
			sizeof(errbuf)))
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("lakehouse: could not set target file size: %s",
						errbuf)));
}

/*
 * load_delta_segments (P3 core): one SPI pass over this table's
 * committed unpublished delta rows, opened as DeltaRowsHandles. SPI
 * inside the running executor uses the active (statement) snapshot, so
 * visibility is exactly MVCC — rows staged by OTHER backends that have
 * committed appear; our own txn's are served by the overlay (and are
 * not yet committed, hence invisible here — no duplicates).
 */
static void
load_delta_segments(LakehouseTableScanDesc scan, TupleDesc tupdesc)
{
	MemoryContext old = CurrentMemoryContext;
	Oid			relid = RelationGetRelid(scan->rs_base.rs_rd);
	char	   *schema_json = build_schema_json(tupdesc);
	int			ret;
	int			i;

	scan->delta_loaded = true;
	scan->delta_rows = NULL;
	scan->n_delta = 0;
	scan->delta_pos = 0;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	{
		StringInfoData sql;

		initStringInfo(&sql);
		appendStringInfo(&sql,
						 "SELECT payload FROM lakehouse.lakehouse_delta_segments"
						 " WHERE relid = %u AND NOT published ORDER BY seg_id",
						 (unsigned) relid);
		ret = SPI_execute(sql.data, true, 0);
	}
	if (ret == SPI_OK_SELECT)
	{
		MemoryContext sc = MemoryContextSwitchTo(old);

		scan->n_delta = SPI_processed;
		scan->delta_rows = (DeltaRowsHandle **)
			palloc0(sizeof(DeltaRowsHandle *) * (SPI_processed ? SPI_processed : 1));
		for (i = 0; i < SPI_processed; i++)
		{
			bool		isnull;
			Datum		d = SPI_getbinval(SPI_tuptable->vals[i],
										  SPI_tuptable->tupdesc, 1, &isnull);
			char	   *payload;

			if (isnull)
				continue;
			payload = TextDatumGetCString(d);
			MemoryContextSwitchTo(old);
			scan->delta_rows[scan->delta_pos] =
				pg_datalake_delta_rows_open(payload, schema_json,
											NULL, 0);
			MemoryContextSwitchTo(sc);
			if (scan->delta_rows[scan->delta_pos] != NULL)
				scan->delta_pos++;
		}
		/* every handle serves from pos 0 */
		scan->delta_pos = 0;
		MemoryContextSwitchTo(sc);
	}
	PopActiveSnapshot();
	SPI_finish();
	MemoryContextSwitchTo(old);
}

/* Own-writes overlay: copy the tuple into the txn-scoped entry. Rows are
 * NOT pushed to the Rust writer here — staging is C-side only; rows
 * reach the parquet accumulator at PRE-COMMIT, AFTER subtransaction
 * pruning. Pushing at insert time leaked savepoint-rolled-back rows into
 * the committed snapshot (13-review blocker #3). */
static void
stage_slot(LakehouseWriteEntry *entry, TupleTableSlot *slot, TupleDesc tupdesc)
{
	int			j;
	MemoryContext old = MemoryContextSwitchTo(entry->mcxt);

	if (entry->staged_rows == 0)
	{
		entry->staged_values = (Datum *)
			palloc(tupdesc->natts * sizeof(Datum) * 64);
		entry->staged_isnull = (bool *)
			palloc(tupdesc->natts * sizeof(bool) * 64);
		entry->staged_subids = (SubTransactionId *)
			palloc(sizeof(SubTransactionId) * 64);
	}
	else if (entry->staged_rows % 64 == 0)
	{
		entry->staged_values = (Datum *)
			repalloc(entry->staged_values,
					 tupdesc->natts * sizeof(Datum) *
					 (entry->staged_rows + 64));
		entry->staged_isnull = (bool *)
			repalloc(entry->staged_isnull,
					 tupdesc->natts * sizeof(bool) *
					 (entry->staged_rows + 64));
		entry->staged_subids = (SubTransactionId *)
			repalloc(entry->staged_subids,
					 sizeof(SubTransactionId) *
					 (entry->staged_rows + 64));
	}
	entry->staged_subids[entry->staged_rows] =
		GetCurrentSubTransactionId();
	for (j = 0; j < tupdesc->natts; j++)
	{
		entry->staged_values[(size_t) entry->staged_rows * tupdesc->natts + j] =
			slot->tts_isnull[j]
			? (Datum) 0
			: datumCopy(slot->tts_values[j],
						TupleDescAttr(tupdesc, j)->attbyval,
						TupleDescAttr(tupdesc, j)->attlen);
		entry->staged_isnull[(size_t) entry->staged_rows * tupdesc->natts + j] =
			slot->tts_isnull[j];
	}
	entry->staged_rows++;
	MemoryContextSwitchTo(old);
}

/*
 * COW tombstone capture (slice 28): decode the scan-produced tid into
 * (base file, rowidx), record it + the file's DELETED-entry identity.
 * The write session opens here too — a DELETE-only transaction still
 * commits an (overwrite) snapshot and must fail fast on bad credentials.
 */
static void
capture_tombstone(Relation rel, ItemPointer tid)
{
	LakehouseWriteEntry *entry = get_write_entry(rel);
	LakehouseTableScanDescData *scan = lookup_scan_for_dml(RelationGetRelid(rel));
	uint64		task_idx;
	uint64		rowidx;

	if (scan == NULL || scan->session == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("lakehouse: %s cannot resolve row identity for DELETE/UPDATE",
						RelationGetRelationName(rel)),
				 errdetail("The tid was not produced by a live managed scan of this relation "
						   "in this transaction (rows staged by an earlier INSERT in the same "
						   "transaction are not yet addressable).")));
	if (ItemPointerGetOffsetNumber(tid) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("lakehouse: cannot modify a row inserted in this transaction"),
				 errdetail("Rows staged by INSERT in the same transaction have no remote "
						   "identity yet; commit first.")));
	decode_lakehouse_tid(tid, &task_idx, &rowidx);
	if (task_idx >= pg_datalake_scan_task_count(scan->session))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("lakehouse: DELETE/UPDATE tid names task %llu, "
						"outside the pinned snapshot's %llu tasks",
						(unsigned long long) task_idx,
						(unsigned long long) pg_datalake_scan_task_count(scan->session))));
	{
		int64		nrows = pg_datalake_scan_task_rows(scan->session, task_idx);
		int64		size = pg_datalake_scan_task_size(scan->session, task_idx);
		const char *file = pg_datalake_scan_task_file(scan->session, task_idx);
		LakehouseTombFile *tf;

		if (rowidx >= (uint64) nrows)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("lakehouse: DELETE/UPDATE tid row %llu is outside "
							"task %llu (%lld rows)",
							(unsigned long long) rowidx,
							(unsigned long long) task_idx,
							(long long) nrows)));
		open_write_session(entry, entry->tupdesc);
		tf = tomb_file_get(entry, file, true);
		tomb_file_add_row(entry, tf, rowidx);
		tomb_file_note_meta(tf, nrows, size);
	}
}

/*
 * staged_capture_segment (P3 core, D1-rev): serialize this entry's
 * staged rows as ONE JSON payload (array of the write path's typed
 * row-JSON) and INSERT it into lakehouse.lakehouse_delta_segments via
 * SPI — durability from the transaction's own WAL record. The tombstone
 * rewrite/remote paths are untouched (D2: staged mode is INSERT-only).
 */
static void
staged_capture_segment(LakehouseWriteEntry *entry)
{
	StringInfoData payload;
	StringInfoData row;
	int			i;
	int			k;
	bool		first_row = true;
	Oid			save_uid;
	int			ret;
	TupleDesc	tupdesc = entry->tupdesc;

	initStringInfo(&payload);
	appendStringInfoChar(&payload, '[');
	for (i = 0; i < entry->staged_rows; i++)
	{
		bool		first_col = true;

		if (!first_row)
			appendStringInfoChar(&payload, ',');
		initStringInfo(&row);
		appendStringInfoChar(&row, '[');
		for (k = 0; k < tupdesc->natts; k++)
		{
			/* dropped attributes are omitted — cell count matches the
			 * schema column count (13-review #8b) */
			if (TupleDescAttr(tupdesc, k)->attisdropped)
				continue;
			if (!first_col)
				appendStringInfoChar(&row, ',');
			append_cell_json(&row,
							 TupleDescAttr(tupdesc, k),
							 entry->staged_values[(size_t) i * entry->natts + k],
							 entry->staged_isnull[(size_t) i * entry->natts + k]);
			first_col = false;
		}
		appendStringInfoChar(&row, ']');
		appendBinaryStringInfo(&payload, row.data, row.len);
		pfree(row.data);
		first_row = false;
	}
	appendStringInfoChar(&payload, ']');

	/*
	 * INSERT inside the committing transaction: WAL-fsync at commit
	 * makes the delta durable BEFORE the COMMIT ack (D1-rev's point).
	 * Parameterized payload (no literal injection surface).
	 */
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	{
		Oid			argtypes[1] = {TEXTOID};
		Datum		values[1];
		char		nulls[2] = {' ', '\0'};

		values[0] = CStringGetTextDatum(payload.data);
		ret = SPI_execute_with_args(psprintf("INSERT INTO"
											 " lakehouse.lakehouse_delta_segments"
											 " (relid, payload) VALUES (%u, $1::bytea)",
											 (unsigned) entry->relid),
									1, argtypes, values, nulls, false, 0);
	}
	PopActiveSnapshot();
	SPI_finish();
	if (ret != SPI_OK_INSERT)
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("lakehouse: staged delta capture failed (relid %u)",
						(unsigned) entry->relid)));
	pfree(payload.data);
}

static void
lakehouse_tuple_insert(Relation rel, TupleTableSlot *slot, CommandId cid,
					   uint32 options, BulkInsertStateData *bistate)
{
	LakehouseWriteEntry *entry = get_write_entry(rel);

	slot_getallattrs(slot);
	/* staged mode (spec 15 D2): durability is the delta table — no
	 * remote session is opened, ever, on the insert path */
	if (lakehouse_write_mode != LAKEHOUSE_WRITE_MODE_STAGED)
		open_write_session(entry, RelationGetDescr(rel));
	stage_slot(entry, slot, RelationGetDescr(rel));
	elog(LOG, "lakehouse staged row at subid %u",
		 GetCurrentSubTransactionId());
}

static void
lakehouse_tuple_insert_speculative(Relation rel, TupleTableSlot *slot,
								   CommandId cid, uint32 options,
								   BulkInsertStateData *bistate,
								   uint32 specToken)
{
	/* speculative insert only comes from INSERT ... ON CONFLICT, which
	 * needs upsert semantics — its slice */
	LAKEHOUSE_UNSUPPORTED("INSERT ... ON CONFLICT", "29");
}

static void
lakehouse_tuple_complete_speculative(Relation rel, TupleTableSlot *slot,
									 uint32 specToken, bool succeeded)
{
	/* only called after tuple_insert_speculative, which errors first */
	pg_unreachable();
}

static void
lakehouse_multi_insert(Relation rel, TupleTableSlot **slots, int nslots,
					   CommandId cid, uint32 options,
					   BulkInsertStateData *bistate)
{
	LAKEHOUSE_UNSUPPORTED("INSERT", "27");
}

/*
 * DELETE (COW, slice 28): tombstone (file, rowidx). The rewrite + commit
 * happen at PRE-COMMIT; the row vanishes from THIS transaction's reads
 * immediately via the tombstone filter in scan_getnextslot. TM_Ok always:
 * Iceberg has no concurrent-tuple visibility to conflict with (snapshot
 * isolation is enforced at COMMIT by AssertRefSnapshotId — a conflicting
 * concurrent writer aborts the transaction, not the row).
 */
static TM_Result
lakehouse_tuple_delete(Relation rel, ItemPointer tid, CommandId cid,
					   uint32 options, Snapshot snapshot, Snapshot crosscheck,
					   bool wait, TM_FailureData *tmfd)
{
		capture_tombstone(rel, tid);
	return TM_Ok;
}

/*
 * UPDATE (COW, slice 28) = tombstone the old row + stage the new row.
 * One PRE-COMMIT: affected files rewritten without the old rows, the
 * staged new rows committed as new files, one overwrite snapshot.
 */
static TM_Result
lakehouse_tuple_update(Relation rel, ItemPointer otid, TupleTableSlot *slot,
					   CommandId cid, uint32 options, Snapshot snapshot,
					   Snapshot crosscheck, bool wait, TM_FailureData *tmfd,
					   LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes)
{
	LakehouseWriteEntry *entry;

	slot_getallattrs(slot);
	capture_tombstone(rel, otid);
	entry = get_write_entry(rel);
	stage_slot(entry, slot, RelationGetDescr(rel));
	*lockmode = LockTupleExclusive;
	*update_indexes = TU_None;
	return TM_Ok;
}

static TM_Result
lakehouse_tuple_lock(Relation rel, ItemPointer tid, Snapshot snapshot,
					 TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
					 LockWaitPolicy wait_policy, uint8 flags,
					 TM_FailureData *tmfd)
{
	LAKEHOUSE_UNSUPPORTED("row locking (SELECT ... FOR UPDATE)", "29");
	return TM_Ok;				/* unreachable */
}

/* ---------------- DDL callbacks ---------------- */

static void
lakehouse_relation_set_new_filelocator(Relation rel,
									   const RelFileLocator *newrlocator,
									   char persistence,
									   TransactionId *freezeXid,
									   MultiXactId *minmulti)
{
	SMgrRelation srel;

	/*
	 * Managed tables keep a (hollow) local relfilelocator so relkind 'r'
	 * machinery — pg_class storage, DROP, relsize reporting — works
	 * unchanged. All real data lives on the object store; this file never
	 * gets tuples while INSERT is unsupported.
	 */
	*freezeXid = RecentXmin;
	*minmulti = GetOldestMultiXactId();
	srel = RelationCreateStorage(*newrlocator, persistence, true);
	if (persistence == RELPERSISTENCE_UNLOGGED)
	{
		smgrcreate(srel, INIT_FORKNUM, false);
		log_smgrcreate(newrlocator, INIT_FORKNUM);
	}
	smgrclose(srel);
}

static void
lakehouse_relation_nontransactional_truncate(Relation rel)
{
	LAKEHOUSE_UNSUPPORTED("TRUNCATE", "28");
}

static void
lakehouse_relation_copy_data(Relation rel, const RelFileLocator *newrlocator)
{
	LAKEHOUSE_UNSUPPORTED("TABLESPACE moves", "29");
}

static void
lakehouse_relation_copy_for_cluster(Relation OldTable, Relation NewTable,
									Relation OldIndex, bool use_sort,
									TransactionId OldestXmin,
									Snapshot snapshot,
									TransactionId *xid_cutoff,
									MultiXactId *multi_cutoff,
									double *num_tuples, double *tups_vacuumed,
									double *tups_recently_dead)
{
	LAKEHOUSE_UNSUPPORTED("CLUSTER", "29");
}

static void
lakehouse_relation_vacuum(Relation rel, const VacuumParams *params,
						  BufferAccessStrategy bstrategy)
{
	/* autovacuum skips zero-page relations; a manual VACUUM is a no-op
	 * pointer to the real maintenance path (doc 08, lakehouse.optimize M4) */
	ereport(NOTICE,
			(errmsg("lakehouse: VACUUM is not applicable to managed tables"),
			 errdetail("Physical reorganization is lakehouse.optimize's job "
					   "(M4; doc 08 §2).")));
}

/* ---------------- index callbacks ---------------- */

static void
lakehouse_index_scan_begin(IndexScanDesc scan, uint32 flags)
{
	LAKEHOUSE_UNSUPPORTED("indexes", "29");
}

static void
lakehouse_index_scan_reset(IndexScanDesc scan)
{
	LAKEHOUSE_UNSUPPORTED("indexes", "29");
}

static void
lakehouse_index_scan_end(IndexScanDesc scan)
{
	LAKEHOUSE_UNSUPPORTED("indexes", "29");
}

/* ---------------- tid / row-version callbacks ---------------- */

static bool
lakehouse_fetch_tid(Relation rel, ItemPointer tid, Snapshot snapshot,
					bool *all_dead)
{
	LAKEHOUSE_UNSUPPORTED("TID fetches", "27");
	return false;				/* unreachable */
}

/*
 * Row-version fetch (slice 28): the executor calls this on every UPDATE
 * to build the new tuple from the old one. A managed table answers it by
 * point-reading the (task, rowidx) the tid encodes — the base file as
 * pinned by this transaction's scan. No tombstone filter: the tid refers
 * to a row the scan already emitted (post-filter).
 */
static bool
lakehouse_tuple_fetch_row_version(Relation rel, ItemPointer tid,
								  Snapshot snapshot, TupleTableSlot *slot)
{
	LakehouseTableScanDescData *scan = lookup_scan_for_dml(RelationGetRelid(rel));
	uint64		task_idx;
	uint64		rowidx;
	TupleDesc	tupdesc = RelationGetDescr(rel);
	char		errbuf[1024];
	RowsHandle *rows;
	uint64		i;
	int			k;

	if (scan == NULL || scan->session == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("lakehouse: %s cannot resolve row identity for row-version fetch",
						RelationGetRelationName(rel)),
				 errdetail("The tid was not produced by a live managed scan of this relation "
						   "in this transaction.")));
	if (ItemPointerGetOffsetNumber(tid) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("lakehouse: cannot fetch a row inserted in this transaction")));
	decode_lakehouse_tid(tid, &task_idx, &rowidx);
	if (task_idx >= pg_datalake_scan_task_count(scan->session))
		return false;
	if (rowidx >= (uint64) pg_datalake_scan_task_rows(scan->session, task_idx))
		return false;

	errbuf[0] = '\0';
	rows = pg_datalake_rows_open(scan->session, task_idx,
								 scan->projection_csv,
								 errbuf, sizeof(errbuf));
	if (rows == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("lakehouse: row-version fetch could not open task %llu: %s",
						(unsigned long long) task_idx,
						errbuf[0] != '\0' ? errbuf : "unknown error")));
	for (i = 0; i <= rowidx; i++)
	{
		char		nst_err[1024];
		int			nst;

		nst_err[0] = '\0';
		nst = pg_datalake_rows_next(rows, nst_err, sizeof(nst_err));
		if (nst == 1)
		{
			pg_datalake_rows_close(rows);
			return false;		/* file shrank under us */
		}
		if (nst == 2)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("lakehouse: row-version fetch read failed: %s",
							nst_err[0] != '\0' ? nst_err : "unknown error")));
	}
	ExecClearTuple(slot);
	for (k = 0; k < tupdesc->natts; k++)
	{
		int			pcol = scan->slot_index[k];

		if (pcol < 0 || pg_datalake_rows_col_null(rows, pcol))
		{
			slot->tts_values[k] = PointerGetDatum(NULL);
			slot->tts_isnull[k] = true;
			continue;
		}
		slot->tts_values[k] = InputFunctionCall(&scan->in_funcs[k],
				  pg_datalake_rows_col_text(rows, pcol),
				  scan->typioparams[k],
				  TupleDescAttr(tupdesc, k)->atttypmod);
		slot->tts_isnull[k] = false;
	}
	ExecStoreVirtualTuple(slot);
	pg_datalake_rows_close(rows);
	return true;
}

static bool
lakehouse_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	return false;
}

static void
lakehouse_tuple_get_latest_tid(TableScanDesc scan, ItemPointer tid)
{
	LAKEHOUSE_UNSUPPORTED("TID fetches", "27");
}

static bool
lakehouse_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
								   Snapshot snapshot)
{
	elog(ERROR, "lakehouse: tuple_satisfies_snapshot is not supported on managed tables");
	return false;				/* unreachable */
}

static TransactionId
lakehouse_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
	LAKEHOUSE_UNSUPPORTED("indexes", "29");
	return InvalidTransactionId;	/* unreachable */
}

static double
lakehouse_index_build_range_scan(Relation table_rel, Relation index_rel,
								 IndexInfo *index_info, bool allow_sync,
								 bool anyvisible, bool progress,
								 BlockNumber start_blockno,
								 BlockNumber numblocks,
								 IndexBuildCallback callback,
								 void *callback_state, TableScanDesc scan)
{
	LAKEHOUSE_UNSUPPORTED("indexes", "29");
	return 0.0;					/* unreachable */
}

static void
lakehouse_index_validate_scan(Relation table_rel, Relation index_rel,
							  IndexInfo *index_info, Snapshot snapshot,
							  ValidateIndexState *state)
{
	LAKEHOUSE_UNSUPPORTED("indexes", "29");
}

/* ---------------- sizing / analysis ---------------- */

static uint64
lakehouse_relation_size(Relation rel, ForkNumber forkNumber)
{
	/* hollow local storage: report 0 — real sizing is Iceberg's table
	 * metrics (EXPLAIN + reportMetrics), not relpages */
	return 0;
}

static void
lakehouse_relation_estimate_size(Relation rel, int32 *attr_widths,
								 BlockNumber *pages, double *tuples,
								 double *allvisfrac)
{
	*pages = 0;
	*tuples = 0;
	*allvisfrac = 0;
}

static bool
lakehouse_relation_needs_toast_table(Relation rel)
{
	/* no local tuple storage until slice 27; TOAST policy is revisited
	 * there (large values go to parquet pages, not a local toast table) */
	return false;
}

static bool
lakehouse_scan_analyze_next_block(TableScanDesc scan, ReadStream *stream)
{
	/* ANALYZE samples nothing yet (stats arrive with managed DDL); a
	 * false here makes acquire_sample_rows return zero rows cleanly */
	return false;
}

static bool
lakehouse_scan_analyze_next_tuple(TableScanDesc scan, double *liverows,
								  double *deadrows, TupleTableSlot *slot)
{
	return false;
}

static bool
lakehouse_scan_bitmap_next_tuple(TableScanDesc scan, TupleTableSlot *slot,
								 bool *recheck, uint64 *lossy_pages,
								 uint64 *exact_pages)
{
	LAKEHOUSE_UNSUPPORTED("bitmap scans", "29");
	return false;				/* unreachable */
}

static bool
lakehouse_scan_sample_next_block(TableScanDesc scan, SampleScanState *scanstate)
{
	LAKEHOUSE_UNSUPPORTED("TABLESAMPLE", "29");
	return false;				/* unreachable */
}

static bool
lakehouse_scan_sample_next_tuple(TableScanDesc scan, SampleScanState *scanstate,
								 TupleTableSlot *slot)
{
	LAKEHOUSE_UNSUPPORTED("TABLESAMPLE", "29");
	return false;				/* unreachable */
}

static void
lakehouse_scan_set_tidrange(TableScanDesc scan, ItemPointer mintid,
							ItemPointer maxtid)
{
	LAKEHOUSE_UNSUPPORTED("TID range scans", "29");
}

static bool
lakehouse_scan_getnextslot_tidrange(TableScanDesc scan,
									ScanDirection direction,
									TupleTableSlot *slot)
{
	LAKEHOUSE_UNSUPPORTED("TID range scans", "29");
	return false;				/* unreachable */
}

static Size
lakehouse_parallelscan_estimate(Relation rel)
{
	return 0;
}

static Size
lakehouse_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	LAKEHOUSE_UNSUPPORTED("parallel scans", "29");
	return 0;					/* unreachable */
}

static void
lakehouse_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	LAKEHOUSE_UNSUPPORTED("parallel scans", "29");
}

/* ---------------- handler ---------------- */

static const TableAmRoutine lakehouse_tableam_methods = {
	.type = T_TableAmRoutine,

	.slot_callbacks = lakehouse_slot_callbacks,

	.scan_begin = lakehouse_scan_begin,
	.scan_end = lakehouse_scan_end,
	.scan_rescan = lakehouse_scan_rescan,
	.scan_getnextslot = lakehouse_scan_getnextslot,

	.parallelscan_estimate = lakehouse_parallelscan_estimate,
	.parallelscan_initialize = lakehouse_parallelscan_initialize,
	.parallelscan_reinitialize = lakehouse_parallelscan_reinitialize,

	.index_scan_begin = lakehouse_index_scan_begin,
	.index_scan_reset = lakehouse_index_scan_reset,
	.index_scan_end = lakehouse_index_scan_end,

	.fetch_tid = lakehouse_fetch_tid,
	.tuple_fetch_row_version = lakehouse_tuple_fetch_row_version,
	.tuple_tid_valid = lakehouse_tuple_tid_valid,
	.tuple_get_latest_tid = lakehouse_tuple_get_latest_tid,
	.tuple_satisfies_snapshot = lakehouse_tuple_satisfies_snapshot,
	.index_delete_tuples = lakehouse_index_delete_tuples,

	.tuple_insert = lakehouse_tuple_insert,
	.tuple_insert_speculative = lakehouse_tuple_insert_speculative,
	.tuple_complete_speculative = lakehouse_tuple_complete_speculative,
	.multi_insert = lakehouse_multi_insert,
	.tuple_delete = lakehouse_tuple_delete,
	.tuple_update = lakehouse_tuple_update,
	.tuple_lock = lakehouse_tuple_lock,
	.finish_bulk_insert = NULL,	/* optional */

	.relation_set_new_filelocator = lakehouse_relation_set_new_filelocator,
	.relation_nontransactional_truncate = lakehouse_relation_nontransactional_truncate,
	.relation_copy_data = lakehouse_relation_copy_data,
	.relation_copy_for_cluster = lakehouse_relation_copy_for_cluster,
	.relation_vacuum = lakehouse_relation_vacuum,
	.scan_analyze_next_block = lakehouse_scan_analyze_next_block,
	.scan_analyze_next_tuple = lakehouse_scan_analyze_next_tuple,
	.index_build_range_scan = lakehouse_index_build_range_scan,
	.index_validate_scan = lakehouse_index_validate_scan,

	.relation_size = lakehouse_relation_size,
	.relation_needs_toast_table = lakehouse_relation_needs_toast_table,
	.relation_toast_am = NULL,	/* only required if needs_toast_table */
	.relation_fetch_toast_slice = NULL,	/* ditto */
	.relation_estimate_size = lakehouse_relation_estimate_size,

	.scan_bitmap_next_tuple = lakehouse_scan_bitmap_next_tuple,
	.scan_sample_next_block = lakehouse_scan_sample_next_block,
	.scan_sample_next_tuple = lakehouse_scan_sample_next_tuple,

	.scan_set_tidrange = lakehouse_scan_set_tidrange,
	.scan_getnextslot_tidrange = lakehouse_scan_getnextslot_tidrange,

	.amoptions = lakehouse_relation_reloptions,
};

/*
 * SUBXACT_EVENT_ABORT_SUB: discard staged rows owned by the aborted
 * subtransaction (or its descendants). ROLLBACK TO SAVEPOINT must hide
 * them again — otherwise rolled-back inserts stay visible through the
 * own-writes overlay and, worse, get published at main commit
 * (13-review blocker #3).
 */
static void
lakehouse_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
						   SubTransactionId parentSubid, void *arg)
{
	HASH_SEQ_STATUS seq;
	LakehouseWriteEntry *entry;

	if (event != SUBXACT_EVENT_ABORT_SUB || write_entries == NULL)
	{
		elog(LOG, "lakehouse subxact event %d subid %u", (int) event, mySubid);
		return;
	}
	elog(LOG, "lakehouse ABORT_SUB subid %u", mySubid);
	hash_seq_init(&seq, write_entries);
	while ((entry = hash_seq_search(&seq)) != NULL)
	{
		int			w = 0;
		int			i;

		if (entry->staged_rows == 0)
			continue;
		for (i = 0; i < entry->staged_rows; i++)
		{
			if (entry->staged_subids[i] >= mySubid)
				continue;		/* owned by the aborted subxact tree */
			for (int k = 0; k < entry->natts; k++)
			{
				entry->staged_values[(size_t) w * entry->natts + k] =
					entry->staged_values[(size_t) i * entry->natts + k];
				entry->staged_isnull[(size_t) w * entry->natts + k] =
					entry->staged_isnull[(size_t) i * entry->natts + k];
			}
			entry->staged_subids[w] = entry->staged_subids[i];
			w++;
		}
		entry->staged_rows = w;
	}
	/* COW tombstones captured in the aborted subxact tree are discarded
	 * too — a savepoint-rolled-back DELETE must not rewrite a file */
	if (event == SUBXACT_EVENT_ABORT_SUB && write_entries != NULL)
	{
		hash_seq_init(&seq, write_entries);
		while ((entry = hash_seq_search(&seq)) != NULL)
		{
			int			w;
			int			i;

			for (i = 0, w = 0; i < entry->n_tomb_files; i++)
			{
				LakehouseTombFile *tf = entry->tomb_files[i];
				int			r;
				int			rw = 0;

				for (r = 0; r < tf->nrows; r++)
				{
					if (tf->row_subids[r] >= mySubid)
						continue;	/* owned by the aborted subxact tree */
					tf->rows[rw] = tf->rows[r];
					tf->row_subids[rw] = tf->row_subids[r];
					rw++;
				}
				tf->nrows = rw;
				if (rw > 0)
					entry->tomb_files[w++] = tf;
				/* fully abandoned: the file record is dropped (no DELETED
				 * entry rides to the commit for a rolled-back DELETE) */
			}
			entry->n_tomb_files = w;
		}
	}
	prune_scan_registry_above(mySubid);
	(void) parentSubid;
}

/* lakehouse GUC surface (spec 15 §7.2 P2): extension-defined via
 * DefineCustom*Variable — the seam Postgres provides; NO core edit
 * (RULES §3: the guc_tables.c allowlist entry is retired). */
int			lakehouse_write_mode = 1;	/* enum: 1 = cow, 2 = staged */
/* kB size-knob bounds (spec 15 P2): min 32MB, max 4GB; defaults 128MB
 * (target_file_size) / 256MB (cache) */
#define LAKEHOUSE_MIN_SIZE_KB (32 * 1024)
#define LAKEHOUSE_MAX_SIZE_KB (4 * 1024 * 1024)

int			lakehouse_target_file_size = 128 * 1024;	/* kB; SHOWs as 128MB */
int			lakehouse_publish_max_lag = 30;		/* s */
int			lakehouse_cache_max_bytes = 256 * 1024;		/* kB; SHOWs as 256MB */

static const struct config_enum_entry lakehouse_write_mode_options[] = {
	{"cow", 1, false},
	{"staged", 2, false},
	{NULL, 0, false},
};

void
_PG_init(void)
{
	DefineCustomEnumVariable("lakehouse.write_mode",
							 "Managed write mode: cow (copy-on-write, default) or staged (deferred publish, spec 15).",
							 NULL,
							 &lakehouse_write_mode,
							 1,	/* boot default: cow */
							 lakehouse_write_mode_options,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("lakehouse.target_file_size",
							"Target size of one written data file; the writer rolls files at this threshold.",
							NULL,
							&lakehouse_target_file_size,
							128 * 1024, LAKEHOUSE_MIN_SIZE_KB,
							LAKEHOUSE_MAX_SIZE_KB,
							PGC_USERSET,
							GUC_UNIT_KB,
							NULL, NULL, NULL);

	DefineCustomIntVariable("lakehouse.publish_max_lag",
							"Maximum age of unpublished staged deltas before the publisher must flush (spec 15 G5).",
							NULL,
							&lakehouse_publish_max_lag,
							30, 0, PG_INT32_MAX / 2,
							PGC_SUSET,
							GUC_UNIT_S,
							NULL, NULL, NULL);

	DefineCustomIntVariable("lakehouse.cache_max_bytes",
							"Byte cap of the per-backend immutable-object read cache (spec 15 §6.2).",
							NULL,
							&lakehouse_cache_max_bytes,
							256 * 1024, LAKEHOUSE_MIN_SIZE_KB,
							LAKEHOUSE_MAX_SIZE_KB,
							PGC_USERSET,
							GUC_UNIT_KB,
							NULL, NULL, NULL);

	MarkGUCPrefixReserved("lakehouse");

	/* per-backend immutable-object block cache, capped by GUC (spec 15
	 * §6.2; first call wins — process-lifetime capacity) */
	pg_datalake_core_cache_init((int64) lakehouse_cache_max_bytes * 1024);

	RegisterXactCallback(lakehouse_xact_callback, NULL);
	RegisterSubXactCallback(lakehouse_subxact_callback, NULL);
}

/* pg_am oid of the lakehouse AM (resolved once per backend) */
static Oid
lakehouse_am_oid(void)
{
	static Oid	amoid = InvalidOid;

	if (OidIsValid(amoid))
		return amoid;
	{
		HeapTuple	tup = SearchSysCache1(AMNAME, CStringGetDatum("lakehouse"));

		if (!HeapTupleIsValid(tup))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("access method \"lakehouse\" does not exist")));
		amoid = ((Form_pg_am) GETSTRUCT(tup))->oid;
		ReleaseSysCache(tup);
	}
	return amoid;
}

/*
 * lakehouse.create_remote(regclass) -> text: create the remote Iceberg
 * table for an attached managed relation (managed CREATE, slice 27c).
 * The schema comes from the relation's own tupdesc — one definition, one
 * catalog, one visibility. Idempotent: a 409 (table exists) reports
 * 'exists' instead of erroring.
 */
PG_FUNCTION_INFO_V1(lakehouse_create_remote);

Datum
lakehouse_create_remote(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Relation	rel;
	LakehouseWriteEntry *entry;
	char	   *schema_json;
	char	   *loc;
	char		errbuf[1024];
	int			err_kind = 0;

	if (!object_ownercheck(RelationRelationId, relid, GetUserId()))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be owner of %s to create its remote table",
						get_rel_name(relid) ? get_rel_name(relid) : "?")));
	rel = table_open(relid, AccessShareLock);
	if (rel->rd_rel->relkind != RELKIND_RELATION ||
		rel->rd_rel->relam != lakehouse_am_oid())
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("%s is not a lakehouse managed table",
						RelationGetRelationName(rel))));

	/* reuse the entry machinery for identity resolution + schema build */
	entry = get_write_entry(rel);
	schema_json = build_schema_json(RelationGetDescr(rel));

	errbuf[0] = '\0';
	loc = pg_datalake_create_table(entry->opts.endpoint,
								   entry->opts.warehouse ? entry->opts.warehouse : "",
								   entry->opts.token ? entry->opts.token : "",
								   entry->opts.namespace,
								   entry->opts.table,
								   entry->opts.s3_endpoint,
								   entry->opts.s3_access_key,
								   entry->opts.s3_secret_key,
								   schema_json,
								   errbuf, sizeof(errbuf), &err_kind);
	table_close(rel, AccessShareLock);
	if (loc == NULL)
	{
		/* error KIND from the FFI (13-review #6): 1 = conflict — classify
		 * by type, never by searching the message for "409" */
		if (err_kind == 1)
			PG_RETURN_TEXT_P(cstring_to_text("exists"));
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg("lakehouse: could not create remote table: %s", errbuf)));
	}
	{
		text	   *result = cstring_to_text(loc);

		pg_datalake_write_commit_result_free(loc);
		PG_RETURN_TEXT_P(result);
	}
}


/*
 * pg_lakehouse_publish_staged (P4, spec 15 §4): emit every unpublished
 * delta segment of `relid` — ONE write session, publish_segment per
 * row-payload, ONE append commit, then the ledger flip (the segment
 * row's own columns). Returns the number of segments published.
 */
PG_FUNCTION_INFO_V1(pg_lakehouse_publish_staged);

Datum
pg_lakehouse_publish_staged(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Relation	rel;
	LakehouseWriteEntry *entry = NULL;
	int			published_count = 0;
	int			ret;
	MemoryContext cb_ctx = CurrentMemoryContext;
	char		errbuf[1024];

	rel = table_open(relid, AccessShareLock);
	entry = get_write_entry(rel);
	open_write_session(entry, RelationGetDescr(rel));
	if (entry->ws == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("lakehouse: publisher could not open a write session")));

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	{
		StringInfoData sql;

		initStringInfo(&sql);
		appendStringInfo(&sql,
						 "SELECT seg_id, payload FROM"
						 " lakehouse.lakehouse_delta_segments"
						 " WHERE relid = %u AND NOT published"
						 " ORDER BY seg_id", (unsigned) relid);
		ret = SPI_execute(sql.data, false, 0);
	}
	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("lakehouse: publisher could not read delta segments")));

	for (int i = 0; i < SPI_processed; i++)
	{
		bool		isnull;
		Datum		seg_id = SPI_getbinval(SPI_tuptable->vals[i],
										   SPI_tuptable->tupdesc, 1, &isnull);
		Datum		payload = SPI_getbinval(SPI_tuptable->vals[i],
											SPI_tuptable->tupdesc, 2, &isnull);
		char	   *payload_c;
		char	   *loc;

		if (isnull)
			continue;
		payload_c = TextDatumGetCString(payload);

		/* emission core (Rust): payload rows -> writer -> flush */
		if (!pg_datalake_publish_segment(entry->ws, payload_c,
										 errbuf, sizeof(errbuf)))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("lakehouse: publisher emission failed for segment %lld: %s",
							(long long) DatumGetInt64(seg_id), errbuf)));

		/* commit ONE snapshot per segment (v1 discipline, spec 15 §4.4
		 * single-segment-per-commit) and flip the ledger immediately */
		loc = pg_datalake_write_commit(entry->ws, 3, errbuf, sizeof(errbuf));
		if (loc == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("lakehouse: publisher commit failed: %s", errbuf)));
		{
			StringInfoData upd;

			initStringInfo(&upd);
			appendStringInfo(&upd,
							 "UPDATE lakehouse.lakehouse_delta_segments"
							 " SET published = true,"
							 " published_snapshot_id = %s"
							 " WHERE seg_id = %lld",
							 "0",	/* snapshot id from loc: parsed below */
							 (long long) DatumGetInt64(seg_id));
			(void) upd;		/* snapshot-id parse lands with P4b; flip now */
			ret = SPI_execute(psprintf("UPDATE"
									   " lakehouse.lakehouse_delta_segments"
									   " SET published = true"
									   " WHERE seg_id = %lld",
									   (long long) DatumGetInt64(seg_id)),
							  false, 0);
			if (ret != SPI_OK_UPDATE)
				ereport(ERROR,
						(errcode(ERRCODE_IO_ERROR),
						 errmsg("lakehouse: publisher ledger flip failed")));
			published_count++;
		}
		pg_datalake_write_commit_result_free(loc);
	}
	PopActiveSnapshot();
	SPI_finish();
	table_close(rel, AccessShareLock);
	MemoryContextSwitchTo(cb_ctx);

	PG_RETURN_INT32(published_count);
}

PG_FUNCTION_INFO_V1(lakehouse_tableam_handler);

Datum
lakehouse_tableam_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&lakehouse_tableam_methods);
}
