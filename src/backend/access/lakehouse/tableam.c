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
#include "utils/datum.h"
#include "nodes/miscnodes.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/storage_xlog.h"
#include "catalog/storage.h"
#include "commands/vacuum.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/smgr.h"
#include "utils/lsyscache.h"
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
	Relation	mt;
	TableScanDesc scan;
	TupleTableSlot *slot;
	bool		found = false;
	char	   *identity = NULL;

	memset(opts, 0, sizeof(*opts));
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

			for (i = 2; i <= 9; i++)
			{
				char	  **field = NULL;

				v = slot_getattr(slot, i, &isnull);
				if (isnull)
					continue;
				switch (i)
				{
					case 2:
						identity = text_to_cstring(DatumGetTextPP(v));
						break;
					case 3:
						field = &opts->endpoint;
						break;
					case 4:
						field = &opts->warehouse;
						break;
					case 5:
						field = &opts->token;
						break;
					case 6:
						field = &opts->s3_endpoint;
						break;
					case 7:
						field = &opts->s3_access_key;
						break;
					case 8:
						field = &opts->s3_secret_key;
						break;
					case 9:
						field = &opts->snapshot_id;
						break;
				}
				if (field != NULL)
					*field = text_to_cstring(DatumGetTextPP(v));
			}
			found = true;
			break;
		}
	}

	ExecDropSingleTupleTableSlot(slot);
	table_endscan(scan);
	table_close(mt, AccessShareLock);

	if (found && identity != NULL)
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
typedef struct LakehouseWriteEntry
{
	Oid			relid;
	WriteSession *ws;			/* FFI write session; NULL until 1st insert */
	MemoryContext mcxt;			/* all entry allocations live here */
	/* own-writes overlay: staged tuples, copied datum-wise into mcxt */
	Datum	   *staged_values;
	bool	   *staged_isnull;
	int			staged_rows;
	int			natts;
	TupleDesc	tupdesc;
	/* identity, resolved at first insert */
	LakehouseOptions opts;
	bool		have_identity;
}			LakehouseWriteEntry;

static HTAB *write_entries = NULL;

static void
lakehouse_xact_callback(XactEvent event, void *arg)
{
	HASH_SEQ_STATUS seq;
	LakehouseWriteEntry *entry;

	if (write_entries == NULL)
		return;

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
			if (entry->ws == NULL)
				continue;
			{
				char		errbuf[1024];
				char	   *loc;

				errbuf[0] = '\0';
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
		case NAMEOID:
		case BPCHAROID:
			escape_json(buf, DatumGetCString(DirectFunctionCall1(textout, v)));
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
			appendStringInfoString(buf, "{\"b\": ");
			escape_json(buf, DatumGetCString(DirectFunctionCall1(byteaout, v)));
			appendStringInfoChar(buf, '}');
			break;
		default:
			/* build_schema_json rejected it before we ever got a row */
			elog(ERROR, "lakehouse: unexpected column type in managed insert");
	}
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
		entry->staged_rows = 0;
		entry->natts = 0;
		entry->tupdesc = NULL;
		memset(&entry->opts, 0, sizeof(entry->opts));
		entry->have_identity =
			ExtractManagedIdentity(relid, &entry->opts);
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
		entry->tupdesc = CreateTupleDescCopyConstr(RelationGetDescr(rel));
		entry->natts = entry->tupdesc->natts;
	}
	return entry;
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
}			LakehouseTableScanDescData;
typedef LakehouseTableScanDescData *LakehouseTableScanDesc;

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

	/* projection: visible attribute names, in attribute order */
	initStringInfo(&proj);
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
		appendStringInfoString(&proj, NameStr(attr->attname));
		first_col = false;
		getTypeInputInfo(attr->atttypid,
						 &scan->in_funcs[i].fn_oid,
						 &scan->typioparams[i]);
		fmgr_info(scan->in_funcs[i].fn_oid, &scan->in_funcs[i]);
	}
	{
		int			proj_col = 0;

		for (i = 0; i < tupdesc->natts; i++)
			if (scan->slot_index[i] >= 0)
				scan->slot_index[i] = proj_col++;
	}
	scan->projection_csv = proj.data;

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

	if (!ExtractManagedIdentity(RelationGetRelid(rel), &scan->opts))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("managed table %s has no lakehouse identity",
						RelationGetRelationName(rel)),
				 errdetail("Managed DDL wiring arrives with M2 slice 27; "
						   "for now attach one explicitly."),
				 errhint("SELECT lakehouse.attach_table('%s'::regclass, "
						 "'namespace.table_name', endpoint => '...');",
						 RelationGetRelationName(rel))));

	lakehouse_scan_common_setup(scan, rel, NULL);
	if (write_entries != NULL)
	{
		bool		found;

		scan->entry = (LakehouseWriteEntry *)
			hash_search(write_entries, &rel->rd_id, HASH_FIND, &found);
		if (!found)
			scan->entry = NULL;
	}
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

	/* own-writes overlay (doc 07 §2): after the snapshot's files, yield
	 * the rows this transaction staged but has not committed yet */
	if (scan->entry != NULL &&
		scan->current_task >= scan->task_count)
	{
		LakehouseWriteEntry *entry = scan->entry;

		while (scan->staged_pos < entry->staged_rows)
		{
			int			k;
			Datum	   *row = entry->staged_values +
				(size_t) scan->staged_pos * entry->natts;
			bool	   *nulls = entry->staged_isnull +
				(size_t) scan->staged_pos * entry->natts;

			scan->staged_pos++;
			ExecClearTuple(slot);
			for (k = 0; k < tupdesc->natts; k++)
			{
				slot->tts_values[k] = row[k];
				slot->tts_isnull[k] = nulls[k];
			}
			ExecStoreVirtualTuple(slot);
			return true;
		}
	}

	for (;;)
	{
		if (scan->rows == NULL)
		{
			char		errbuf[1024];
			const char *file;

			if (scan->current_task >= scan->task_count)
				return false;

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
		}

		if (pg_datalake_rows_next(scan->rows))
		{
			ExecClearTuple(slot);
			for (i = 0; i < tupdesc->natts; i++)
			{
				int			pcol = scan->slot_index[i];

				if (pcol < 0 || pg_datalake_rows_col_null(scan->rows, pcol))
				{
					slot->tts_values[i] = PointerGetDatum(NULL);
					slot->tts_isnull[i] = true;
					continue;
				}
				slot->tts_values[i] = InputFunctionCall(&scan->in_funcs[i],
							pg_datalake_rows_col_text(scan->rows, pcol),
							scan->typioparams[i], -1);
				slot->tts_isnull[i] = false;
			}
			ExecStoreVirtualTuple(slot);
			return true;
		}

		pg_datalake_rows_close(scan->rows);
		scan->rows = NULL;
		scan->current_task++;
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

static void
lakehouse_tuple_insert(Relation rel, TupleTableSlot *slot, CommandId cid,
					   uint32 options, BulkInsertStateData *bistate)
{
	LakehouseWriteEntry *entry = get_write_entry(rel);
	TupleDesc	tupdesc = RelationGetDescr(rel);
	StringInfoData row;
	int			i;
	char		errbuf[1024];

	slot_getallattrs(slot);

	/* first insert opens the FFI write session (loadTable + writer) */
	if (entry->ws == NULL)
	{
		char	   *schema_json = build_schema_json(tupdesc);

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
										  schema_json,
										  errbuf, sizeof(errbuf));
		if (entry->ws == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("lakehouse: could not open write session: %s",
							errbuf)));
	}

	/* tuple -> typed row JSON (datum arithmetic, no text formatting) */
	initStringInfo(&row);
	appendStringInfoChar(&row, '[');
	for (i = 0; i < tupdesc->natts; i++)
	{
		if (i > 0)
			appendStringInfoChar(&row, ',');
		append_cell_json(&row, TupleDescAttr(tupdesc, i),
						 slot->tts_values[i], slot->tts_isnull[i]);
	}
	appendStringInfoChar(&row, ']');

	errbuf[0] = '\0';
	if (!pg_datalake_write_push(entry->ws, row.data, errbuf, sizeof(errbuf)))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("lakehouse: could not stage row: %s", errbuf)));

	/* own-writes overlay: copy the tuple into the txn-scoped entry */
	{
		int			j;
		MemoryContext old = MemoryContextSwitchTo(entry->mcxt);

		if (entry->staged_rows == 0)
		{
			entry->staged_values = (Datum *)
				palloc(tupdesc->natts * sizeof(Datum) * 64);
			entry->staged_isnull = (bool *)
				palloc(tupdesc->natts * sizeof(bool) * 64);
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
		}
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

static TM_Result
lakehouse_tuple_delete(Relation rel, ItemPointer tid, CommandId cid,
					   uint32 options, Snapshot snapshot, Snapshot crosscheck,
					   bool wait, TM_FailureData *tmfd)
{
	LAKEHOUSE_UNSUPPORTED("DELETE", "28");
	return TM_Ok;				/* unreachable */
}

static TM_Result
lakehouse_tuple_update(Relation rel, ItemPointer otid, TupleTableSlot *slot,
					   CommandId cid, uint32 options, Snapshot snapshot,
					   Snapshot crosscheck, bool wait, TM_FailureData *tmfd,
					   LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes)
{
	LAKEHOUSE_UNSUPPORTED("UPDATE", "28");
	return TM_Ok;				/* unreachable */
}

static TM_Result
lakehouse_tuple_lock(Relation rel, ItemPointer tid, Snapshot snapshot,
					 TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
					 LockWaitPolicy wait_policy, uint8 flags,
					 TM_FailureData *tmfd)
{
	LAKEHOUSE_UNSUPPORTED("row locking", "28");
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

static bool
lakehouse_tuple_fetch_row_version(Relation rel, ItemPointer tid,
								  Snapshot snapshot, TupleTableSlot *slot)
{
	LAKEHOUSE_UNSUPPORTED("row-version fetches", "27");
	return false;				/* unreachable */
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
};

void
_PG_init(void)
{
	RegisterXactCallback(lakehouse_xact_callback, NULL);
}

PG_FUNCTION_INFO_V1(lakehouse_tableam_handler);

Datum
lakehouse_tableam_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&lakehouse_tableam_methods);
}
