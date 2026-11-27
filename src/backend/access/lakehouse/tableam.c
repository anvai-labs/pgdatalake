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
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_inherits.h"
#include "foreign/foreign.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "catalog/pg_am_d.h"
#include "catalog/storage_xlog.h"
#include "catalog/storage.h"
#include "commands/vacuum.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "storage/smgr.h"
#include "utils/lsyscache.h"
#include "access/reloptions.h"
#include "utils/guc.h"
#include "executor/spi.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "tcop/tcopprot.h"
#include "utils/rel.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include "lakehouse.h"

#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/numeric.h"
#include "utils/typcache.h"
#include "utils/rls.h"
#include "utils/uuid.h"
#include "varatt.h"

#include "batch_fill.h"
#include "lakehouse_tableam.h"
#include "managed_credentials.h"

/*
 * The 1.7 tables are optional. When present, only the extension-owned,
 * permanent heap tables can supply capture policy or receive elevated writes.
 */
static Oid
capture_table_owner(const char *name, bool missing_ok)
{
	Oid			namespace_id = get_namespace_oid("lakehouse", false);
	Oid			table_id = get_relname_relid(name, namespace_id);
	Oid			extension_id;
	HeapTuple	tuple;
	Form_pg_class form;
	Oid			owner;

	if (missing_ok && !OidIsValid(table_id))
		return InvalidOid;

	extension_id = get_extension_oid("lakehouse", false);
	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(table_id));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "lakehouse capture table %s is missing", name);
	form = (Form_pg_class) GETSTRUCT(tuple);
	if (form->relkind != RELKIND_RELATION ||
		form->relpersistence != RELPERSISTENCE_PERMANENT ||
		form->relam != HEAP_TABLE_AM_OID ||
		getExtensionOfObject(RelationRelationId, table_id) != extension_id)
		elog(ERROR, "invalid lakehouse capture table %s", name);
	owner = form->relowner;
	ReleaseSysCache(tuple);
	return owner;
}

/*
 * All callers hold a lock on the managed relation that conflicts with an
 * operator transition. Read current policy even under REPEATABLE READ.
 * Callers that don't need the active ID can also detect an inactive slot.
 */
static bool
lookup_capture_binding(Oid relid, pg_uuid_t *binding_id)
{
	Oid			owner = capture_table_owner("managed_capture_state", true);
	Oid			user;
	int			sec_context;
	bool		required = false;

	if (!OidIsValid(owner))
		return false;

	GetUserIdAndSecContext(&user, &sec_context);
	SPI_connect();
	PushActiveSnapshot(GetLatestSnapshot());
	PG_TRY();
	{
		Oid			argtype = OIDOID;
		Datum		arg = ObjectIdGetDatum(relid);
		int			ret;
		const char *sql;

		SetUserIdAndSecContext(owner, sec_context | SECURITY_LOCAL_USERID_CHANGE |
							   SECURITY_RESTRICTED_OPERATION);

		if (binding_id == NULL)
			sql = "SELECT 1 FROM lakehouse.managed_capture_state"
				" WHERE relid OPERATOR(pg_catalog.=) $1";
		else
			sql = "SELECT b.binding_id,"
				" b.source_columns OPERATOR(pg_catalog.=) lakehouse.capture_source_columns($1)"
				" FROM lakehouse.managed_capture_state s"
				" LEFT JOIN lakehouse.managed_capture_bindings b"
				" ON b.binding_id OPERATOR(pg_catalog.=) s.active_binding_id"
				" WHERE s.relid OPERATOR(pg_catalog.=) $1";
		ret = SPI_execute_with_args(sql, 1, &argtype, &arg, NULL, true, 1);
		if (ret != SPI_OK_SELECT)
			elog(ERROR, "could not read lakehouse capture binding");
		if (SPI_processed == 1)
		{
			required = true;
			if (binding_id != NULL)
			{
				bool		isnull;
				Datum		id = SPI_getbinval(SPI_tuptable->vals[0],
											   SPI_tuptable->tupdesc, 1, &isnull);

				if (isnull)
					ereport(ERROR,
							(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							 errmsg("managed capture binding is inactive")));
				memcpy(binding_id, DatumGetUUIDP(id), sizeof(*binding_id));
				id = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull);
				if (isnull || !DatumGetBool(id))
					ereport(ERROR,
							(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							 errmsg("managed capture source schema changed"),
							 errhint("Re-enroll the table against a verified Iceberg schema.")));
			}
		}
	}
	PG_FINALLY();
	{
		SetUserIdAndSecContext(user, sec_context);
	}
	PG_END_TRY();
	PopActiveSnapshot();
	SPI_finish();
	return required;
}

/*
 * Planner-hook predicate: a capture-bound relation must keep its ordinary
 * aggregate — the count rewrite cannot preserve the managed scan's
 * admission, snapshot and cut, and the executor's bound scan now serves
 * exactly that multiplicity. Runs SPI inside the planner the same way
 * scan_begin does.
 */
bool
lakehouse_rel_is_capture_bound(Oid relid)
{
	/*
	 * Planning inside a parallel worker cannot take the latest snapshot
	 * (snapmgr raises an internal error); decline the count rewrite and let
	 * the executor's bound gate refuse parallel scans catalog-locally with
	 * the ordinary 0A000.
	 */
	if (IsInParallelMode())
		return false;
	return lookup_capture_binding(relid, NULL);
}

void
lakehouse_check_capture_read(Oid relid)
{
	if (lookup_capture_binding(relid, NULL))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("bound managed capture does not support table scans")));
}

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
	/*
	 * merge mode: caller's opts (possibly from WITH-options) keeps its
	 * identity fields; only NULL fields get filled from the attach row
	 */
	Relation	mt;
	TableScanDesc scan;
	TupleTableSlot *slot;
	bool		found = false;
	char	   *identity = NULL;
	Oid			serverid = InvalidOid;

	mt = table_open(get_managed_tables_relid(), AccessShareLock);
	scan = table_beginscan(mt, GetActiveSnapshot(), 0, NULL, 0);

	/*
	 * the slot type must be the scanned table's AM's own (heap scans refuse
	 * minimal/virtual slots — on-disk tuple storage)
	 */
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
					default:
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
		 * storage scope come from the SERVER; credentials come from the USER
		 * MAPPING of the effective user — never from table options or this
		 * table (both are dumpable).
		 */
		ForeignServer *server = GetForeignServer(serverid);
		ListCell   *lc;

		foreach(lc, server->options)
		{
			DefElem    *def = (DefElem *) lfirst(lc);

			if (lakehouse_IsBoundServerOption(def->defname))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("managed tables cannot use a bound read server")));
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

/*
 * TID encoding (slice 28): block = task_idx * MAX_OFFSET + (rowidx >> 11),
 * offset = (rowidx & 0x7FF) + 1 — offset stays within MaxOffsetNumber,
 * rowidx up to ~4M per task, task_idx up to ~1M. Row indexes beyond the
 * encoding range error loudly at DML time rather than mis-targeting.
 */
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
	/*
	 * HASH KEY — first member, per the dynahash key-aliasing idiom (see the
	 * full warning above LakehouseWriteEntry, where a non-key member at
	 * offset 0 once corrupted lookups). dynahash copies the key in; never
	 * write this field from init code.
	 */
	Oid			relid;
	LakehouseTableScanDescData *scan;
	SubTransactionId subid;
} LakehouseScanRegistryEntry;

static HTAB *scan_registry = NULL;

/* lakehouse GUCs (defined + registered in _PG_init, spec 15 P2) */
extern int	lakehouse_write_mode;	/* enum: 1 = cow, 2 = staged */
extern int	lakehouse_target_file_size;
extern int	lakehouse_publish_max_lag;
extern int	lakehouse_cache_max_bytes;

/* P4c publisher worker knobs (defined + registered in _PG_init) */
extern bool lakehouse_publisher_enabled;
extern char *lakehouse_publish_database;

#define LAKEHOUSE_WRITE_MODE_COW		1
#define LAKEHOUSE_WRITE_MODE_STAGED		2

static bool xact_callbacks_registered = false;

static void staged_capture_segment(LakehouseWriteEntry *entry);


static HTAB *write_entries = NULL;

static void append_cell_json(StringInfoData *buf, Form_pg_attribute attr,
							 Datum v, bool isnull);
static LakehouseTombFile *tomb_file_get(LakehouseWriteEntry *entry,
										const char *path, bool create);
static void tomb_file_add_row(LakehouseWriteEntry *entry,
							  LakehouseTombFile *tf, uint64 rowidx);
static bool tomb_file_has_row(const LakehouseTombFile *tf, uint64 rowidx);
void		open_write_session(LakehouseWriteEntry *entry, TupleDesc tupdesc);
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
	 * Two-phase commit is rejected with pending managed writes BEFORE prepare
	 * (13-review blocker #3): a prepared transaction's remote commit decision
	 * must survive crashes, which this AM's recovery protocol does not yet
	 * support.
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
		 * callback then frees the sessions) — no partial commit ever exists
		 * remotely (Iceberg's core guarantee).
		 */
		/*
		 * P3 regression fix: hash_seq_search is INVALIDATED by work done
		 * mid-iteration (the capture's SPI runs hash operations); the old
		 * inline capture re-yielded the same entry per row. Phase 1: iterate
		 * + collect. Phase 2 (seq finished): capture.
		 */
		{
			/*
			 * BULK CAPACITY (spec 16 blocker 2): the collector was a FIXED
			 * 64-slot array that silently dropped entry 65 onward — a
			 * transaction inserting into 65 staged relations committed its
			 * rows in Postgres but wrote only 64 ledger segments (silent row
			 * loss on exactly the bulk multi-table load workload staging
			 * exists for; reproduced in staged_insert.sql). Size the list
			 * from the hash itself: every entry can hold staged rows, so slot
			 * count == entry count. palloc'd in the transaction context;
			 * freed with it.
			 */
			LakehouseWriteEntry **staged_list;
			int			n_staged = 0;

			staged_list = (LakehouseWriteEntry **)
				palloc(hash_get_num_entries(write_entries) *
					   sizeof(LakehouseWriteEntry *));

			hash_seq_init(&seq, write_entries);
			while ((entry = hash_seq_search(&seq)) != NULL)
			{
				/*
				 * staged mode (D1-rev): the delta row IS the commit —
				 * durability from the heap+WAL machinery, no remote I/O
				 */
				if (entry->write_mode == LAKEHOUSE_WRITE_MODE_STAGED &&
					entry->staged_rows > 0)
				{
					staged_list[n_staged++] = entry;
					continue;
				}

				/*
				 * NEVER-MISREAD GUARD (R2): staged rows with no capture path
				 * = silent row loss.
				 */
				if (entry->staged_rows > 0 &&
					(entry->write_mode != LAKEHOUSE_WRITE_MODE_COW ||
					 entry->ws == NULL))
					ereport(ERROR,
							(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							 errmsg("lakehouse: %d staged rows cannot be committed"
									" (write mode %d, no remote session)",
									entry->staged_rows, entry->write_mode),
							 errdetail("This is a lakehouse bug, not an operator"
									   " error — the rows would be silently"
									   " lost.")));
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
					 * DELETED entries. Reading the CURRENT snapshot here
					 * would be a lost-update race — a concurrent writer may
					 * have already replaced these files; the commit's
					 * liveness check catches that and aborts.
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

									/*
									 * projection column = ordinal among
									 * visible attrs
									 */
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

					/*
					 * push the SURVIVING staged rows (post subxact pruning)
					 * into the accumulator now, then flush + commit
					 */
					for (i = 0; i < entry->staged_rows; i++)
					{
						int			k;

						initStringInfo(&row);
						appendStringInfoChar(&row, '[');
						for (k = 0; k < tupdesc->natts; k++)
						{
							/*
							 * Omit dropped attributes from the schema and row
							 * so the cell and column counts match (13-review
							 * #8b).
							 */
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
						/*
						 * overwrite commit (slice 28): staged/rewritten files
						 * ADDED, the tombstoned originals DELETED entries
						 */
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
							escape_json(&del, tf->path);	/* emits its own quotes */
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
								(errcode(lakehouse_commit_errcode(entry)),
								 errmsg("lakehouse: managed commit failed for %s: %s",
										get_rel_name(entry->relid) ? get_rel_name(entry->relid) : "?",
										errbuf)));
					pg_datalake_write_commit_result_free(loc);
				}
			}

			/*
			 * P3 phase 2: capture the staged entries COLLECTED during the seq
			 * iteration (the capture's own SPI runs hash operations and
			 * re-yielded the same entry per row — the live-proven per-row
			 * capture bug). The seq search has COMPLETED here, so SPI is
			 * safe. COW entries were handled inline above.
			 */
			{
				int			ci;

				for (ci = 0; ci < n_staged; ci++)
					staged_capture_segment(staged_list[ci]);
			}
		}						/* staged_list collection scope */
	}

	/*
	 * teardown: free FFI sessions + the hash. PRE_COMMIT's commit errors
	 * leave the state intact so the ABORT callback reaps it.
	 */
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

/*
 * projection for pg_datalake_rows_open_path: counted JSON array of the
 * relation's visible attribute names (same shape as the scan projection).
 */
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

/*
 * input-function plumbing for the PRE-COMMIT rewrite pass: surviving rows
 * read from a base file are converted to datums and re-encoded exactly
 * like staged rows (one code path for every cell type).
 */
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
char *
lakehouse_build_schema_json(TupleDesc tupdesc)
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

			/*
			 * name Datums are NameData*, NOT varlena text — routing them
			 * through textout read garbage (13-review #8b)
			 */
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
				/*
				 * hex-encode DIRECTLY: byteaout follows the bytea_output GUC
				 * (escape vs hex), which would silently corrupt stored bytes
				 * under bytea_output=escape (13-review #8b)
				 */
				bytea	   *b = DatumGetByteaPP(v);
				const unsigned char *raw = (const unsigned char *) VARDATA_ANY(b);
				int			blen = (int) VARSIZE_ANY_EXHDR(b);
				int			j;

				appendStringInfoString(buf, "{\"b\": \"\\\\x");
				for (j = 0; j < blen; j++)
					appendStringInfo(buf, "%02x", raw[j]);
				appendStringInfoString(buf, "\"}");
				break;
			}
		default:
			/* Schema validation already rejected this type. */
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
	char		blob[FLEXIBLE_ARRAY_MEMBER];	/* "table\0endpoint\0warehouse\0s3_endpoint\0" */
} LakehouseRelOptions;

static const char *lakehouse_relopt_keys[] = {
	"table", "endpoint", "warehouse", "s3_endpoint", "write_mode",
};
#define LAKEHOUSE_NUM_RELOPTS 5

/*
 * credentials (token, s3_access_key, s3_secret_key) are deliberately NOT
 * table options: they live in the attach row / secure settings (doc 09)
 * and must never enter pg_class.reloptions, which pg_dump exposes.
 */

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
								 "s3_endpoint, write_mode (credentials come "
								 "from the attach row or secure settings, "
								 "never table options).")));
		}
	}

	/*
	 * write_mode alone is legal (R2: mode can be set at CREATE, identity
	 * attached later via lakehouse.attach_table)
	 */
	if ((values[1] != NULL || values[2] != NULL || values[3] != NULL) &&
		values[0] == NULL)
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

	std_len = std != NULL ? (int32) VARSIZE(std) : (int32) (offsetof(StdRdOptions, autovacuum) + sizeof(AutoVacOpts));
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
	{
		/*
		 * bound the walk by the BLOB LENGTH: legacy blobs hold fewer keys
		 * than LAKEHOUSE_NUM_RELOPTS, and reading past the last NUL hit
		 * garbage bytes that (a) errored loudly as an invalid write_mode and
		 * (b) aborted every statement touching the table (CI regression, R2)
		 */
		char	   *blob_end = ro->blob + ro->blob_len;

		for (i = 0; i < LAKEHOUSE_NUM_RELOPTS && p < blob_end; i++)
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
				case 4:			/* write_mode: int, not a char* field */
					if (p > start)
					{
						/*
						 * empty slot = unset (blobs written before R2 or
						 * CREATEs without the option) — only a NON-empty
						 * unrecognized value is a loud error
						 */
						if (strcmp(start, "staged") == 0)
							opts->write_mode = LAKEHOUSE_WRITE_MODE_STAGED;
						else if (strcmp(start, "cow") == 0)
							opts->write_mode = LAKEHOUSE_WRITE_MODE_COW;
						else
							ereport(ERROR,
									(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
									 errmsg("invalid write_mode \"%s\" (cow|staged)",
											start)));
					}
					break;
				default:
					break;
			}
			if (p > start && i != 4)
				*field = pnstrdup(start, p - start);
			p++;				/* skip the NUL */
		}
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

/* A read must not create transaction-local write state. */
LakehouseWriteEntry *
lakehouse_find_write_entry(Oid relid)
{
	if (write_entries == NULL)
		return NULL;
	return hash_search(write_entries, &relid, HASH_FIND, NULL);
}

LakehouseWriteEntry *
get_write_entry(Relation rel)
{
	LakehouseWriteEntry *entry;
	bool		found;
	Oid			relid = RelationGetRelid(rel);
	pg_uuid_t	binding_id;
	bool		bound;

	entry = lakehouse_find_write_entry(relid);
	if (entry != NULL)
	{
		/*
		 * An aborted savepoint can release the original relation lock and
		 * discard its rows. Recheck before reusing the empty entry.
		 */
		if (entry->staged_rows == 0)
		{
			bound = lookup_capture_binding(relid, &binding_id);
			if (entry->bound_capture != bound ||
				(bound && memcmp(&entry->binding_id, &binding_id, sizeof(binding_id)) != 0))
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("managed capture binding changed within the transaction")));
		}
		return entry;
	}

	/*
	 * A rejected enrollment must not leave an entry behind after savepoint
	 * abort.
	 */
	bound = lookup_capture_binding(relid, &binding_id);
	if (bound)
		pfree(lakehouse_build_schema_json(RelationGetDescr(rel)));
	if (write_entries == NULL)
	{
		HASHCTL		ctl;

		/*
		 * HASH_ELEM entry structs overlay the key (see the hash-key aliasing
		 * warning above LakehouseWriteEntry): keysize bytes at offset 0 of
		 * LakehouseWriteEntry ARE the stored key. relid is deliberately the
		 * struct's first member — keep it that way.
		 */
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(LakehouseWriteEntry);
		write_entries = hash_create("lakehouse managed write state", 8, &ctl,
									HASH_ELEM | HASH_BLOBS);
	}
	entry = (LakehouseWriteEntry *) hash_search(write_entries, &relid,
												HASH_ENTER, &found);
	if (!found)
	{
		/*
		 * Init a freshly minted entry. dynahash has ALREADY copied the key
		 * into entry->relid — do not (re)write it here; see the hash-key
		 * aliasing warning on the struct.
		 */
		entry->ws = NULL;
		entry->mcxt = AllocSetContextCreate(TopTransactionContext,
											"lakehouse managed write",

		/*
		 * ALLOCSET_DEFAULT_SIZES, with explicit Size casts: clang-tidy flags
		 * the upstream macros' int-internal multiplication
		 */
											(Size) ALLOCSET_DEFAULT_MINSIZE,
											(Size) ALLOCSET_DEFAULT_INITSIZE,
											(Size) ALLOCSET_DEFAULT_MAXSIZE);
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

		/*
		 * WITH-options identity wins; attach row still supplies the
		 * credentials (they never live in reloptions — doc 09)
		 */
		entry->have_identity = decode_lakehouse_reloptions(rel, &entry->opts);

		/*
		 * R2: WITH (write_mode=...) sticks over the session GUC — resolved
		 * on EVERY identity path (attach row included), not just the
		 * WITH-options one
		 */
		{
			/*
			 * R2: a TABLE-LEVEL write_mode sticks over the session GUC —
			 * resolved on EVERY reloption path, NOT just the ones that also
			 * carry identity keys. decode_lakehouse_reloptions returns "has
			 * identity" (opts->table != NULL), which is the WRONG gate: a
			 * blob holding write_mode alone (CREATE-time write_mode without
			 * identity — legal per the validator; ALTER TABLE SET is its
			 * post-create form) was silently ignored and the session GUC won.
			 * Discriminate on wm_opts.write_mode instead. decode skips its
			 * memset on the early-return paths (no rd_options, foreign
			 * layout), so zero the struct here first.
			 */
			LakehouseOptions wm_opts;

			memset(&wm_opts, 0, sizeof(wm_opts));
			decode_lakehouse_reloptions(rel, &wm_opts);
			entry->write_mode = (wm_opts.write_mode != 0)
				? wm_opts.write_mode
				: lakehouse_write_mode;
		}
		entry->bound_capture = bound;
		if (bound)
		{
			entry->binding_id = binding_id;
			entry->write_mode = LAKEHOUSE_WRITE_MODE_STAGED;
			entry->checked_layout_cid = InvalidCommandId;
		}
		else if (ExtractManagedIdentity(relid, &entry->opts))
			entry->have_identity = true;
		if (!bound && !entry->have_identity)
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
		 * tupdesc/opts allocated in the calling context were freed memory by
		 * the time PRE-COMMIT read them (intermittent garbage attribute names
		 * -> garbage palloc sizes / crashes, caught by valgrind as reads of
		 * an ExecutorEnd-freed block).
		 */
		{
			MemoryContext old = MemoryContextSwitchTo(entry->mcxt);

			entry->tupdesc = CreateTupleDescCopyConstr(RelationGetDescr(rel));
			entry->natts = entry->tupdesc->natts;

			/*
			 * re-copy the identity strings into the txn mcxt (decode helpers
			 * pstrdup into the caller's context)
			 */
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

PG_FUNCTION_INFO_V1(lakehouse_capture_check_legacy_write);

Datum
lakehouse_capture_check_legacy_write(PG_FUNCTION_ARGS)
{
	LakehouseWriteEntry *entry = lakehouse_find_write_entry(PG_GETARG_OID(0));

	/* The operator already holds AccessExclusiveLock on the target. */
	if (entry != NULL && entry->ws != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot require bound capture in a transaction with a legacy write session")));
	PG_RETURN_VOID();
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
				repalloc((void *) entry->tomb_files,
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

/*
 * Drop registry entries created above the aborted subtransaction —
 * their scan descs die with the aborted executor context.
 */
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

/*
 * One row of lakehouse.admit_managed_read: the operator-approved route,
 * table incarnation and enrollment anchor for the checked role. Strings
 * are copied into the scan context; the fresh-proof frame rechecks the
 * operator's grant directly under an explicit fresh snapshot (the
 * function's VOLATILE body cannot honor a pushed snapshot).
 */
typedef struct ManagedReadAdmission
{
	pg_uuid_t	binding;
	Oid			role;
	Oid			mapping;
	Oid			server;
	pg_uuid_t	incarnation;
	pg_uuid_t	authority;
	pg_uuid_t	table_uuid;
	int64		anchor;
	int32		schema_id;
	int32		spec_id;
	char	   *target_identity;
	char	   *storage_root;
	char	   *catalog_endpoint;
	char	   *catalog_prefix;
	char	   *s3_region;
	char	   *s3_endpoint;
	char	   *mapping_fingerprint;
} ManagedReadAdmission;

typedef struct LakehouseTableScanDescData
{
	TableScanDescData rs_base;
	LakehouseOptions opts;
	ScanSession *session;
	RowsHandle *rows;
	MemoryContextCallback cleanup;
	uint64		current_task;
	uint64		task_count;
	char	   *projection_csv;
	int			natts;
	FmgrInfo   *in_funcs;
	Oid		   *typioparams;
	int		   *slot_index;

	/*
	 * own-writes overlay: staged rows of THIS txn (entry == NULL when the
	 * relation has no open write state)
	 */
	LakehouseWriteEntry *entry;
	uint64		staged_pos;

	/*
	 * COW tids (slice 28): 0-based row index within the current task, and the
	 * task's file path (session-owned CString pointer, stable) for tombstone
	 * filtering. M3-R1: the identity is the file-absolute row index r from
	 * the batch live list (equal to the live-row ordinal for managed tables,
	 * which carry no delete files).
	 */
	uint64		row_in_task;
	const char *cur_task_path;
	/* M3-R1 typed batch cursor (no per-row renderer on this path) */
	PgDatalakeRowBatch batch;
	bool		batch_valid;
	uint64		batch_cursor;
	LakehouseBatchMap *maps;	/* per visible attribute */

	/*
	 * staged-delta read side (P3 core, D1-rev): committed unpublished
	 * segments of this table, loaded once per scan via SPI
	 */
	DeltaRowsHandle **delta_rows;
	int			n_delta;
	int			delta_pos;
	bool		delta_loaded;

	/*
	 * managed cut (03-managed-visibility, "SQL executor acceptance"): the
	 * demand-time compatible-pin cut for bound capture tables. Admission runs
	 * at scan_begin (catalog-local declines, zero origin requests); the cut
	 * itself is observed and planned on the first getnextslot, so every
	 * failure precedes the first served row.
	 */
	bool		capture_bound;
	bool		cut_installed;
	bool		cut_proved;
	bool		for_count;		/* metadata-only COUNT: skip payload loads */
	int64		cut_count;		/* for_count: task rows + overlay row_count */
	int64		cut_pin;
	int64	   *cut_overlay_ids;
	int			n_cut_overlay;
	int64	   *overlay_row_counts; /* per visible segment (statement frame) */
	int			n_overlay_row_counts;
	struct ManagedReadObservation *read_observation;
	ManagedReadAdmission admission;
} LakehouseTableScanDescData;
typedef LakehouseTableScanDescData *LakehouseTableScanDesc;

static void load_delta_segments(LakehouseTableScanDesc scan, TupleDesc tupdesc);
static void lakehouse_admit_managed_cut(LakehouseTableScanDesc scan);
static void install_managed_cut(LakehouseTableScanDesc scan, TupleDesc tupdesc);

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

	/*
	 * projection: visible attribute names, in attribute order — a counted
	 * JSON ARRAY, not comma-separated: column names may contain commas
	 * (13-review #8b)
	 */
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

	/*
	 * R2 staged bypass: a table-level WITH (write_mode='staged') makes this a
	 * delta-only scan — no remote session, zero tasks. Decoded into a
	 * LOCAL: opts here carries the server-derived endpoint/ warehouse that
	 * OpenSession needs. Same gate as the write path: discriminate on
	 * wm_opts.write_mode alone (see get_write_entry) — keying on decode's
	 * "has identity" return silently ignored a write_mode-only blob. decode
	 * skips its memset on early returns (no rd_options, foreign layout), so
	 * zero the struct here first.
	 */
	{
		LakehouseOptions wm_opts;
		int			wm;

		memset(&wm_opts, 0, sizeof(wm_opts));
		decode_lakehouse_reloptions(rel, &wm_opts);
		wm = (wm_opts.write_mode != 0)
			? wm_opts.write_mode
			: lakehouse_write_mode;

		if (scan->capture_bound)
		{
			if (wm != LAKEHOUSE_WRITE_MODE_STAGED)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("bound managed capture scans require the staged write mode")));

			/*
			 * Managed-cut bypass: no session and no tasks until the
			 * demand-time cut plans the compatible pin exactly. The cut's
			 * overlay serves through the staged delta loop (the identity
			 * decode above was skipped for bound tables).
			 */
			scan->opts.write_mode = LAKEHOUSE_WRITE_MODE_STAGED;
			scan->session = NULL;
			scan->task_count = 0;
			return;
		}

		if (wm == LAKEHOUSE_WRITE_MODE_STAGED)
		{
			scan->session = NULL;
			scan->task_count = 0;
			return;
		}
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

/* Also runs when an ERROR discards the executor context before EndScan. */
static void
lakehouse_scan_release(void *arg)
{
	LakehouseTableScanDesc scan = (LakehouseTableScanDesc) arg;

	if (scan->read_observation != NULL)
		pg_datalake_managed_read_observation_drop(&scan->read_observation);
	pg_datalake_rows_close(scan->rows);
	pg_datalake_scan_free(scan->session);
	for (int i = 0; i < scan->n_delta; i++)
		pg_datalake_delta_rows_close(scan->delta_rows[i]);
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
	scan->cleanup.func = lakehouse_scan_release;
	scan->cleanup.arg = scan;
	MemoryContextRegisterResetCallback(GetMemoryChunkContext(scan), &scan->cleanup);

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
		pg_uuid_t	binding;

		/*
		 * Bound managed capture (03-managed-visibility, "Admission before the
		 * native cut"): supported-shape declines are catalog-local — a
		 * declined scan makes zero origin requests. Ordinary bound SQL was
		 * refused outright before the managed cut existed. The bound path
		 * also skips the attached-identity decode below: its route and
		 * credentials come from the admitted purpose=read mapping, never from
		 * the read server of the querying user.
		 */
		if (lookup_capture_binding(RelationGetRelid(rel), &binding))
		{
			if (has_subclass(RelationGetRelid(rel)))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("bound managed capture does not support inheritance scans")));
			if (check_enable_rls(RelationGetRelid(rel), GetUserId(), false) != RLS_NONE)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("bound managed capture does not support row-level security scans")));
			if (lakehouse_find_write_entry(RelationGetRelid(rel)) != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("bound managed capture does not support scans with staged writes")));
			scan->capture_bound = true;
			lakehouse_admit_managed_cut(scan);
		}
	}

	if (!scan->capture_bound)
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
	scan->entry = lakehouse_find_write_entry(RelationGetRelid(rel));
	register_scan_for_dml(RelationGetRelid(rel), scan);
	return (TableScanDesc) scan;
}

static void
lakehouse_scan_end(TableScanDesc bscan)
{
	LakehouseTableScanDesc scan = (LakehouseTableScanDesc) bscan;

	MemoryContextUnregisterResetCallback(GetMemoryChunkContext(scan), &scan->cleanup);
	lakehouse_scan_release(scan);
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

	/*
	 * Rewind the existing scan without resolving another Iceberg pin or
	 * reloading the staged payloads from the ledger.
	 */
	pg_datalake_rows_close(scan->rows);
	scan->rows = NULL;
	scan->current_task = 0;
	scan->row_in_task = 0;
	scan->cur_task_path = NULL;
	scan->batch_valid = false;
	scan->batch_cursor = 0;
	scan->staged_pos = 0;
	scan->delta_pos = 0;
	for (int i = 0; i < scan->n_delta; i++)
		pg_datalake_delta_rows_rewind(scan->delta_rows[i]);
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

	/*
	 * Demand-time managed cut: EXPLAIN and LIMIT 0 never reach this, and
	 * every install failure precedes the first served row. Rescans and
	 * portals keep the installed cut (zero re-admission, zero catalog
	 * resolutions).
	 */
	if (scan->capture_bound && !scan->cut_installed)
		install_managed_cut(scan, tupdesc);

	for (;;)
	{
		if (scan->rows == NULL)
		{
			char		errbuf[1024];
			const char *file;

			/*
			 * own-writes overlay (doc 07 §2): after the snapshot's files
			 * drain, yield the rows this transaction staged but has not
			 * committed yet. This runs on EVERY exhausted call (not once): a
			 * tombstone skip can drain the last task mid-loop, and the staged
			 * rows must still be served.
			 */
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

				/*
				 * staged mode: committed unpublished delta segments next
				 * (MVCC visibility via the SPI snapshot)
				 */
				if (lakehouse_write_mode == LAKEHOUSE_WRITE_MODE_STAGED ||
					scan->opts.write_mode == LAKEHOUSE_WRITE_MODE_STAGED)
				{
					char		delta_err[1024];

					if (!scan->delta_loaded)
						load_delta_segments(scan, tupdesc);
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
		 * arrow batch; Datums are built directly (batch_fill.c). The explicit
		 * ROW/EOF/ERROR tri-state carries over: EOF advances the task, ERROR
		 * aborts the scan (13-review #8 — a caught panic is never mistaken
		 * for EOF).
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
				/*
				 * lr: batch-LOCAL index into the typed buffers; r:
				 * file-ABSOLUTE row index — the tombstone/TID identity
				 * (slice 28). Conflating the two silently retargeted
				 * tombstones at the wrong rows in every batch after the first
				 * (caught by the M3-R1 live DML verification).
				 */
				uint32		lr = scan->batch.live_idx[scan->batch_cursor];
				uint64		r = scan->batch.start_abs + lr;

				/*
				 * COW read-your-deletes: rows tombstoned earlier in THIS
				 * transaction are filtered here, exactly like the rewrite
				 * pass filters them at PRE-COMMIT. r is the file-absolute row
				 * index (the tombstone/TID identity, slice 28).
				 */
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

				/*
				 * COW tid (slice 28): (task, rowidx) — the identity
				 * tuple_delete/tuple_update decode back into the base
				 * snapshot's file + row position
				 */
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


/*
 * Open the FFI write session lazily (first insert or first tombstone):
 * validates identity/credentials early, fail-fast per doc 03 §5.
 */
void
open_write_session(LakehouseWriteEntry *entry, TupleDesc tupdesc)
{
	char		errbuf[1024];

	if (entry->bound_capture)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("bound managed capture only supports staged inserts")));
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
									  lakehouse_build_schema_json(tupdesc),
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
	Oid			relid = RelationGetRelid(scan->rs_base.rs_rd);
	char	   *schema_json = lakehouse_build_schema_json(tupdesc);
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
	if (ret == SPI_OK_SELECT && SPI_processed > 0)
	{
		scan->delta_rows =
			MemoryContextAllocZero(GetMemoryChunkContext(scan),
								   mul_size(sizeof(DeltaRowsHandle *), SPI_processed));
		for (i = 0; i < SPI_processed; i++)
		{
			bool		isnull;
			Datum		d = SPI_getbinval(SPI_tuptable->vals[i],
										  SPI_tuptable->tupdesc, 1, &isnull);
			DeltaRowsHandle *rows;

			if (isnull)
				continue;
			rows = pg_datalake_delta_rows_open(TextDatumGetCString(d),
											   schema_json, NULL, 0);
			if (rows != NULL)
				scan->delta_rows[scan->n_delta++] = rows;
		}
	}
	PopActiveSnapshot();
	SPI_finish();
}

 /* ---------------- managed cut (03-managed-visibility) ---------------- */

#define MANAGED_READ_MAX_PINS 512
#define MANAGED_READ_MAX_MEMBERS 16384
#define MANAGED_READ_MAX_OVERLAY 16384

/* Backend-local evidence of the last installed cut (one line of JSON). */
static char *managed_cut_evidence = NULL;

static void
managed_read_status_error(enum PgDatalakeReadStatus status, const char *errbuf,
						  const char *summary)
{
	int			errcode_val = ERRCODE_FEATURE_NOT_SUPPORTED;

	if (status == ReadChangedBinding || status == ReadPinUnavailable)
		errcode_val = ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE;
	ereport(ERROR,
			(errcode(errcode_val),
			 errmsg("%s", summary),
			 errdetail("%s", errbuf[0] != '\0' ? errbuf : "the enrollment refused the request")));
}

static char *
managed_cred_string(const PgDatalakeCredentialBytes *bytes)
{
	char	   *value;

	if (bytes->data == NULL || bytes->len == 0)
		return NULL;
	value = palloc(bytes->len + 1);
	memcpy(value, bytes->data, bytes->len);
	value[bytes->len] = '\0';
	return value;
}

/* Re-home the string fields of one admission row into `ctx`. */
static void
managed_read_admission_copy_strings(ManagedReadAdmission *a, MemoryContext ctx)
{
	MemoryContext old = MemoryContextSwitchTo(ctx);

	if (a->target_identity)
		a->target_identity = pstrdup(a->target_identity);
	if (a->storage_root)
		a->storage_root = pstrdup(a->storage_root);
	if (a->catalog_endpoint)
		a->catalog_endpoint = pstrdup(a->catalog_endpoint);
	if (a->catalog_prefix)
		a->catalog_prefix = pstrdup(a->catalog_prefix);
	if (a->s3_region)
		a->s3_region = pstrdup(a->s3_region);
	if (a->s3_endpoint)
		a->s3_endpoint = pstrdup(a->s3_endpoint);
	if (a->mapping_fingerprint)
		a->mapping_fingerprint = pstrdup(a->mapping_fingerprint);
	MemoryContextSwitchTo(old);
}

/* Read one row of lakehouse.admit_managed_read into caller-owned memory. */
static void
managed_read_admission_fill(ManagedReadAdmission *a, HeapTuple tuple,
							TupleDesc desc)
{
	bool		isnull;
	Datum		val;
	bytea	   *raw;

	memset(a, 0, sizeof(*a));
	val = SPI_getbinval(tuple, desc, 1, &isnull);
	if (isnull)
		elog(ERROR, "managed read admission lost its binding");
	memcpy(&a->binding, DatumGetUUIDP(val), sizeof(pg_uuid_t));
	val = SPI_getbinval(tuple, desc, 2, &isnull);
	a->role = DatumGetObjectId(val);
	val = SPI_getbinval(tuple, desc, 3, &isnull);
	a->mapping = DatumGetObjectId(val);
	val = SPI_getbinval(tuple, desc, 4, &isnull);
	a->server = DatumGetObjectId(val);
	val = SPI_getbinval(tuple, desc, 6, &isnull);
	memcpy(&a->incarnation, DatumGetUUIDP(val), sizeof(pg_uuid_t));
	val = SPI_getbinval(tuple, desc, 7, &isnull);
	memcpy(&a->authority, DatumGetUUIDP(val), sizeof(pg_uuid_t));
	val = SPI_getbinval(tuple, desc, 8, &isnull);
	memcpy(&a->table_uuid, DatumGetUUIDP(val), sizeof(pg_uuid_t));
	val = SPI_getbinval(tuple, desc, 11, &isnull);
	a->anchor = DatumGetInt64(val);
	val = SPI_getbinval(tuple, desc, 12, &isnull);
	a->schema_id = DatumGetInt32(val);
	val = SPI_getbinval(tuple, desc, 13, &isnull);
	a->spec_id = DatumGetInt32(val);
	val = SPI_getbinval(tuple, desc, 14, &isnull);
	raw = isnull ? NULL : DatumGetByteaPP(val);
	if (raw == NULL)
		elog(ERROR, "managed read admission lost its identity");
	a->target_identity = pnstrdup(VARDATA_ANY(raw), VARSIZE_ANY_EXHDR(raw));
	val = SPI_getbinval(tuple, desc, 10, &isnull);
	raw = isnull ? NULL : DatumGetByteaPP(val);
	a->storage_root = raw == NULL ? NULL : pnstrdup(VARDATA_ANY(raw), VARSIZE_ANY_EXHDR(raw));
	val = SPI_getbinval(tuple, desc, 15, &isnull);
	raw = isnull ? NULL : DatumGetByteaPP(val);
	a->catalog_endpoint = raw == NULL ? NULL : pnstrdup(VARDATA_ANY(raw), VARSIZE_ANY_EXHDR(raw));
	val = SPI_getbinval(tuple, desc, 16, &isnull);
	raw = isnull ? NULL : DatumGetByteaPP(val);
	a->catalog_prefix = raw == NULL ? NULL : pnstrdup(VARDATA_ANY(raw), VARSIZE_ANY_EXHDR(raw));
	val = SPI_getbinval(tuple, desc, 17, &isnull);
	raw = isnull ? NULL : DatumGetByteaPP(val);
	a->s3_region = raw == NULL ? NULL : pnstrdup(VARDATA_ANY(raw), VARSIZE_ANY_EXHDR(raw));
	val = SPI_getbinval(tuple, desc, 18, &isnull);
	raw = isnull ? NULL : DatumGetByteaPP(val);
	a->s3_endpoint = raw == NULL ? NULL : pnstrdup(VARDATA_ANY(raw), VARSIZE_ANY_EXHDR(raw));
	val = SPI_getbinval(tuple, desc, 19, &isnull);
	raw = isnull ? NULL : DatumGetByteaPP(val);
	a->mapping_fingerprint = raw == NULL ? NULL : pnstrdup(VARDATA_ANY(raw), VARSIZE_ANY_EXHDR(raw));
}

/*
 * Executor admission (plan step 4): run lakehouse.admit_managed_read under
 * the ledger owner's security context for the CHECKED role. Zero rows
 * declines catalog-locally with the historical 0A000 message, so the
 * pre-grant baseline keeps failing exactly as before. No origin contact.
 */
static void
lakehouse_admit_managed_cut(LakehouseTableScanDesc scan)
{
	Oid			relid = RelationGetRelid(scan->rs_base.rs_rd);
	Oid			reader = GetUserId();
	Oid			owner = capture_table_owner("managed_capture_state", true);
	Oid			user;
	int			sec_context;
	MemoryContext scan_context = GetMemoryChunkContext(scan);
	bool		admitted = false;

	GetUserIdAndSecContext(&user, &sec_context);
	SPI_connect();
	PushActiveSnapshot(GetLatestSnapshot());
	PG_TRY();
	{
		Oid			argtypes[2] = {OIDOID, OIDOID};
		Datum		args[2] = {ObjectIdGetDatum(relid), ObjectIdGetDatum(reader)};
		bool		isnull;
		int			ret;

		SetUserIdAndSecContext(owner, sec_context | SECURITY_LOCAL_USERID_CHANGE |
							   SECURITY_RESTRICTED_OPERATION);

		/*
		 * Extensions below 1.19 have no admission function yet: an unadmitted
		 * scan keeps the historical 0A000 baseline instead of failing on the
		 * missing function (the capture-binding fixture runs the 1.6/1.7
		 * chain).
		 */
		ret = SPI_execute("SELECT to_regprocedure('lakehouse.admit_managed_read(regclass,regrole)')"
						  " IS NOT NULL", true, 1);
		if (ret != SPI_OK_SELECT || SPI_processed != 1 ||
			!DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1,
										&isnull)))
			admitted = false;
		else
		{
			ret = SPI_execute_with_args("SELECT * FROM lakehouse.admit_managed_read($1, $2)",
										2, argtypes, args, NULL, true, 1);
			if (ret != SPI_OK_SELECT)
				elog(ERROR, "could not read the managed read admission");
			if (SPI_processed == 1)
			{
				managed_read_admission_fill(&scan->admission,
											SPI_tuptable->vals[0],
											SPI_tuptable->tupdesc);
				/* strings die with the SPI context; re-home them first */
				managed_read_admission_copy_strings(&scan->admission, scan_context);
				admitted = true;
			}
		}
	}
	PG_FINALLY();
	{
		SetUserIdAndSecContext(user, sec_context);
	}
	PG_END_TRY();
	PopActiveSnapshot();
	SPI_finish();

	if (!admitted)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("bound managed capture does not support table scans")));
}

/*
 * Demand-time managed cut (plan step 5), the joined scan:
 *
 * 1. observe the enrolled catalog (the ONLY catalog contact) under the
 *    purpose=read credential of the admitted mapping;
 * 2. fresh-proof frame (owner switch, GetLatestSnapshot): re-run the
 *    admission and compare every field, read the certified addition chain,
 *    and prove an unknown pending head against this observation;
 * 3. statement frame (owner switch, the REGISTERED statement snapshot):
 *    visible segments and payload retention;
 * 4. select the compatible pin (pure computation);
 * 5. verify and load the overlay payloads under the same snapshot;
 * 6. plan the selected pin exactly (consumes the observation).
 *
 * Runs before the first getnextslot row: every failure below aborts the
 * statement with zero rows served.
 */
static void
install_managed_cut(LakehouseTableScanDesc scan, TupleDesc tupdesc)
{
	Oid			relid = RelationGetRelid(scan->rs_base.rs_rd);
	MemoryContext scan_context = GetMemoryChunkContext(scan);
	struct ManagedReadObservation *observation;
	PgDatalakePublicationCredentialMaterial material;
	PgDatalakeManagedReadRequest request;
	PgDatalakeReadStatus status;
	char		errbuf[1024];
	char	   *catalog_token;
	char	   *s3_access_key;
	char	   *s3_secret_key;
	char	   *namespace_name;
	char	   *table_name;
	char	   *schema_json;
	char	   *dot;
	int64	   *pins;
	int64	  **pin_members;
	int		   *pin_member_counts;
	bool	   *pin_known;
	int			pin_count;
	int64	   *visible_ids = NULL;
	bool	   *visible_retained = NULL;
	int			n_visible = 0;
	int64	   *overlay_ids = NULL;
	int			n_overlay = 0;
	int64		cut_pin = 0;
	bool		has_pending = false;
	Snapshot	fresh_snapshot = NULL;
	pg_uuid_t	pending_operation;
	uint8		pending_input_digest[32];
	uint8		pending_proposal_digest[32];
	int64		pending_expected_parent = 0;
	int64		pending_snapshot = 0;
	char	   *pending_body = NULL;
	int			pending_body_len = 0;
	int64	   *pending_seg_ids = NULL;
	int32	   *pending_row_counts = NULL;
	uint8	   *pending_digests = NULL;
	int			pending_member_count = 0;
	int			proof_edge = -1;
	int			i;

	/* everything this routine builds lives and dies with the scan */
	MemoryContextSwitchTo(scan_context);
	scan->cut_installed = true; /* never attempt twice, even on error paths */

	/* ---- 0. statement-scope preflight (zero origin contact) ---- */

	/*
	 * Acquire the ORIGINAL-snapshot scope before any catalog access: a reader
	 * whose statement snapshot predates a binding rotation has no scope for
	 * the admitted binding and must fail with the consistency error before
	 * the observation makes a single request.
	 */
	{
		Oid			owner = capture_table_owner("managed_capture_state", true);
		Oid			user;
		int			sec_context;

		GetUserIdAndSecContext(&user, &sec_context);
		SPI_connect();
		PushActiveSnapshot(scan->rs_base.rs_snapshot);
		PG_TRY();
		{
			bool		isnull;
			Oid			argtypes[2] = {OIDOID, UUIDOID};
			Datum		args[2] = {ObjectIdGetDatum(relid),
				UUIDPGetDatum(&scan->admission.binding)
			};
			int			ret;

			SetUserIdAndSecContext(owner, sec_context | SECURITY_LOCAL_USERID_CHANGE |
								   SECURITY_RESTRICTED_OPERATION);
			{
				SPIPlanPtr	plan = SPI_prepare(
											   "SELECT EXISTS (SELECT FROM lakehouse.get_managed_read_scope($1, $2))",
											   2, argtypes);

				if (plan == NULL)
					elog(ERROR, "could not prepare the managed read scope probe");

				/* the ORIGINAL snapshot must recognize the admitted scope... */
				ret = SPI_execute_snapshot(plan, args, NULL,
										   scan->rs_base.rs_snapshot, InvalidSnapshot,
										   true, false, 1);
				if (ret != SPI_OK_SELECT || SPI_processed != 1 ||
					!DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],
												SPI_tuptable->tupdesc, 1,
												&isnull)) || isnull)
					ereport(ERROR,
							(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							 errmsg("managed read anchor is unavailable or its scope changed")));

				/*
				 * ... and the CURRENT one: the enrollment the cut will
				 * observe must still be the state's active binding as of now,
				 * or the scope changed under the held snapshot.
				 */
				ret = SPI_execute_snapshot(plan, args, NULL,
										   GetLatestSnapshot(), InvalidSnapshot,
										   true, false, 1);
				if (ret != SPI_OK_SELECT || SPI_processed != 1 ||
					!DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],
												SPI_tuptable->tupdesc, 1,
												&isnull)) || isnull)
					ereport(ERROR,
							(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							 errmsg("managed read anchor is unavailable or its scope changed")));
			}
		}
		PG_FINALLY();
		{
			SetUserIdAndSecContext(user, sec_context);
		}
		PG_END_TRY();
		PopActiveSnapshot();
		SPI_finish();
	}

	/* ---- 1. observe ---- */
	lakehouse_load_publication_material(&material, scan->admission.role,
										scan->admission.server,
										scan->admission.mapping,
										"read", scan_context);
	catalog_token = managed_cred_string(&material.catalog_token);
	s3_access_key = managed_cred_string(&material.access_key);
	s3_secret_key = managed_cred_string(&material.secret_key);

	dot = strrchr(scan->admission.target_identity, '.');
	if (dot == NULL || dot == scan->admission.target_identity ||
		dot[1] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("managed read admission carries a malformed table identity"),
				 errdetail("expected <namespace>.<table>, got \"%s\"",
						   scan->admission.target_identity)));
	namespace_name = pnstrdup(scan->admission.target_identity,
							  dot - scan->admission.target_identity);
	table_name = pstrdup(dot + 1);

	memset(&request, 0, sizeof(request));
	request.binding.expected_table_uuid =
		DatumGetCString(DirectFunctionCall1(uuid_out,
											UUIDPGetDatum(&scan->admission.table_uuid)));
	request.binding.storage_root = scan->admission.storage_root;
	request.binding.catalog_prefix = scan->admission.catalog_prefix;
	request.binding.s3_region = scan->admission.s3_region;
	request.catalog_endpoint = scan->admission.catalog_endpoint;
	request.catalog_token = catalog_token;
	request.namespace_ = namespace_name;
	request.table = table_name;
	request.s3_endpoint = scan->admission.s3_endpoint;
	request.s3_access_key = s3_access_key;
	request.s3_secret_key = s3_secret_key;
	memcpy(request.authority_id, &scan->admission.authority, 16);
	memcpy(request.incarnation_id, &scan->admission.incarnation, 16);
	request.anchor_snapshot_id = scan->admission.anchor;
	request.enrolled_schema_id = scan->admission.schema_id;

	errbuf[0] = '\0';
	observation = pg_datalake_managed_read_observe(&request, &status,
												   errbuf, sizeof(errbuf));
	if (observation == NULL)
		managed_read_status_error(status, errbuf,
								  "managed read could not observe the enrolled catalog");
	scan->read_observation = observation;
	scan->cut_proved = false;

	pin_count = (int) pg_datalake_managed_read_pin_count(observation);
	if (pin_count < 1 || pin_count > MANAGED_READ_MAX_PINS)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("managed read certified ancestry is empty")));
	pins = palloc(sizeof(int64) * pin_count);
	pin_members = palloc0(sizeof(int64 *) * pin_count);
	pin_member_counts = palloc0(sizeof(int) * pin_count);
	pin_known = palloc0(sizeof(bool) * pin_count);
	for (i = 0; i < pin_count; i++)
	{
		pins[i] = pg_datalake_managed_read_pin_at(observation, i);
		if (pins[i] <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("managed read certified ancestry is malformed")));
	}

	/* ---- 2. fresh-proof frame ---- */
	{
		Oid			owner = capture_table_owner("managed_capture_state", true);
		Oid			reader = GetUserId();
		Oid			user;
		int			sec_context;

		GetUserIdAndSecContext(&user, &sec_context);
		SPI_connect();
		fresh_snapshot = RegisterSnapshot(GetLatestSnapshot());
		PG_TRY();
		{
			bool		recheck_null = false;
			Oid			argtypes[6];
			Datum		args[6];
			int			ret;

			SetUserIdAndSecContext(owner, sec_context | SECURITY_LOCAL_USERID_CHANGE |
								   SECURITY_RESTRICTED_OPERATION);

			/*
			 * admission recheck: the operator's read decision must still
			 * stand.  admit_managed_read is VOLATILE, so its body executes
			 * with GetTransactionSnapshot() (executor/functions.c) — in a
			 * REPEATABLE READ scan that is the held snapshot, which would
			 * keep a mid-window revocation invisible no matter which snapshot
			 * the caller pushes.  Query the grant row directly under the
			 * registered fresh snapshot instead: revocation, expiry, a
			 * swapped mapping, a narrowed prefix/region and a same-OID
			 * credential-option change (canonical jsonb fingerprint) all
			 * refuse here.  Binding rotation, lineage and shape drift are
			 * covered by the statement-scope preflight's dual scope probes.
			 */
			if (scan->admission.mapping_fingerprint == NULL ||
				scan->admission.catalog_prefix == NULL ||
				scan->admission.s3_region == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("managed read admission record is incomplete")));
			argtypes[0] = UUIDOID;
			argtypes[1] = OIDOID;
			argtypes[2] = OIDOID;
			argtypes[3] = TEXTOID;
			argtypes[4] = TEXTOID;
			argtypes[5] = TEXTOID;
			args[0] = UUIDPGetDatum(&scan->admission.binding);
			args[1] = ObjectIdGetDatum(reader);
			args[2] = ObjectIdGetDatum(scan->admission.mapping);
			args[3] = CStringGetTextDatum(scan->admission.mapping_fingerprint);
			args[4] = CStringGetTextDatum(scan->admission.catalog_prefix);
			args[5] = CStringGetTextDatum(scan->admission.s3_region);
			{
				SPIPlanPtr	plan = SPI_prepare(
											   "SELECT NOT g.revoked AND g.expires_at > clock_timestamp()"
											   " AND g.mapping_id = $3"
											   " AND g.catalog_prefix = $5"
											   " AND g.s3_region = $6"
											   " AND md5(coalesce(to_jsonb(m.umoptions)::text, 'null')) = $4"
											   " FROM lakehouse.managed_read_grants g"
											   " JOIN pg_user_mapping m ON m.oid = g.mapping_id"
											   "  AND m.umuser = g.reader_role"
											   " WHERE g.binding_id = $1 AND g.reader_role = $2",
											   6, argtypes);

				if (plan == NULL)
					elog(ERROR, "could not prepare the managed read recheck");
				ret = SPI_execute_snapshot(plan, args, NULL,
										   fresh_snapshot, InvalidSnapshot,
										   true, false, 1);
			}
			if (ret != SPI_OK_SELECT || SPI_processed != 1)
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("managed read admission changed under the scan")));
			if (!DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],
											SPI_tuptable->tupdesc, 1,
											&recheck_null)) || recheck_null)
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("managed read admission changed under the scan")));

			/* certified addition chain, newest-first in pins[] order */
			{
				int64	   *cumulative = palloc(sizeof(int64) * MANAGED_READ_MAX_MEMBERS);
				int			cumulative_len = 0;
				ArrayType  *pin_array = construct_array_builtin((Datum *) pins, pin_count,
																INT8OID);

				argtypes[0] = OIDOID;
				argtypes[1] = UUIDOID;
				argtypes[2] = INT8ARRAYOID;
				args[0] = ObjectIdGetDatum(relid);
				args[1] = UUIDPGetDatum(&scan->admission.binding);
				args[2] = PointerGetDatum(pin_array);
				{
					SPIPlanPtr	plan = SPI_prepare(
												   "SELECT snapshot_id, additions FROM"
												   " lakehouse.get_managed_read_additions($1, $2, $3)",
												   3, argtypes);

					if (plan == NULL)
						elog(ERROR, "could not prepare the managed read additions");
					ret = SPI_execute_snapshot(plan, args, NULL,
											   fresh_snapshot, InvalidSnapshot,
											   true, false, 0);
				}
				if (ret != SPI_OK_SELECT || SPI_processed != pin_count)
					elog(ERROR, "could not read the managed read additions");

				/*
				 * Rows return anchor-first; pins[] is newest-first. A NULL
				 * additions marks an unknown edge and every descendant.
				 */
				for (int r = 0; r < SPI_processed; r++)
				{
					int			p = pin_count - 1 - r;
					bool		isnull;
					Datum		additions = SPI_getbinval(SPI_tuptable->vals[r],
														  SPI_tuptable->tupdesc,
														  2, &isnull);

					if (!isnull)
					{
						Datum	   *ids;
						bool	   *nulls;
						int			n;

						deconstruct_array(DatumGetArrayTypeP(additions),
										  INT8OID, 8, FLOAT8PASSBYVAL, 'd',
										  &ids, &nulls, &n);
						for (int k = 0; k < n; k++)
						{
							int64		id = DatumGetInt64(ids[k]);

							if (nulls[k] || id <= 0 ||
								cumulative_len >= MANAGED_READ_MAX_MEMBERS)
								ereport(ERROR,
										(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
										 errmsg("managed read membership chain is malformed")));
							for (int c = 0; c < cumulative_len; c++)
								if (cumulative[c] == id)
									ereport(ERROR,
											(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
											 errmsg("managed read membership chain repeats a segment")));
							cumulative[cumulative_len++] = id;
						}
						pin_known[p] = true;
						pin_members[p] = MemoryContextAlloc(scan_context,
															sizeof(int64) * cumulative_len);
						memcpy(pin_members[p], cumulative,
							   sizeof(int64) * cumulative_len);
						pin_member_counts[p] = cumulative_len;
					}
				}
			}

			/*
			 * Prove the head edge only: the #88 verifier accepts a pending
			 * attempt AT the certified head (it resolves the attempt's
			 * snapshot against pins()[..2]). Once the provider head has moved
			 * past the child, the cut falls back to the retained parent with
			 * the full overlay — the recorded fallback shape.
			 */
			if (pin_count >= 2 && !pin_known[0] && pin_known[1])
				proof_edge = 0;
			if (proof_edge == 0)
			{
				argtypes[0] = OIDOID;
				argtypes[1] = UUIDOID;
				argtypes[2] = INT8OID;
				argtypes[3] = INT8OID;
				args[0] = ObjectIdGetDatum(relid);
				args[1] = UUIDPGetDatum(&scan->admission.binding);
				args[2] = Int64GetDatum(pins[proof_edge]);
				args[3] = Int64GetDatum(pins[proof_edge + 1]);
				{
					SPIPlanPtr	plan = SPI_prepare(
												   "SELECT (value->>'operation_id')::uuid,"
												   " decode(value->>'input_digest', 'hex'),"
												   " (value->>'expected_parent')::bigint,"
												   " (value->>'snapshot_id')::bigint,"
												   " decode(value->>'proposal_digest', 'hex'),"
												   " decode(value->>'proposal_body', 'hex'),"
												   " value->'members'"
												   " FROM (SELECT lakehouse.get_managed_read_pending_head($1, $2, $3, $4)"
												   " AS value) q",
												   4, argtypes);

					if (plan == NULL)
						elog(ERROR, "could not prepare the managed pending head");
					ret = SPI_execute_snapshot(plan, args, NULL,
											   fresh_snapshot, InvalidSnapshot,
											   true, false, 1);
				}
				if (ret != SPI_OK_SELECT)
					elog(ERROR, "could not read the managed pending head");
				if (SPI_processed == 1)
				{
					bool		isnull;
					Datum		val;
					bytea	   *raw;
					Datum		members;

					val = SPI_getbinval(SPI_tuptable->vals[0],
										SPI_tuptable->tupdesc, 1, &isnull);
					if (!isnull)
					{
						memcpy(&pending_operation, DatumGetUUIDP(val),
							   sizeof(pg_uuid_t));
						val = SPI_getbinval(SPI_tuptable->vals[0],
											SPI_tuptable->tupdesc, 2, &isnull);
						raw = isnull ? NULL : DatumGetByteaPP(val);
						if (raw == NULL || VARSIZE_ANY_EXHDR(raw) != 32)
							ereport(ERROR,
									(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
									 errmsg("managed pending head carries a malformed input digest")));
						memcpy(pending_input_digest, VARDATA_ANY(raw), 32);
						val = SPI_getbinval(SPI_tuptable->vals[0],
											SPI_tuptable->tupdesc, 3, &isnull);
						pending_expected_parent = DatumGetInt64(val);
						val = SPI_getbinval(SPI_tuptable->vals[0],
											SPI_tuptable->tupdesc, 4, &isnull);
						pending_snapshot = DatumGetInt64(val);
						val = SPI_getbinval(SPI_tuptable->vals[0],
											SPI_tuptable->tupdesc, 5, &isnull);
						raw = isnull ? NULL : DatumGetByteaPP(val);
						if (raw == NULL || VARSIZE_ANY_EXHDR(raw) != 32)
							ereport(ERROR,
									(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
									 errmsg("managed pending head carries a malformed proposal digest")));
						memcpy(pending_proposal_digest, VARDATA_ANY(raw), 32);
						val = SPI_getbinval(SPI_tuptable->vals[0],
											SPI_tuptable->tupdesc, 6, &isnull);
						if (isnull)
							ereport(ERROR,
									(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
									 errmsg("managed pending head lost its proposal body")));
						raw = DatumGetByteaPP(val);
						pending_body_len = VARSIZE_ANY_EXHDR(raw);
						pending_body = MemoryContextAlloc(scan_context,
														  pending_body_len + 1);
						memcpy(pending_body, VARDATA_ANY(raw), pending_body_len);
						pending_body[pending_body_len] = '\0';
						members = SPI_getbinval(SPI_tuptable->vals[0],
												SPI_tuptable->tupdesc, 7, &isnull);
						if (isnull)
							ereport(ERROR,
									(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
									 errmsg("managed pending head lost its members")));
						{
							Oid			mtypes[1] = {JSONBOID};
							Datum		margs[1] = {members};
							int			mret;
							SPIPlanPtr	mplan;

							mplan = SPI_prepare(
												"SELECT (m->>'segment_id')::bigint,"
												" (m->>'row_count')::integer,"
												" decode(m->>'payload_digest', 'hex')"
												" FROM jsonb_array_elements($1) m",
												1, mtypes);
							if (mplan == NULL)
								elog(ERROR, "could not prepare the pending members");
							mret = SPI_execute_snapshot(mplan, margs, NULL,
														fresh_snapshot,
														InvalidSnapshot,
														true, false, 0);
							if (mret != SPI_OK_SELECT || SPI_processed < 1)
								ereport(ERROR,
										(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
										 errmsg("managed pending head carries no members")));
							pending_member_count = (int) SPI_processed;
							pending_seg_ids = MemoryContextAlloc(scan_context,
																 sizeof(int64) * pending_member_count);
							pending_row_counts = MemoryContextAlloc(scan_context,
																	sizeof(int32) * pending_member_count);
							pending_digests = MemoryContextAlloc(scan_context,
																 32 * pending_member_count);
							for (int m = 0; m < pending_member_count; m++)
							{
								bytea	   *digest;

								val = SPI_getbinval(SPI_tuptable->vals[m],
													SPI_tuptable->tupdesc, 1, &isnull);
								pending_seg_ids[m] = DatumGetInt64(val);
								val = SPI_getbinval(SPI_tuptable->vals[m],
													SPI_tuptable->tupdesc, 2, &isnull);
								pending_row_counts[m] = DatumGetInt32(val);
								val = SPI_getbinval(SPI_tuptable->vals[m],
													SPI_tuptable->tupdesc, 3, &isnull);
								digest = isnull ? NULL : DatumGetByteaPP(val);
								if (digest == NULL || VARSIZE_ANY_EXHDR(digest) != 32)
									ereport(ERROR,
											(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
											 errmsg("managed pending head member carries a malformed digest")));
								memcpy(pending_digests + 32 * m,
									   VARDATA_ANY(digest), 32);
							}
							has_pending = true;
						}
					}
				}
			}
		}
		PG_FINALLY();
		{
			SetUserIdAndSecContext(user, sec_context);
		}
		PG_END_TRY();
		SPI_finish();
		UnregisterSnapshot(fresh_snapshot);
		fresh_snapshot = NULL;
	}

	/* pending-head proof: the observation's own metadata (origin contact) */
	if (has_pending)
	{
		PgDatalakeManagedPendingProof proof;
		PgDatalakeManagedPendingMember *members =
			MemoryContextAlloc(scan_context,
							   sizeof(PgDatalakeManagedPendingMember) *
							   pending_member_count);
		int64	   *included = MemoryContextAlloc(scan_context,
												  sizeof(int64) *
												  MANAGED_READ_MAX_MEMBERS);
		uintptr_t	n_included;

		for (int m = 0; m < pending_member_count; m++)
		{
			members[m].segment_id = pending_seg_ids[m];
			members[m].row_count = pending_row_counts[m];
			memcpy(members[m].payload_digest, pending_digests + 32 * m, 32);
		}
		memset(&proof, 0, sizeof(proof));
		proof.parent_pin = pins[proof_edge + 1];
		proof.parent_members = pin_members[proof_edge + 1];
		proof.parent_member_count = pin_member_counts[proof_edge + 1];
		proof.schema_id = scan->admission.schema_id;
		proof.spec_id = scan->admission.spec_id;
		memcpy(proof.operation_id, &pending_operation, 16);
		memcpy(proof.input_digest, pending_input_digest, 32);
		proof.expected_parent = pending_expected_parent;
		proof.snapshot_id = pending_snapshot;
		memcpy(proof.request_digest, pending_proposal_digest, 32);
		proof.request_body = (const uint8 *) pending_body;
		proof.request_body_len = pending_body_len;
		proof.members = members;
		proof.member_count = pending_member_count;
		proof.s3_endpoint = scan->admission.s3_endpoint;
		proof.s3_access_key = s3_access_key;
		proof.s3_secret_key = s3_secret_key;
		proof.s3_region = scan->admission.s3_region;

		errbuf[0] = '\0';
		n_included = pg_datalake_managed_read_prove_pending(observation, &proof,
															included,
															MANAGED_READ_MAX_MEMBERS,
															&status, errbuf,
															sizeof(errbuf));
		if (status != ReadOk)
			managed_read_status_error(status, errbuf,
									  "managed read could not prove the pending head");
		if (n_included > 0)
		{
			pin_known[proof_edge] = true;
			scan->cut_proved = true;
			pin_members[proof_edge] = MemoryContextAlloc(scan_context,
														 sizeof(int64) * n_included);
			memcpy(pin_members[proof_edge], included, sizeof(int64) * n_included);
			pin_member_counts[proof_edge] = (int) n_included;
		}
	}

	/* ---- 3. statement frame: visible segments + retention ---- */
	{
		Oid			owner = capture_table_owner("managed_capture_state", true);
		Oid			user;
		int			sec_context;

		GetUserIdAndSecContext(&user, &sec_context);
		SPI_connect();
		PushActiveSnapshot(scan->rs_base.rs_snapshot);
		PG_TRY();
		{
			Oid			argtypes[2] = {OIDOID, UUIDOID};
			Datum		args[2] = {ObjectIdGetDatum(relid),
				UUIDPGetDatum(&scan->admission.binding)
			};
			int			ret;
			int			retention_pos = 0;

			SetUserIdAndSecContext(owner, sec_context | SECURITY_LOCAL_USERID_CHANGE |
								   SECURITY_RESTRICTED_OPERATION);
			{
				SPIPlanPtr	plan = SPI_prepare(
											   "SELECT segment_id, row_count FROM lakehouse.get_managed_read_visible_segments($1, $2)",
											   2, argtypes);

				if (plan == NULL)
					elog(ERROR, "could not prepare the visible segments");
				ret = SPI_execute_snapshot(plan, args, NULL,
										   scan->rs_base.rs_snapshot, InvalidSnapshot,
										   true, false, 0);
			}
			if (ret != SPI_OK_SELECT)
				elog(ERROR, "could not read the managed read visible segments");
			if (SPI_processed > 0)
			{
				visible_ids = MemoryContextAlloc(scan_context,
												 sizeof(int64) * SPI_processed);
				visible_retained = MemoryContextAlloc(scan_context,
													  sizeof(bool) * SPI_processed);
				scan->overlay_row_counts = MemoryContextAlloc(scan_context,
															  sizeof(int64) * SPI_processed);
				scan->n_overlay_row_counts = (int) SPI_processed;
				for (int v = 0; v < SPI_processed; v++)
				{
					bool		isnull;

					visible_ids[v] = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[v],
																 SPI_tuptable->tupdesc,
																 1, &isnull));
					scan->overlay_row_counts[v] =
						DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[v],
													SPI_tuptable->tupdesc,
													2, &isnull));
					if (isnull)
						ereport(ERROR,
								(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
								 errmsg("managed COUNT found a visible segment without row_count")));
					visible_retained[v] = false;
				}
				n_visible = (int) SPI_processed;
			}

			/* merge retention flags (both sides ordered by seg_id) */
			{
				SPIPlanPtr	plan = SPI_prepare(
											   "SELECT segment_id, payload_retained FROM"
											   " lakehouse.get_managed_read_segment_retention($1, $2)",
											   2, argtypes);

				if (plan == NULL)
					elog(ERROR, "could not prepare the segment retention");
				ret = SPI_execute_snapshot(plan, args, NULL,
										   scan->rs_base.rs_snapshot, InvalidSnapshot,
										   true, false, 0);
			}
			if (ret != SPI_OK_SELECT)
				elog(ERROR, "could not read the managed read segment retention");
			for (int v = 0; v < n_visible; v++)
			{
				bool		row_null;

				while (retention_pos < SPI_processed &&
					   DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[retention_pos],
												   SPI_tuptable->tupdesc, 1,
												   &row_null)) < visible_ids[v])
					retention_pos++;
				if (retention_pos < SPI_processed)
				{
					bool		isnull;
					Datum		id = SPI_getbinval(SPI_tuptable->vals[retention_pos],
												   SPI_tuptable->tupdesc, 1,
												   &isnull);
					Datum		kept = SPI_getbinval(SPI_tuptable->vals[retention_pos],
													 SPI_tuptable->tupdesc, 2,
													 &isnull);

					if (!isnull && DatumGetInt64(id) == visible_ids[v])
						visible_retained[v] = DatumGetBool(kept);
				}
			}
		}
		PG_FINALLY();
		{
			SetUserIdAndSecContext(user, sec_context);
		}
		PG_END_TRY();
		PopActiveSnapshot();
		SPI_finish();
	}

	/* ---- 4. select the compatible cut (pure) ---- */
	{
		PgDatalakeManagedCutCandidate *candidates =
			palloc(sizeof(PgDatalakeManagedCutCandidate) * pin_count);

		for (i = 0; i < pin_count; i++)
		{
			candidates[i].pin = pins[i];
			candidates[i].membership = pin_known[i] ? 1 : 0;
			candidates[i].members = pin_members[i];
			candidates[i].member_count = pin_member_counts[i];
		}
		overlay_ids = palloc(sizeof(int64) * MANAGED_READ_MAX_OVERLAY);
		errbuf[0] = '\0';
		n_overlay = (int) pg_datalake_managed_read_select_cut(observation,
															  candidates,
															  pin_count,
															  visible_ids,
															  (const uint8 *) visible_retained,
															  n_visible,
															  &cut_pin,
															  overlay_ids,
															  MANAGED_READ_MAX_OVERLAY,
															  &status, errbuf,
															  sizeof(errbuf));
		if (status != ReadOk)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("managed read found no compatible snapshot"),
					 errdetail("%s", errbuf[0] != '\0' ? errbuf :
							   "no certified pin is consistent with the visible ledger")));
		scan->cut_pin = cut_pin;
		scan->cut_overlay_ids = palloc(sizeof(int64) * n_overlay);
		memcpy(scan->cut_overlay_ids, overlay_ids, sizeof(int64) * n_overlay);
		scan->n_cut_overlay = n_overlay;
	}

	/* ---- 5. verify + load the overlay payloads (statement snapshot) ---- */
	schema_json = lakehouse_build_schema_json(tupdesc);
	if (n_overlay > 0 && !scan->for_count)
	{
		/*
		 * select_cut returns ascending IDs; enforce the loader's strict order
		 * defensively before building the SQL array.
		 */
		for (i = 1; i < n_overlay; i++)
			if (overlay_ids[i] <= overlay_ids[i - 1])
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("managed read overlay selection is unordered")));
		{
			Oid			owner = capture_table_owner("managed_capture_state", true);
			Oid			user;
			int			sec_context;

			GetUserIdAndSecContext(&user, &sec_context);
			SPI_connect();
			PushActiveSnapshot(scan->rs_base.rs_snapshot);
			PG_TRY();
			{
				ArrayType  *id_array = construct_array_builtin((Datum *) overlay_ids,
															   n_overlay, INT8OID);
				Oid			argtypes[3] = {OIDOID, UUIDOID, INT8ARRAYOID};
				Datum		args[3] = {ObjectIdGetDatum(relid),
					UUIDPGetDatum(&scan->admission.binding),
					PointerGetDatum(id_array)
				};
				int			ret;

				SetUserIdAndSecContext(owner, sec_context | SECURITY_LOCAL_USERID_CHANGE |
									   SECURITY_RESTRICTED_OPERATION);
				{
					SPIPlanPtr	plan = SPI_prepare(
												   "SELECT segment_id, payload_digest, payload, sha256(payload)"
												   " FROM lakehouse.load_managed_read_overlay($1, $2, $3, 65536, 8388608)",
												   3, argtypes);

					if (plan == NULL)
						elog(ERROR, "could not prepare the overlay load");
					ret = SPI_execute_snapshot(plan, args, NULL,
											   scan->rs_base.rs_snapshot, InvalidSnapshot,
											   true, false, 0);
				}
				if (ret != SPI_OK_SELECT || SPI_processed != n_overlay)
					ereport(ERROR,
							(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							 errmsg("managed read overlay payload is unavailable in this snapshot")));
				for (int o = 0; o < SPI_processed; o++)
				{
					bool		isnull;
					Datum		val;
					bytea	   *digest;
					bytea	   *computed;
					bytea	   *payload;
					DeltaRowsHandle *rows;
					int64		seg_id;

					seg_id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[o],
														 SPI_tuptable->tupdesc,
														 1, &isnull));
					if (seg_id != overlay_ids[o])
						ereport(ERROR,
								(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
								 errmsg("managed read overlay returned unexpected segments")));
					val = SPI_getbinval(SPI_tuptable->vals[o],
										SPI_tuptable->tupdesc, 2, &isnull);
					digest = isnull ? NULL : DatumGetByteaPP(val);
					val = SPI_getbinval(SPI_tuptable->vals[o],
										SPI_tuptable->tupdesc, 3, &isnull);
					payload = isnull ? NULL : DatumGetByteaPP(val);
					val = SPI_getbinval(SPI_tuptable->vals[o],
										SPI_tuptable->tupdesc, 4, &isnull);
					computed = isnull ? NULL : DatumGetByteaPP(val);
					/* verify the sealed payload BEFORE it becomes rows */
					if (digest == NULL || payload == NULL || computed == NULL ||
						VARSIZE_ANY_EXHDR(digest) != 32 ||
						VARSIZE_ANY_EXHDR(computed) != 32 ||
						memcmp(VARDATA_ANY(digest), VARDATA_ANY(computed), 32) != 0)
						ereport(ERROR,
								(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
								 errmsg("managed read overlay payload digest mismatch"),
								 errdetail("segment %lld failed its sealed payload verification",
										   (long long) seg_id)));
					rows = pg_datalake_delta_rows_open(TextDatumGetCString(PointerGetDatum(payload)),
													   schema_json, NULL, 0);
					if (rows == NULL)
						ereport(ERROR,
								(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
								 errmsg("managed read overlay payload could not be decoded")));
					if (scan->n_delta == 0)
						scan->delta_rows = MemoryContextAlloc(scan_context,
															  sizeof(DeltaRowsHandle *));
					else
						scan->delta_rows = repalloc(scan->delta_rows,
													sizeof(DeltaRowsHandle *) *
													(scan->n_delta + 1));
					scan->delta_rows[scan->n_delta++] = rows;
				}
			}
			PG_FINALLY();
			{
				SetUserIdAndSecContext(user, sec_context);
			}
			PG_END_TRY();
			PopActiveSnapshot();
			SPI_finish();
		}
	}

	/*
	 * The cut owns the staged delta read side: never fall back to the
	 * ordinary unpublished-segment loader (it would double-serve the accepted
	 * append outside the compatible-pin discipline).
	 */
	scan->delta_loaded = true;
	scan->delta_pos = 0;

	/* ---- 6. plan the selected pin exactly (consumes the observation) ---- */
	errbuf[0] = '\0';
	scan->session = pg_datalake_managed_read_plan_exact(&scan->read_observation,
														scan->cut_pin, &status,
														errbuf, sizeof(errbuf));
	if (scan->session == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("managed read could not plan the selected pin"),
				 errdetail("%s", errbuf[0] != '\0' ? errbuf :
						   "the selected snapshot failed its schema proof")));
	scan->task_count = pg_datalake_scan_task_count(scan->session);
	scan->rows = NULL;
	scan->current_task = 0;

	/*
	 * Metadata-only COUNT (for_count): the cut's two parts are disjoint by
	 * construction — published base rows come from the plan's Parquet
	 * metadata (no data-file reads) and overlay members from the ledger
	 * row_count captured in the statement frame — so the sum is the
	 * resolved scan's exact multiplicity.
	 */
	if (scan->for_count)
	{
		int64		total = 0;

		/*
		 * The elision contract (ffi.rs): record_count is pre-delete, so a
		 * delete-bearing plan MUST decline. Unreachable today — capture
		 * publications are INSERT-only — but fail closed for any future
		 * writer that publishes deletes into certified pins.
		 */
		if (pg_datalake_scan_has_deletes(scan->session))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("managed COUNT is unsupported on a snapshot with Iceberg deletes")));

		for (int t = 0; t < scan->task_count; t++)
		{
			int64		rows = pg_datalake_scan_task_rows(scan->session, t);

			elog(DEBUG1, "managed-cut-debug: count task %d rows %lld",
				 t, (long long) rows);
			total += rows;
		}

		/*
		 * Only the cut's overlay members count: a segment outside the
		 * selected overlay is either already in the base pin or not part of
		 * this statement's view.
		 */
		for (int o = 0; o < scan->n_cut_overlay; o++)
			for (int v = 0; v < scan->n_overlay_row_counts; v++)
				if (scan->cut_overlay_ids[o] == visible_ids[v])
				{
					total += scan->overlay_row_counts[v];
					break;
				}
		scan->cut_count = total;
		elog(DEBUG1, "managed-cut-debug: count total %lld overlay_members %d",
			 (long long) total, scan->n_cut_overlay);
	}

	/* backend-local evidence for the fixture drill and operator debugging */
	{
		StringInfoData ev;

		initStringInfo(&ev);
		appendStringInfo(&ev, "{\"pin\": %lld, \"proved\": %s, \"overlay\": [",
						 (long long) scan->cut_pin,
						 scan->cut_proved ? "true" : "false");
		for (i = 0; i < scan->n_cut_overlay; i++)
			appendStringInfo(&ev, "%s%lld", i > 0 ? ", " : "",
							 (long long) scan->cut_overlay_ids[i]);
		appendStringInfoString(&ev, "]}");
		if (managed_cut_evidence != NULL)
			pfree(managed_cut_evidence);
		managed_cut_evidence = MemoryContextStrdup(TopMemoryContext, ev.data);
	}

	/* the fast path is done: no payload readers, no own-writes overlay */
	if (scan->for_count)
		return;

	MemoryContextSwitchTo(scan_context);
}

/*
 * Metadata-only COUNT fast path for one capture-bound relation (spec 03,
 * "managed COUNT"): run the demand-time cut install with for_count set —
 * the statement frame captures the overlay ledger row_count and the plan
 * contributes the published base's Parquet-metadata task rows, both
 * disjoint by construction — then release the cut. Returns the exact
 * multiplicity; non-bound relations, unsupported shapes and unadmitted
 * roles ereport exactly as the scan path does.
 */
int64
lakehouse_managed_cut_count(Oid relid)
{
	Relation	rel;
	LakehouseTableScanDesc scan;
	int64		count = 0;

	if (IsInParallelMode())
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("bound managed capture does not support parallel scans")));
	if (has_subclass(relid))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("bound managed capture does not support inheritance scans")));
	if (check_enable_rls(relid, GetUserId(), false) != RLS_NONE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("bound managed capture does not support row-level security scans")));
	if (lakehouse_find_write_entry(relid) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("bound managed capture does not support scans with staged writes")));

	rel = RelationIdGetRelation(relid);
	scan = palloc0(sizeof(LakehouseTableScanDescData));
	scan->rs_base.rs_rd = rel;
	scan->rs_base.rs_snapshot = GetTransactionSnapshot();
	scan->capture_bound = true;
	scan->for_count = true;

	PG_TRY();
	{
		LakehouseOptions wm_opts;
		int			wm;

		lakehouse_admit_managed_cut(scan);

		/* same refusal as the scan path (common_setup's bound bypass) */
		memset(&wm_opts, 0, sizeof(wm_opts));
		decode_lakehouse_reloptions(rel, &wm_opts);
		wm = (wm_opts.write_mode != 0)
			? wm_opts.write_mode
			: lakehouse_write_mode;
		if (wm != LAKEHOUSE_WRITE_MODE_STAGED)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("bound managed capture scans require the staged write mode")));

		install_managed_cut(scan, RelationGetDescr(rel));
		count = scan->cut_count;
	}
	PG_FINALLY();
	{
		lakehouse_scan_release(scan);
		RelationClose(rel);
		pfree(scan);
	}
	PG_END_TRY();

	/*
	 * Return the saved value, NOT scan->cut_count: the chunk was pfreed
	 * above, and a cassert build poisons freed memory with 0x7f — the CI
	 * drill caught exactly that as a snapshot-id-sized count.
	 */
	return count;
}

/*
 * Read the last installed cut of THIS backend. NULL before the first
 * demand-time install of a bound managed scan.
 */
PG_FUNCTION_INFO_V1(lakehouse_managed_read_cut_evidence);

Datum
lakehouse_managed_read_cut_evidence(PG_FUNCTION_ARGS)
{
	if (managed_cut_evidence == NULL)
		PG_RETURN_NULL();
	PG_RETURN_DATUM(DirectFunctionCall1(jsonb_in,
										CStringGetDatum(managed_cut_evidence)));
}

/*
 * Own-writes overlay: copy the tuple into the txn-scoped entry. Rows are
 * NOT pushed to the Rust writer here — staging is C-side only; rows
 * reach the parquet accumulator at PRE-COMMIT, AFTER subtransaction
 * pruning. Pushing at insert time leaked savepoint-rolled-back rows into
 * the committed snapshot (13-review blocker #3).
 */
static void
stage_slot(LakehouseWriteEntry *entry, TupleTableSlot *slot, TupleDesc tupdesc)
{
	int			j;
	MemoryContext old;

	if (entry->bound_capture)
	{
		CommandId	cid = GetCurrentCommandId(false);

		/*
		 * DDL in a later command can replace the relation descriptor inside
		 * this transaction. Compare the layouts before interpreting any Datum
		 * with the transaction's saved descriptor.
		 */
		if ((entry->checked_layout_cid != cid || entry->natts != tupdesc->natts ||
			 TupleDescGetVersion(entry->tupdesc) != TupleDescGetVersion(tupdesc)) &&
			!equalRowTypes(entry->tupdesc, tupdesc))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("managed capture source schema changed within the transaction")));
		entry->checked_layout_cid = cid;
	}

	old = MemoryContextSwitchTo(entry->mcxt);

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

/* The raw COW rewrite and UPDATE's row-version fetch both need a delete-free base. */
static void
check_cow_scan(ScanSession *session)
{
	if (pg_datalake_scan_has_deletes(session))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("lakehouse: copy-on-write UPDATE/DELETE is unsupported on a snapshot with Iceberg deletes")));
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
						   "in this transaction; rows inserted in the same transaction, or "
						   "committed but not yet published under staged append, are not "
						   "addressable for mutation.")));
	check_cow_scan(scan->session);
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
		if (!pg_datalake_write_matches_scan(entry->ws, scan->session))
			ereport(ERROR,
					(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
					 errmsg("lakehouse: copy-on-write source changed between scan and write")));
		tf = tomb_file_get(entry, file, true);
		tomb_file_add_row(entry, tf, rowidx);
		tomb_file_note_meta(tf, nrows, size);
	}
}

/*
 * Run one parameterized insert as the validated ledger owner, after all
 * datum conversion has completed under the writer's ordinary permissions.
 * Admission and the final binding check share the INSERT's relation lock.
 */
static void
insert_bound_capture(LakehouseWriteEntry *entry, StringInfo payload)
{
	char	   *utf8 = pg_server_to_any(payload->data, payload->len, PG_UTF8);
	int			length = utf8 == payload->data ? payload->len : strlen(utf8);
	Oid			argtypes[] = {OIDOID, BYTEAOID, INT4OID, UUIDOID};
	Datum		values[] = {
		ObjectIdGetDatum(entry->relid),
		PointerGetDatum(cstring_to_text_with_len(utf8, length)),
		Int32GetDatum((int32) entry->staged_rows),
		UUIDPGetDatum(&entry->binding_id),
	};
	Oid			owner = capture_table_owner("lakehouse_delta_segments", false);
	Oid			user;
	int			sec_context;

	if (utf8 != payload->data)
		pfree(utf8);
	GetUserIdAndSecContext(&user, &sec_context);
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	PG_TRY();
	{
		int			ret;

		SetUserIdAndSecContext(owner, sec_context | SECURITY_LOCAL_USERID_CHANGE |
							   SECURITY_RESTRICTED_OPERATION);
		ret = SPI_execute_with_args(
									"INSERT INTO lakehouse.lakehouse_delta_segments"
									" (relid, payload, row_count, capture_binding_id, authority_id, table_uuid)"
									" SELECT $1, $2, $3, $4, b.authority_id, b.table_uuid"
									" FROM lakehouse.managed_capture_state s"
									" JOIN lakehouse.managed_capture_bindings b"
									" ON b.binding_id OPERATOR(pg_catalog.=) s.active_binding_id"
									" WHERE s.relid OPERATOR(pg_catalog.=) $1 AND s.active_binding_id OPERATOR(pg_catalog.=) $4"
									" AND b.source_columns OPERATOR(pg_catalog.=) lakehouse.capture_source_columns($1)",
									lengthof(argtypes), argtypes, values, NULL, false, 0);
		if (ret != SPI_OK_INSERT)
			elog(ERROR, "could not write to lakehouse capture ledger");
		if (SPI_processed != 1)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("managed capture binding or source schema changed before commit")));
	}
	PG_FINALLY();
	{
		SetUserIdAndSecContext(user, sec_context);
	}
	PG_END_TRY();
	PopActiveSnapshot();
	SPI_finish();
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
	int			ret;
	TupleDesc	tupdesc = entry->tupdesc;

	if (entry->bound_capture)
	{
		pg_uuid_t	binding_id;

		LockRelationOid(entry->relid, RowExclusiveLock);
		if (!lookup_capture_binding(entry->relid, &binding_id) ||
			memcmp(&entry->binding_id, &binding_id, sizeof(binding_id)) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("managed capture binding changed before commit")));
	}

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
			/*
			 * dropped attributes are omitted — cell count matches the
			 * schema column count (13-review #8b)
			 */
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

	if (entry->bound_capture)
	{
		insert_bound_capture(entry, &payload);
		pfree(payload.data);
		return;
	}

	/*
	 * INSERT inside the committing transaction: WAL-fsync at commit makes the
	 * delta durable BEFORE the COMMIT ack (D1-rev's point). Parameterized
	 * payload (no literal injection surface).
	 */
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	{
		/*
		 * P0b4 slice B (spec 15 §6.6): the segment's row_count is EXACT at
		 * capture time — it is the txn's own staged-row total — so the
		 * COUNT elision reads the ledger instead of decoding payloads.
		 * Pre-upgrade rows have NULL row_count and are tolerated (the elision
		 * declines on NULL).
		 */
		Oid			argtypes[2] = {TEXTOID, INT4OID};
		Datum		values[2];
		char		nulls[3] = {' ', ' ', '\0'};

		values[0] = CStringGetTextDatum(payload.data);
		values[1] = Int32GetDatum((int32) entry->staged_rows);
		ret = SPI_execute_with_args(psprintf("INSERT INTO"
											 " lakehouse.lakehouse_delta_segments"
											 " (relid, payload, row_count)"
											 " VALUES (%u, $1::bytea, $2)",
											 (unsigned) entry->relid),
									2, argtypes, values, nulls, false, 0);
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

	/*
	 * staged mode (spec 15 D2): durability is the delta table — no remote
	 * session is opened, ever, on the insert path. R2: a table-level WITH
	 * (write_mode='staged') sticks over the session GUC.
	 */
	if (entry->write_mode != LAKEHOUSE_WRITE_MODE_STAGED)
		open_write_session(entry, RelationGetDescr(rel));
	stage_slot(entry, slot, RelationGetDescr(rel));
}

static void
lakehouse_tuple_insert_speculative(Relation rel, TupleTableSlot *slot,
								   CommandId cid, uint32 options,
								   BulkInsertStateData *bistate,
								   uint32 specToken)
{
	/*
	 * speculative insert only comes from INSERT ... ON CONFLICT, which needs
	 * upsert semantics — its slice
	 */
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
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("lakehouse: row locking (SELECT ... FOR UPDATE) is not "
					"supported on managed tables yet"),
			 errdetail("Managed row locking arrives with the M3 publication "
					   "and MVCC milestone (spec 18 §5).")));
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
	/*
	 * autovacuum skips zero-page relations; a manual VACUUM is a no-op
	 * pointer to the real maintenance path (doc 08, lakehouse.optimize M4)
	 */
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
						   "in this transaction; rows inserted in the same transaction, or "
						   "committed but not yet published under staged append, are not "
						   "addressable for mutation.")));
	check_cow_scan(scan->session);
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
	/*
	 * hollow local storage: report 0 — real sizing is Iceberg's table
	 * metrics (EXPLAIN + reportMetrics), not relpages
	 */
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
	/*
	 * no local tuple storage until slice 27; TOAST policy is revisited there
	 * (large values go to parquet pages, not a local toast table)
	 */
	return false;
}

static bool
lakehouse_scan_analyze_next_block(TableScanDesc scan, ReadStream *stream)
{
	/*
	 * ANALYZE samples nothing yet (stats arrive with managed DDL); a false
	 * here makes acquire_sample_rows return zero rows cleanly
	 */
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
	.finish_bulk_insert = NULL, /* optional */

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
	.relation_fetch_toast_slice = NULL, /* ditto */
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
		return;
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

	/*
	 * COW tombstones captured in the aborted subxact tree are discarded too —
	 * a savepoint-rolled-back DELETE must not rewrite a file
	 */
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

				/*
				 * fully abandoned: the file record is dropped (no DELETED
				 * entry rides to the commit for a rolled-back DELETE)
				 */
			}
			entry->n_tomb_files = w;
		}
	}
	prune_scan_registry_above(mySubid);
	(void) parentSubid;
}

/*
 * lakehouse GUC surface (spec 15 §7.2 P2): extension-defined via
 * DefineCustom*Variable — the seam Postgres provides; NO core edit
 * (RULES §3: the guc_tables.c allowlist entry is retired).
 */
int			lakehouse_write_mode = 1;	/* enum: 1 = cow, 2 = staged */

/*
 * kB size-knob bounds (spec 15 P2): min 32MB, max 4GB; defaults 128MB
 * (target_file_size) / 256MB (cache)
 */
#define LAKEHOUSE_MIN_SIZE_KB (32 * 1024)
#define LAKEHOUSE_MAX_SIZE_KB (4 * 1024 * 1024)

int			lakehouse_target_file_size = 128 * 1024;	/* kB; SHOWs as 128MB */
int			lakehouse_publish_max_lag = 30; /* s */
int			lakehouse_cache_max_bytes = 256 * 1024; /* kB; SHOWs as 256MB */
bool		lakehouse_publisher_enabled = true;
char	   *lakehouse_publish_database = NULL;	/* default: "postgres" */

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
							 1, /* boot default: cow */
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

	/*
	 * POSTMASTER-context GUCs are legal only during preload; a non-preload
	 * load (CREATE EXTENSION) skips them — the publisher then runs via
	 * lakehouse.start_publisher()'s dynamic registration with its built-in
	 * defaults.
	 */
	if (process_shared_preload_libraries_in_progress)
	{
		DefineCustomBoolVariable("lakehouse.publisher_enabled",
								 "Run the staged-delta publisher background worker (spec 15 §4).",
								 NULL,
								 &lakehouse_publisher_enabled,
								 true,
								 PGC_POSTMASTER,
								 0,
								 NULL, NULL, NULL);

		DefineCustomStringVariable("lakehouse.publish_database",
								   "Database the publisher worker connects to.",
								   NULL,
								   &lakehouse_publish_database,
								   "postgres",
								   PGC_POSTMASTER,
								   0,
								   NULL, NULL, NULL);
	}							/* end preload-only GUC block */

	/* spec 19 §4 (L0): telemetry GUCs — same "lakehouse." prefix */
	lakehouse_telemetry_guc_init();

	/* spec 19 §6.5 (family E): planner relsize GUCs */
	lakehouse_fdw_plan_guc_init();

	/* spec 19 §6.5 (family C/E): CTE-output statistics hook */
	lakehouse_cte_stats_hook_init();

	/* spec 19 §5: correlated-agg pullup operator gate */
	lakehouse_agg_pullup_guc_init();

	MarkGUCPrefixReserved("lakehouse");

	/*
	 * per-backend immutable-object block cache, capped by GUC (spec 15 §6.2;
	 * first call wins — process-lifetime capacity)
	 */
	pg_datalake_core_cache_init((int64) lakehouse_cache_max_bytes * 1024);

	/*
	 * P4c: the publisher worker registers STATICALLY under preload
	 * (production) and DYNAMICALLY via lakehouse.start_publisher() when the
	 * library loads without preload (tests/dev) — dynamic workers are
	 * postmaster-owned and survive the registering backend.
	 */
	if (process_shared_preload_libraries_in_progress &&
		lakehouse_publisher_enabled)
		register_publisher_worker();

	/*
	 * idempotent: the library can load more than once per backend (extension
	 * script + AM handoff); duplicate registrations ran the staged capture
	 * once PER registration per commit
	 */
	if (!xact_callbacks_registered)
	{
		/*
		 * Execution-time COUNT (managed_count.c, spec 16 successor of the
		 * gated-off plan-time elision): rewrites trivial managed count(*)
		 * into lakehouse.managed_count(relid), a volatile invoker function
		 * that resolves permissions, RLS and the snapshot per execution with
		 * an ordinary-aggregate fallback.
		 */
		install_lakehouse_count_exec();
		install_lakehouse_telemetry();
		install_lakehouse_scan_stats();
		RegisterXactCallback(lakehouse_xact_callback, NULL);
		RegisterSubXactCallback(lakehouse_subxact_callback, NULL);
		xact_callbacks_registered = true;
	}
}

/* pg_am oid of the lakehouse AM (resolved once per backend) */
Oid
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
	if (entry->bound_capture)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("bound managed capture requires an existing remote table")));
	schema_json = lakehouse_build_schema_json(RelationGetDescr(rel));

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
		/*
		 * error KIND from the FFI (13-review #6): 1 = conflict — classify
		 * by type, never by searching the message for "409"
		 */
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
PG_FUNCTION_INFO_V1(lakehouse_tableam_handler);

Datum
lakehouse_tableam_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&lakehouse_tableam_methods);
}
