/*
 * lakehouse_tableam.h — shared surface of the lakehouse TableAM write
 * machinery and the publisher worker (specs/lakehouse/15-staged-writes-
 * and-caching.md §3/§4). tableam.c owns the AM callbacks + sessions;
 * publisher.c rides the write-state machinery via these exports.
 */
#ifndef LAKEHOUSE_TABLEAM_H
#define LAKEHOUSE_TABLEAM_H

#include "postgres.h"
#include "nodes/primnodes.h"
#include "utils/relcache.h"
#include "utils/uuid.h"
#include "lakehouse.h"

typedef struct LakehouseTombFile
{
	char	   *path;			/* base-snapshot file URI (mcxt-owned) */
	uint64	   *rows;			/* 0-based row indexes, sorted ascending */
	SubTransactionId *row_subids;	/* owning subxact per tombstone */
	int			nrows;
	int			rows_cap;
	int64		record_count;	/* identity of the DELETED manifest entry */
	int64		file_size_in_bytes;
} LakehouseTombFile;

/* publisher worker knobs (GUCs defined + registered in tableam.c _PG_init) */
extern int	lakehouse_publish_max_lag;
extern bool lakehouse_publisher_enabled;
extern char *lakehouse_publish_database;

/*
 * LakehouseWriteEntry — per-transaction per-relation write state.
 *
 * ⚠️ HASH-KEY ALIASING (do not re-break — this cost us a multi-day debug):
 *
 * dynahash does NOT store the key beside the user struct. The hash element
 * layout is [header][key][data], and hash_search returns a pointer to the
 * KEY (ELEMENTKEY), cast to the entry type — the ENTIRE struct overlays the
 * stored key bytes. Consequence: any member of this struct that lands at
 * offset 0 (within keysize = sizeof(Oid) bytes) IS the stored key; writing
 * it from init code silently REWRITES the key, and every later hash_search
 * with the same key MISSES.
 *
 * That exact bug shipped here: `int write_mode` sat first, and its !found
 * init (`entry->write_mode = ...`) trampled the key, so HASH_ENTER minted
 * a DUPLICATE ENTRY PER ROW (probe showed found=0 three times, hash
 * growing 1→2→3, same relid, same table). A 3-row staged INSERT captured
 * three single-row delta segments instead of one 3-row segment, and COW
 * mode silently opened one remote write session per row instead of one
 * per transaction. It survived every other hypothesis (xact-callback
 * registration, seq-search iteration, teardown) because nothing was
 * "losing" the entry — the table was full of keys that no longer matched.
 *
 * RULES for this struct (and any other HASH_ELEM entry struct here):
 * 1. The hash key field comes FIRST and is owned by dynahash — hash_search
 *    copies it in; init code must never write it (re-writing the same
 *    value is harmless but forbidden by convention; a different value
 *    corrupts the table).
 * 2. Adding a member is safe only if the key stays first. NEVER reorder
 *    without re-checking that no member aliases [0, keysize).
 * 3. Debugging symptom to recognize instantly: HASH_ENTER reports
 *    found=false repeatedly for an identical key on the same HTAB, and
 *    hash_get_num_entries climbs with duplicates. That is key aliasing,
 *    not a teardown/registration/iteration bug.
 *
 * (LakehouseScanRegistryEntry below follows the same idiom correctly.)
 */
typedef struct LakehouseWriteEntry
{
	/*
	 * HASH KEY — MUST be first (see the block comment above for why, and
	 * for the aliasing failure mode). Written by hash_search itself; init
	 * code must not touch it.
	 */
	Oid			relid;
	int			write_mode;		/* resolved: reloption wins over GUC (R2) */
	WriteSession *ws;			/* FFI write session; NULL until 1st insert */
	MemoryContext mcxt;			/* all entry allocations live here */
	/* own-writes overlay: staged tuples, copied datum-wise into mcxt */
	Datum	   *staged_values;
	bool	   *staged_isnull;
	SubTransactionId *staged_subids;	/* owning subxact per staged row */
	int			staged_rows;
	int			natts;
	TupleDesc	tupdesc;
	/* identity, resolved at first insert */
	LakehouseOptions opts;
	bool		have_identity;
	bool		bound_capture;
	pg_uuid_t	binding_id;
	CommandId	checked_layout_cid;

	/*
	 * COW tombstones (slice 28): linear scan over AFFECTED FILES (small:
	 * files one statement touches), sorted rows + bsearch within a file
	 */
	LakehouseTombFile **tomb_files;
	int			n_tomb_files;
	int			tomb_cap;

	/*
	 * input-function plumbing for the PRE-COMMIT rewrite pass (reads a base
	 * file, re-encodes surviving rows exactly like staged ones)
	 */
	FmgrInfo   *in_funcs;
	Oid		   *typioparams;
	bool		have_in_funcs;
} LakehouseWriteEntry;

/* tableam.c — write-state machinery used by the publisher */
static inline int
lakehouse_commit_errcode(LakehouseWriteEntry *entry)
{
	switch (pg_datalake_write_last_commit_failure(entry->ws))
	{
		case PgDatalakeCommitFailure_Conflict:
			return ERRCODE_T_R_SERIALIZATION_FAILURE;
		case PgDatalakeCommitFailure_Unknown:
			return ERRCODE_T_R_STATEMENT_COMPLETION_UNKNOWN;
		default:
			return ERRCODE_CONNECTION_EXCEPTION;
	}
}

extern Oid	lakehouse_am_oid(void);
extern void install_lakehouse_count_exec(void);
extern void install_lakehouse_telemetry(void);
extern void install_lakehouse_scan_stats(void);
extern void lakehouse_scan_stats_record(Oid relid, const char *endpoint,
										const char *ns_table,
										int64 snapshot_id, int64 row_count,
										int files_total, int files_pruned,
										int scan_tasks, int64 total_bytes,
										bool has_deletes);
struct ScanSession;
extern void lakehouse_column_stats_record(Oid relid, const char *ns_table,
										  int64 snapshot_id, int64 rows,
										  struct ScanSession *session,
										  Relation rel);
extern void lakehouse_telemetry_guc_init(void);
extern void lakehouse_fdw_plan_guc_init(void);
extern void lakehouse_agg_pullup_guc_init(void);
extern void lakehouse_check_capture_read(Oid relid);
extern bool lakehouse_rel_is_capture_bound(Oid relid);
extern int64 lakehouse_managed_cut_count(Oid relid);
extern LakehouseWriteEntry *lakehouse_find_write_entry(Oid relid);
extern LakehouseWriteEntry *get_write_entry(Relation rel);
extern char *lakehouse_build_schema_json(TupleDesc tupdesc);
extern void open_write_session(LakehouseWriteEntry *entry, TupleDesc tupdesc);
extern void register_publisher_worker(void);

#endif							/* LAKEHOUSE_TABLEAM_H */
