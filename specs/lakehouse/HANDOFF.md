# HANDOFF.md — State for the Next Session

*Updated 2026-11-19. Working directory: `/home/vsingh/code/postgres`.*

## What exists

- Repo: `/home/vsingh/code/postgres`, clone of `postgres/postgres` master
  (`61f6021b110`, PG 20 dev). `origin` = `anvai-labs/postgres` (private),
  `upstream` = `postgres/postgres`.
- **Branch layout**: `main` = fork base (upstream master + specs commits, merge
  sync per RULES §1 as amended); `lakehouse-dev` = integration branch (renamed
  from `iceberg-dev` 2026-11-19). NOTE: GitHub's default branch is still
  `master` on the remote — flipping it to `main` via `gh repo edit` was denied
  by the harness and `origin/master` still exists; do that manually
  (`gh repo edit anvai-labs/postgres --default-branch main`, then
  `git push origin --delete master`).
- **Naming (decided 2026-11-19, user)**: format-generic lakehouse. Rust core
  `pgdatalake-core` (C ABI `pg_datalake_*`, `libpgdatalake_core.a`), SQL
  surface `lakehouse` (schema/extension/`USING lakehouse FORMAT iceberg`),
  views `pg_lakehouse`, GUCs `lakehouse.*`, SQLSTATE `LD0`, meson
  `-Ddatalake=auto`, branch/tests/fixtures/make-targets all `lakehouse*`.
  Iceberg is a FORMAT layer, not the product name. Don't relitigate.
- **M0 done 2026-11-19** — tag `milestone/m0`, GitHub release, cold-room gate
  ALL GREEN (transcript `retro/m0-smoke-transcript.txt`, retro `retro/m0.md`).
  Upstream regress 243/243 under `meson test --suite regress` (meson-only
  tree; autoconf `make check` = legacy escape hatch, see RULES §9 amendment).
- **M1 in progress** (REST client + read path, all TDD, live-verified vs the
  Lakekeeper + SeaweedFS fixture stack):
  - slice 1 `GET /v1/config` (+ `?warehouse=` — Lakekeeper requires it);
  - slice 2 OAuth2 client-credentials, cached, proactive refresh at 80% TTL;
  - slice 3 `loadTable` + **v3-first** metadata model (FormatVersion enum
    rejects v1; v3 types — unknown/timestamp_ns/variant + snapshot
    sequence-number/first-row-id — are first-class; v2 parses same model).
    Namespace `%1F` joins, prefix auto-stored from merged config view
    (Lakekeeper puts prefix in **defaults**), 404 -> `RestError::NotFound`;
  - slice 4 object-store IO: `Location` (s3:// full bucket keys, file://
    rooted local), `StorageIo` over object_store (aws+http), gzip decode
    for `.gz.metadata.json`, blocking over internal tokio current-thread rt.
  - live example: `cargo run --example fetch_config` walks config ->
    loadTable -> S3 fetch -> gzip -> parse (fixture: Lakekeeper `smoke`
    warehouse, table `analytics.events` — both created idempotently by
    `lakekeeper-setup.sh`, wired into `make lakehouse-up`).
  - 22 cargo unit tests; deps: serde/serde_json/reqwest(rustls,blocking)/
    tokio(rt)/object_store(aws,http)/flate2/uuid/urlencoding; tiny_http +
    tempfile in dev-deps (unit tests use in-process mocks + local store —
    no network at unit-test time, doc 10 §1).
  - slice 5 manifest-list Avro reader (apache-avro dep; embedded writer
    schema VALIDATED against required fields — doc 10 §4 fuzz hardening);
  - slice 6 manifest data-file reader (entry status, delete-file content
    types, column-stats maps by field id; partition opaque until pruning);
  - slice 7 plan_scan() — snapshot selection -> manifest-list -> manifests
    -> ScanTask list (DELETED dropped, sequence numbers resolved, row/file
    aggregates); fully offline-tested via file:// fixtures;
  - 27 cargo tests; apache-avro dep added.
  - slice 8 partition-spec model + identity-transform pruning
    (conservative: non-identity transforms always keep the file);
  - slice 9 FFI boundary (pg_datalake_scan_*, panic-free catch_unwind,
    caller-owned error buffers, # Safety docs) + CatalogClient
    orchestrator + lakehouse.scan_files() SQL SRF (8 text args; storage
    credentials are caller-side — object_store IMDS fallback caught live);
  - slice 10 Parquet row reads (parquet/arrow 54) + rows FFI
    (pg_datalake_rows_open/next/col_*/close; values rendered to text at
    advance time, nulls via parallel flags; CellValue normalization
    Int32->Int64, Float32->Float64);
  - slice 11 FDW handler (fdw.c): GetForeignRelSize/Paths/Plan +
    Begin/Iterate/ReScan/End; table option 'analytics.events' with
    namespace = last-dot prefix (doc 03 §3); quals evaluated locally
    (no pushdown yet); regress fdw.sql proves pg_class relkind 'f' +
    information_schema + 08000 fail-fast. NOTE: FDW ships in the single
    lakehouse module for M1 (doc 01 said contrib/lakehouse_fdw — split is
    the M5 extraction item; note in retro).
  - 35 cargo tests + 3 regress groups; deps now incl. parquet/arrow 54.
  - slice 12 PyIceberg fixture data + interop fixes: scripts/gen_fixtures.py
    (3 runs x 1000 rows into analytics.events; venv at build/venv, or run
    in-container per script docs); int->long Avro promotions, nullable
    union snapshot_id, long split_offsets, arrow temporal primitive
    arrays, CellValue::TimestampMicros + ISO rendering (civil-from-days,
    no chrono). LIVE: SELECT count/min/max/WHERE through the FDW over
    PyIceberg-written data — full read path works end-to-end.
  - 36 cargo tests + 3 regress groups.
  - slice 13 EXPLAIN visibility (ExplainForeignScan: table/endpoint/
    task count/rows/projection/pushdown status) + projection pushdown
    from targetlist+qual Vars + live BUG FIX: local quals were in the
    fdw_recheck_quals slot (never evaluated on plain scans — WHERE
    returned all rows); moved to qpqual, verified live (4/3000/2100).
  - slice 14 bounds-based predicate pushdown: ScanPredicate Lt/Le/Gt/Ge
    + bounds_keep() over manifest column stats (conservative: missing
    bounds/ids/types always keep; field-id resolution via current
    schema); plan_scan prunes partitions AND bounds.
  - slice 15 FDW qual->predicate translation: TranslateQual (Var-op-Const,
    =/</<=/>/>= with flipped-op inversion; per-type value conversion —
    int/date days, timestamp(s)/tstz micros matching bounds width,
    floats, text with JSON escaping) -> predicate JSON via FFI ->
    plan_scan pruning; ALL clauses stay in qpqual (pruning conservative,
    executor enforces exact per-row filter); live bugs fixed: RestrictInfo
    vs plain-Expr (translate after extract_actual_clauses), double
    extract segfault, predicate/opts ordering (pruning silently skipped +
    wrong counts), qpqual slot (Filter never applied). LIVE: EXPLAIN shows
    tasks: 1 (pruned from 3) for WHERE id<5; count(*)=4 exact.
  - pyiceberg manifest metric maps fixed: real manifests encode metrics as
    ARRAYS of {key:int, value} records (raw avro dump confirmed); parser
    accepts both shapes; fixtures updated to spec shape.
  - 40 cargo tests + 3 regress groups.
  - slice 16 EXPLAIN pruned evidence: ScanPlan.files_before/after_pruning
    + FFI accessors; EXPLAIN shows 'Iceberg files examined: N' and
    'Iceberg files pruned: M' (omitted when nothing pruned). LIVE:
    WHERE id<5 -> examined 3 / pruned 2 / tasks 1.
  - 41 cargo tests + 3 regress groups.
  - slice 17 parallel scans: IsForeignScanParallelSafe (required or the
    planner disables parallel entirely), partial path with
    parallel_aware=true (without it the worker DSM hook never runs and
    every process scans all tasks — the 3x-duplicates bug, caught live),
    DSM hooks + pg_atomic_uint64 claim counter in Iterate. LIVE:
    Gather/2 workers, count=3000 distinct=3000, WHERE under parallelism
    exact. M1 limitation: one foreign scan per query for the shared
    static pointer (documented, revisit when multi-scan queries matter).
  - slice 18 snapshot pinning: FDW option snapshot_id -> FFI
    scan_new(snapshot_id) -> plan_scan snapshot selection. LIVE:
    table pinned to first pyiceberg snapshot reads 1000 rows while
    current reads 3000 — time-separated reads verified.
  - slice 19 time travel grammar (SELECT ... FOR TIMESTAMP/VERSION/TAG
    AS OF): the design DECISION — the clause lives in the LOCKING-CLAUSE
    position (per-table attachment is impossible: after a from-item a
    bare FOR starts FOR UPDATE and LALR(1) cannot disambiguate; bison
    %expect 0 rejects it). The pin rides in SelectStmt.lockingClause as
    a raw DefElem; consumers of that list skip DefElem entries
    (analyze.c x2 error paths + main loop, parse_relation.c
    isLockedRefname). applyLakehouseTimeTravel (parse_clause.c) resolves
    the DefElem into a canonical selector string on RTE.lakehouse_snapshot
    ("ts:<ms>" / "id:<n>" / "ref:<name>") for every foreign-table RTE of
    the level — timestamps via timestamptz_in at parse time (rules/views
    store epoch ms, timezone-stable); big ids lex as Float and are
    validated with pg_strtoint64_safe. FDW carries the selector in
    fdw_private[2], OpenSession prefers it over the snapshot_id option,
    plan-time errors name the selector (regress-observable). Rust core:
    SnapshotSelector enum (Id/Ts/Ref) + metadata refs map + resolve
    (ts = latest snapshot <= ts; ref = refs map lookup); FFI takes
    selector JSON instead of i64. catversion bumped (parsed trees in
    rules). LIVE: 3 snapshots x 1000 rows — VERSION AS OF id -> 2000,
    TIMESTAMP AS OF between snapshots -> 1000, TAG AS OF 'main' -> 3000,
    pin wins over table option, stored view keeps pin, EXPLAIN pruning
    within pinned snapshot. NEW regress group time_travel (deterministic:
    parse validation, VALUES/setop/locking non-applicability, plain-table
    no-op, rule round-trip via selector-named plan errors). 44 cargo
    tests + 4 regress groups.
    SPEC: 02-sql-surface.md §4 amended (implemented surface + LALR(1)
    rationale, original sketch kept as Superseded); RULES §3 grammar
    plumbing row added (kwlist/parse_clause/parse_relation/analyze/
    parsenodes RTE field/catversion).
  - slice 20 v2 delete application: plan_scan separates delete files
    (manifest content 1/2) into ScanPlan.delete_tasks (never data tasks);
    load_row_deletes reads them eagerly at scan_new -> LoadedDeletes
    (positions_by_file from (file_path,pos) parquet + EqDeleteMatch
    tuples with the delete's sequence number). rows_open slices
    LoadedDeletes per data file: position deletes exact per-file;
    equality deletes only when delete seq > data seq and, in M1, only
    for UNPARTITIONED data files (partitioned + eq deletes -> loud
    Config error; partition-scoped application deferred). ParquetRows
    gained open_with_deletes + absolute-position tracking across batches
    (batch_start_abs) + eq columns join the INTERNAL read set when not
    projected. FFI end-to-end test: mock catalog + REAL position-delete
    parquet -> rows 1,3 (id=2 removed) through pg_datalake_rows_open.
    LIVE: PyIceberg 0.12 table.delete() is COW (rewrites the data file,
    no delete files) — the live transcript proves COW-rewrite read compat
    (analytics.events_del: 10 appended -> delete -> FDW reads 8 rows,
    pre-delete snapshot via FOR VERSION AS OF reads 10); live MoR
    fixtures come from Spark/Trino at the M1 gate or our M2 writer.
    GOTCHA: lakekeeper serves per-table config s3.endpoint=http://s3:9000
    (container-scoped) which BEATS pyiceberg client/table properties —
    host-side pyiceberg writes must run inside the compose network
    (docker run --network pgdatalake_default with the venv mounted at
    its absolute path; see scripts note in retro). SPEC: 06-execution.md
    §3 amended (slice 20 amendment). 46 cargo tests + 4 regress groups.
  - slice 21 conformance read-half (M1 gate item): full primitive type
    matrix decodes to PG-input-compatible TEXT — NEW decodes: decimal128
    -> canonical unscaled/scale text, uuid (parquet FLBA(16)) ->
    canonical hyphenated text, date/date64 -> ISO, time32/64 ->
    HH:MM:SS.ffffff, binary -> \x-prefixed bytea hex (previously raw
    ints / bare hex that date_in/time_in/bytea_in reject — caught by
    TDD red tests). Conformance fixtures:
    scripts/gen_conformance_fixtures.py (PyIceberg writer, MUST run
    inside the compose network — see lakekeeper per-table
    s3.endpoint gotcha at slice 20) writes analytics.types_all (full
    type matrix, 3 rows incl. all-NULL) + analytics.parts_multi
    (day/month/bucket[8]/truncate[2]/identity spec, 12 files). LIVE:
    types_all reads with correct SQL values (uuid/decimal/date/time/
    bytea round-trip, all-NULL row); parts_multi identity pruning
    EXPLAIN examined 12 / pruned 6 / tasks 6, non-identity predicates
    correct-conservative. INTEROP QUIRK: pyiceberg 0.12 writes Iceberg
    fixed[L] as ASCII hex STRINGS (spec wants FLBA(L)) — documented in
    05-types.md §1; Trino/Spark-written fixed reads fine. PyIceberg
    table.delete() is COW (slice 20 note). 49 cargo tests + 4 regress
    groups.
  - slice 22 Trino interop (M1 gate item DONE): trino service in
    docker-compose (profile "trino", host port 18081 — 8080 is taken on
    the dev host; config fixtures/trino-catalog.properties: REST catalog
    over lakekeeper + native S3 filesystem). LIVE: Trino writes
    day-partitioned trino_smoke.events (bigint/varchar/decimal/
    timestamp/boolean), DELETEs id=2 via an MoR POSITION-DELETE file
    (verified content=1 in $manifests); our FDW reads exactly ids {1,3}
    — agreeing with Trino's own view — and FOR VERSION AS OF matches
    Trino on every historical snapshot (Trino's two-phase commit writes
    an empty first append snapshot; both engines read 0). INTEROP FIX:
    Trino writes single-valued optional manifest fields as plain Int
    inside the Avro union (sort_order_id hit it live) — manifest parser
    accepts Int/Long singles in unions alongside pyiceberg's shapes.
    SPEC: 10-testing.md §2 interop-gate status amended (outstanding:
    Spark eq-deletes, Trino codec matrix, Trino-reads-our-writes at M2).
  - slice 23 M1 TPC-DS gate + rescan fix: dataset = TPC-DS SF=10 via
    duckdb dsdgen (21 tables, 3.1GB parquet on S3; store_sales 28.8M
    rows). PRUNING EVIDENCE: identity-partitioned store_sales (18,240
    files) -> WHERE ss_sold_date_sk = 2450992 examined 18,240 / pruned
    18,230 / tasks 10, exact count, 3.6s incl. planning over 18k
    manifests; negative control documented (unpartitioned dsdgen output
    cannot bounds-prune — every file spans the full date range; correct,
    and the motivating case for optimize). TIMINGS: full scale q1 1.2s /
    q2 18.0s / q3 >60min terminated (CPU-bound; row-by-row FDW
    transport = known M1 bottleneck, fix path slice 24 columnar batch
    reads); sampled-scale (1% facts) ALL 10 QUERIES GREEN 0 errors
    (q1 1.33s q2 .36s q3 .21s q4 2.31s q5 7.26s q6 .5s q7 2.97s q8 1.0s
    q9 7.26s q10 5.3s). Engine findings: (1) FIXED
    ReScanForeignScan re-planned per rescan (catalog round-trip +
    manifest fetch) — merge-join inner rescans made join queries take
    minutes; now rewind-only, session reused, snapshot pinned at open.
    (2) RECORDED correlated-subplan pathology (q6: per-outer-row full
    foreign scan of item, no index available) — fix path remote
    parameterization or hash-subplan conversion (M2/M3). Evidence
    ledger: src/test/lakehouse/fixtures/gate-evidence-m1-tpcds.md.
    TPC-DS helpers: duckdb dsdgen -> /tmp/tpcds parquet ->
    scripts-driven PyIceberg write (transient scripts in /tmp during the
    gate run; worth promoting to scripts/ if the fixture is reused).
    Cross-engine: proximadb session (proximadb-e4) benchmarking the
    same fixture (native/DataFusion/DuckDB) against these numbers;
    results land in 10-testing.md when delivered.
  - M1 CLOSED 2026-11-19: cold-room ceremony COLDROOM M1 ALL GREEN
    (scripts/coldroom-smoke-m1.sh — fresh clone + fresh fixture stack:
    build, setup/upstream/lakehouse suites incl. time_travel, cargo,
    PyIceberg fixture generation in-network, live read-path/
    time-travel-mechanics/type-matrix verification). Retro at
    specs/lakehouse/retro/m1.md (8 findings incl. row-transport
    bottleneck, rescan re-plan bug, LALR(1) FOR-clause lesson, 2
    operational gotchas). Tag milestone/m1. NEXT: M2 per the slice
    plan above (slice 24 first: Rust Parquet writer).
  - **Next slices**: M1 conformance
    gate (Trino-written tables, TPC-DS subset, perf vs doc 10 §5) ->
    smoke ceremony -> M1 retro + tag milestone/m1.
- **M2 slice plan (written 2026-11-19, before M1 close; per doc 07 §2
  commit protocol + doc 01 §2 module layout)**:
  - slice 24 — DONE 2026-11-19 (writer.rs, CI green): Rust Parquet
    writer: columnar batch accumulator -> arrow RecordBatch -> parquet
    bytes -> StorageIo put; deterministic <rows>-<seq>.parquet naming.
  - slice 25 — DONE 2026-11-19 (catalog/commit.rs, CI green): REST
    commit protocol: commitTable with requirements [AssertTableUUID,
    AssertRefSnapshotId(main, base), + all spec requirement types],
    conflict classification (409 -> RestError::Conflict, the only
    retryable class) + bounded re-plan retry; wire shapes pinned against
    installed pyiceberg 0.12 (requirements tag "type", updates tag
    "action", explicit-null snapshot-id). 59 cargo tests incl. scripted
    mock-catalog 409->re-plan->success and budget-exhaustion paths.
  - slice 26 — DONE 2026-11-19 (tableam.c + extension SQL, regress
    green): `USING lakehouse` TableAM skeleton. PLAN AMENDMENT: the
    pg_am.dat [core-allowlist] row + catversion bump turned out
    unnecessary — runtime CREATE ACCESS METHOD (in lakehouse--1.0.sql)
    registers the AM as an ordinary pg_am row, zero core edits. Scan
    callbacks DELEGATE to the FDW session pipeline (shared lakehouse.h:
    LakehouseOptions + lakehouse_OpenSession + BuildSelectorJson moved
    out of fdw.c statics); identity lives in lakehouse.managed_tables +
    lakehouse.attach_table (slice 27 wires it at CREATE time; drop-GC
    via sql_drop event trigger). Regress tableam.sql: AM round-trip,
    relam wiring, default_table_access_method, unattached-scan clean
    error, unreachable-endpoint 08000 fail-fast parity with the FDW,
    INSERT 0A000, re-attach idempotence, DROP hygiene.
  - slice 27 — INSERT, SPLIT into two commits (2026-11-20 planning,
    after slice 26 showed the gap): the missing piece between slice 24
    (parquet data files) and slice 25 (commitTable client) is ICEBERG
    METADATA WRITING:
    - 27a — DONE 2026-11-20 (snapshot_write.rs, CI green): manifest
      avro + manifest-list avro writers (spec field-ids included —
      Trino/pyiceberg readers map by field-id, needed for the slice-29
      gate) + write_snapshot assembler (writes both avros, returns the
      AddSnapshot payload Snapshot + written keys). 62 cargo tests incl.
      the END-TO-END anchor: BatchWriter -> data file -> write_snapshot
      -> metadata merge -> OUR plan_scan reads rows back; manifest and
      manifest-list round-trip through the existing readers.
    - 27b-i — DONE 2026-11-20 (ffi.rs write surface + session.rs +
      writer.rs row-JSON, CI green): pg_datalake_write_new/push/flush/
      commit/free. write_table_open resolves identity + field-ids from
      the table's schema; commit_append flushes -> write_snapshot ->
      commit_with_retry with requirements rebuilt from each fresh
      loadTable (appends are additive — only the base pin moves).
      Typed row-JSON cells (bare values + {"d"}/{ "t"}/{"ts"}/{"dec"}/
      {"u"}/{"b"} wrappers) so the C side sends datum arithmetic, not
      text formatting. Anchor: mock-catalog write->commit->capture
      commitTable body->merge snapshot->scan OUR OWN rows back (1,2).
    - 27b-ii — DONE 2026-11-20 (tableam.c + ffi fixes, LIVE-VERIFIED):
      tuple_insert accumulates typed row-JSON into the FFI write
      session (backend-local hash keyed by relid; RegisterXactCallback:
      PRE_COMMIT commits each session via pg_datalake_write_commit,
      ABORT/COMMIT frees + destroys the hash), own-writes overlay
      (scan yields staged datum copies after file rows — verified
      live: staged_visible = 3 pre-commit), datum arithmetic for
      date/time/ts (PG epoch 2000 -> unix epoch via JULIAN offsets),
      numeric/uuid/bytea as canonical text, unsupported types rejected
      at first insert. LIVE CYCLE: INSERT 3 rows (incl NULL) -> commit
      -> fresh PG session reads 3 rows -> PYICEBERG reads
      {1,2,3}/{alpha,beta,None} -> TRINO reads 1|alpha,2|beta,3|NULL.
      THE SLICE-29 GATE (Trino reads a table we wrote) IS PROVEN LIVE
      ahead of the ceremony. Offline regress anchors updated: INSERT
      now stages (identity error 0A000 unattached; 08000 unreachable at
      session open).
      WIRE-CONVENTION GOTCHAS (all caught live by cross-engine reads,
      recorded for doc 03): (1) manifest avro field-ids go ONLY on
      record FIELDS — never on records/arrays; missing field-id on the
      data_file FIELD made Trino+pyiceberg both fail ("Missing required
      field: data_file"); (2) real LIST arrays need "element-id"
      (partitions 508, split_offsets 133, equality_ids 136); (3) kv
      metric arrays need "logicalType": "map" (pyiceberg's reader
      requirement); (4) parquet footers MUST carry PARQUET:field_id
      metadata (WriteColumn.field_id) — Trino/pyiceberg map parquet
      columns by field id and read EMPTY columns without it; (5) S3
      keys are bucket-relative in StorageIo (location path prefix must
      be re-appended on writes); (6) Lakekeeper's commitTable response
      carries in-band metadata + explicit-null config (tolerant decode).
      27c — DONE 2026-11-20 (managed CREATE, live-verified): PURE-SQL
      lifecycle now works end-to-end: CREATE TABLE ... USING lakehouse
      -> attach_table -> lakehouse.create_remote(regclass) (schema from
      the relation's tupdesc, namespace auto-created, 409 -> 'exists')
      -> INSERT. Verified live across PG + pyiceberg ({7,8}/{seven,
      None}) + Trino (7|seven, 8|NULL) on the freshly created table.
      RestCatalog gained create_namespace/create_table (spec wire
      shapes; tolerant decode). Remote CREATE went from manual pyiceberg
      helper to SQL.
  - slice 28-prep — DONE 2026-11-20 (DECISION + core patch): managed
    DDL UX decided by thought experiment over three alternatives
    (one-statement CREATE ... WITH vs multi-statement attach flow vs
    wrapper functions) under 5-year scenarios (pg_dump/restore, ORM/
    migrations, multi-AM ecosystem, upstreamability): ONE-STATEMENT
    CREATE ... USING lakehouse WITH (...) wins; attach/create_remote
    stay as internals. [core-allowlist] TableAmRoutine.
    relation_reloptions hook + table_am_reloptions() dispatch in
    reloptions.c (extract path) + tablecmds.c (CREATE/ALTER validate
    path, AM resolved from stmt->accessMethod or
    default_table_access_method). No catversion impact. Our AM parses
    endpoint/warehouse/token/table/s3_*/snapshot_id into a fixed-order
    NUL-packed bytea (rd_options); identity resolves WITH-options
    FIRST, attach row second — attach_table optional when WITH carries
    the identity. Regress: WITH-options scan fail-fast, unknown-key +
    missing-table rejections (22023), NULL-options guard (segfault
    caught live). Upstream regress suite green on the patch. Upstream
    patch + proposal being filed from the core commit.
  - slice 28 — UPDATE/DELETE as COW (rewrite affected files, reuse
    slice-20 delete reading for concurrent reads), TRUNCATE (empty
    snapshot commit); regress vs heap parity.
  - slice 29 — MERGE + M2 gate: Trino reads a table WE wrote (the
    compose trino service is already in place for exactly this);
    smoke ceremony + tag milestone/m2.
  - Principle to honor throughout: one catalog, one visibility
    (00-goals #3) — managed tables are ordinary pg_class rows; the
    remote catalog is source of truth; local reconciliation via
    metadata_ttl.
- Core-allowlist patches so far: meson_options.txt,
  src/backend/access/meson.build, root Makefile (each committed with
  `[core-allowlist]` rationale).

## First actions for the next session (M1)

1. Read `specs/lakehouse/README.md` → `00-goals.md` → `01-architecture.md`;
   naming amendment notes in RULES §4 / 01 D1-D2.
2. Verify CI green on GitHub Actions (datalake-ci on lakehouse-dev).
3. Start M1 per `12-roadmap.md`: REST catalog client + metadata model in
   `pgdatalake-core`, `lakehouse_fdw` read path, S3 fixtures in tests.
   TDD per RULES §8; weekly upstream sync per RULES §2 before starting.

## Gotchas found while exploring the tree

- `cc`/`python3` in PATH resolve to anaconda/conda-forge toolchains; conda gcc
  has its own sysroot (no readline headers). Build with `CC=/usr/bin/gcc`.
  meson picks anaconda python — works, but mind version-sensitive tooling.
- meson setup FAILS if autoconf configure artifacts exist in the source tree
  (GNUmakefile conflict check) — stay meson-only, or distclean first.
- `pgindent` needs `pg_bsd_indent` in PATH + perltidy; pg_bsd_indent can be
  built manually (gcc -I src/include -I build/src/include
  src/tools/pg_bsd_indent/*.c build/src/port/libpgport.a
  build/src/common/libpgcommon.a). perltidy not yet installed.
- `pg_regress --inputdir <dir>` expects `<dir>/sql/` and `<dir>/expected/`
  subdirs; psql column headers are CENTERED — take expected output from the
  results file after eyeballing correctness.
- **meson test runs the INSTALLED binaries** (`tmp_install`) — after changing
  backend C code you MUST `meson install --destdir build/tmp_install` (and
  rerun `--suite setup` after a catversion bump) or the suite silently tests
  old code; this cost one CI cycle on slice 19 (stale expected file with a
  pre-fix error message). `make lakehouse-check` now installs first.
- Upstream `pg_mod_args`/`backend_mod_code` are defined *after*
  `subdir('access')`; `src/backend/access/lakehouse/meson.build` assembles
  module args from earlier-defined components (see file comment).
- meson cannot `link_with` a custom_target: link the static lib via
  `link_args` + `link_depends` (see lakehouse/meson.build).
- `#[no_mangle]` is an unsafe attribute: crate uses `deny(unsafe_code)` with
  `allow` only in `ffi.rs` (RULES §4).
- Fixture creds are fake (`lakehouse-test`/`lakehouse-test-password` etc.);
  never use real cloud credentials outside OIDC-federated CI (RULES §6).

## Rules

`specs/lakehouse/RULES.md` is binding: allowlist-limited core edits, branch
layout, style, secrets, TDD (§8), smoke-test ceremony (§9). Follow it.
