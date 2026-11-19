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
  - **Next slices**: EXPLAIN pruned-task evidence polish (show skipped
    count explicitly) -> parallel scans (ForeignScan parallel API) ->
    time travel (FOR TIMESTAMP/VERSION, doc 02 §4, allowlisted grammar
    patch) -> M1 conformance gate (Trino-written tables, TPC-DS subset,
    perf vs doc 10 §5).
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
