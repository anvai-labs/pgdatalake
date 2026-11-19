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
  - **Next slices**: partition-spec-aware pruning (resolve opaque partition
    JSON + bounds predicates, doc 06 §2) -> FFI boundary (pg_datalake_* C
    structs, panic-free, RULES §4) + FDW C glue -> first `CREATE FOREIGN
    TABLE` SQL surface + regress tests -> EXPLAIN.
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
