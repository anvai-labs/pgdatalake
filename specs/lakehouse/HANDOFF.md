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
- **M0 done (see 12-roadmap.md M0 + retro/m0.md)**:
  - meson feature `-Ddatalake`, `src/backend/access/lakehouse/` (Rust core +
    C glue + `lakehouse` extension with `lakehouse.version()`).
  - meson-only build (autoconf tree is distcleaned; `make check` = upstream
    regress suite via `meson test --suite regress`; both builds can't coexist
    in one source tree).
  - TDD'd regress test `src/test/regress/lakehouse/` + cargo test; upstream
    regress suite 243/243 green under meson.
  - docker fixtures `src/test/lakehouse/docker-compose.yml` (SeaweedFS S3, Lakekeeper,
    Azurite, fake-gcs; fake creds). `make lakehouse-up/down`.
  - CI `.github/workflows/datalake-ci.yml` (additive; upstream pg-ci.yml
    untouched).
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
