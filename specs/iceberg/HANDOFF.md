# HANDOFF.md — State for the Next Session

*Written 2026-11-19. Working directory: `/home/vsingh/code/postgres`.*

## What exists

- Repo: `/home/vsingh/code/postgres`, clone of `postgres/postgres` master
  (`61f6021b110`, PG 20 dev, `CATALOG_VERSION_NO 202611121`).
- `origin` = `https://github.com/anvai-labs/postgres` (PRIVATE, gh account
  `vjsingh1984`, token has admin:org). `upstream` = `postgres/postgres`.
  `main`-equivalent (`master`) pushed and verified.
- `specs/iceberg/` — full design doc set (README has the reading order; start
  there). Key decisions already made, do not relitigate without a spec revision:
  - **D1**: managed tables = TableAM (`USING iceberg`); external = FDW;
    both share one Rust core `pgiceberg-core` (arrow-rs, object_store, REST).
  - **D2**: Rust core as static library via C ABI; DuckDB embedding rejected
    (see `11-related-work.md`).
  - Iceberg v3 target, v2 delete-read compatibility; REST catalog is the
    primary, Glue/Nessie/Hadoop secondary; JDBC deferred.
  - Transaction model fully specified in `07-transactions.md` (overlay,
    optimistic commit with requirements, journal 2PC for non-REST catalogs,
    honest mixed-transaction limits).
- Nothing implemented yet. No Rust workspace, no code, no CI.

## First actions for the new session

1. Read `specs/iceberg/README.md` → `00-goals.md` → `01-architecture.md`.
2. `git checkout -b iceberg-dev` (off `master`), push to origin
   (RULES.md §1 — also create `main` if desired: `git branch -m master main`
   is NOT done yet; decide and align RULES.md §1 wording with reality).
3. Execute M0 from `12-roadmap.md`: Rust workspace skeleton at
   `src/backend/access/iceberg/core/`, cbindgen header step, meson
   `-Diceberg=auto`, CI with MinIO+Lakekeeper fixtures, stub
   `iceberg_version()` SQL function.
4. Build PG locally first to establish the baseline:
   `./configure --prefix=$PWD/build/pg20 && make -j16 world && make check`.

## Gotchas found while exploring the tree

- This checkout has NO `PG_VERSION` at root (git worktree style layout, version
  from configure: master = 20dev).
- `configure` requires autoconf 2.69 if regenerating (don't regenerate; use
  the committed `configure`).
- Upstream now uses meson as primary build system — put the Rust build step in
  meson first, autoconf Makefile second (or autoconf-only if we keep effort
  minimal for M0).
- `src/backend/access/table/` is the TableAM core; `src/backend/access/heap/`
  is the reference AM implementation to copy structure from.

## Rules

`specs/iceberg/RULES.md` is binding: allowlist-limited core edits, branch
layout, style, secrets. Follow it.
