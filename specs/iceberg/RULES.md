# RULES.md — Fork Discipline

Binding rules for all work in this repository.

## 1. Git layout

- `main` = **exact mirror** of `upstream/master` at last sync. Never commit here.
  Fast-forward only: `git fetch upstream && git merge --ff-only upstream/master`.
- `origin` = `anvai-labs/postgres` (private). `upstream` = `postgres/postgres`.
- `iceberg-dev` = integration branch (our commits only, always based on latest
  `main`).
- Feature work: `feature/<milestone>-<topic>` off `iceberg-dev`
  (e.g. `feature/m1-rest-catalog`). Rebase onto `iceberg-dev` before merge;
  merge with `--no-ff` so integration commits are visible.
- Tags of upstream releases: keep `REL_19_RC2` etc. as fetched from upstream —
  never re-tag.

## 2. Upstream sync cadence

- Sync `main` from `upstream/master` **at least weekly** and before starting any
  milestone. Rebase `iceberg-dev` onto it; resolve conflicts immediately — they
  only grow.
- When a conflict touches an allowlist file (§3), fix it in the *allowlist patch
  commit* (via `git rebase`), not as a new compensating commit.

## 3. Core-allowlist (files outside the iceberg feature dirs)

The ONLY core/upstream files we may modify. Each modification is its own commit
prefixed `[core-allowlist]` with a rationale line and a `Spec: specs/iceberg/…`
reference.

| Area | Files |
|---|---|
| bootstrap catalogs | `src/include/catalog/pg_am.dat` (+ catversion bump) |
| grammar | `src/backend/parser/gram.y` (iceberg `FOR … AS OF` clause, `USING iceberg`), `src/backend/nodes/*` if a new node needed |
| reloptions | `src/backend/access/common/reloptions.c`, `src/include/utils/rel.h` |
| GUCs | `src/backend/utils/misc/guc_tables.c`, `postgresql.conf.sample` |
| autovacuum dispatch | `src/backend/postmaster/autovacuum.c` |
| storage hooks | `src/backend/catalog/storage.c` (relfilenode-less rels), `src/include/storage/smgr.h` |
| meson/build | `meson.build`, `src/backend/access/meson.build`, `config/` |

**Anything else must be solved with extension hooks** (TableAM, FDW API,
`planner_hook`, `ProcessUtility_hook`, `ExecutorStart/Run/End_hook`,
`object_access_hook`, smgr API v2). If you find yourself needing a core edit
outside the table, stop and redesign — or raise it in a spec revision first.

## 4. Code style

- C code follows upstream postgres style exactly (`src/tools/pgindent/` before
  every commit touching C; `--build` with typedefs file regenerated).
- Rust code: `cargo fmt`, `cargo clippy -D warnings` on
  `pgiceberg-core`; `#![forbid(unsafe_code)]` everywhere except the single
  `ffi.rs` boundary module; no panics across the FFI boundary — every exported
  fn returns `Result<_, PGError>`-shaped C structs with message buffers owned by
  the caller-allocated arena.
- Error discipline: core-side uses `ereport(ERROR, (errcode(ERRCODE_…),
  errmsg(…)))` with new SQLSTATE class `IC0` (iceberg) — register via
  `errcodes.txt` addition (allowlist: `src/backend/utils/errcodes.txt`).
- SQL-visible names: schema `iceberg` (functions), `pg_iceberg` (views),
  SQLSTATE class `IC0`, GUC prefix `iceberg.`. Nothing else may add
  `iceberg*` names (collision discipline with future extensions).

## 5. Commit & PR rules (this repo)

- Short imperative subjects: `iceberg: add REST catalog token refresh`,
  `[core-allowlist] gram.y: FOR VERSION AS OF clause`.
- Every commit that changes behavior includes or updates the relevant spec
  section in `specs/iceberg/` — specs are living docs, not artifacts; a code
  change that contradicts a spec must update the spec in the same commit.
- CI must be green before push to `iceberg-dev` (fast tier, doc 10 §1).
- Never force-push `main` or `iceberg-dev` after they're shared.

## 6. Secrets & data

- No credentials in specs, code, fixtures, logs, or commit messages. Fixture
  buckets use fake creds; real cloud credentials only via CI OIDC federation.
- Don't copy data files or metadata.json from real lakes into the repo —
  generate fixtures with `scripts/gen_fixtures.py` (M1).

## 7. Spec amendment process

Material deviations from specs/iceberg require: update the doc, note the old
decision as superseded with date and reason in a `## Superseded` section (never
delete history of a decision), reference in the commit message.
