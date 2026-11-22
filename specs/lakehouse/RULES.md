# RULES.md — Fork Discipline

Binding rules for all work in this repository.

## 1. Git layout

- `main` = fork base branch: `upstream/master` plus the `specs/` documentation
  commits. Nothing but spec/doc updates and upstream sync merges is ever
  committed directly to `main` — all feature and build work happens on
  `lakehouse-dev` or below. Sync: `git fetch upstream && git merge upstream/master`
  (a merge commit is expected and fine; upstream never touches `specs/`, so
  conflicts are rare).
  *(Superseded 2026-11-19: originally `main` was to be an exact ff-only mirror
  of `upstream/master`, but the fork was created with a default `master` branch
  that already carried the specs commits. Renamed `master` → `main` instead of
  maintaining a separate mirror branch; HANDOFF.md delegated this decision.)*
- `origin` = `anvai-labs/pgdatalake` (public open-core repository). `upstream` = `postgres/postgres`.
- `lakehouse-dev` = integration branch (our commits only, always based on latest
  `main`).
- Feature work: `feature/<milestone>-<topic>` off `lakehouse-dev`
  (e.g. `feature/m1-rest-catalog`). Rebase onto `lakehouse-dev` before merge;
  merge with `--no-ff` so integration commits are visible.
- Tags of upstream releases: keep `REL_19_RC2` etc. as fetched from upstream —
  never re-tag.

### 1a. Branch & promotion discipline (amended 2026-11-20 — the binding graph)

*(Born from an incident: work on feature branch `cf5631-port` was pushed for
days while only upstream's no-op CI ran, because `datalake-ci` triggered on
`lakehouse-dev`/`main` only — and edits were nearly cross-pasted between
branches. The graph below is now binding.)*

```mermaid
gitGraph
    commit id: "upstream/master"
    branch main
    commit id: "fork base + specs/"
    branch lakehouse-dev
    commit id: "integration"
    branch feature/x order: 2
    commit id: "slice work"
    commit id: "slice work"
    checkout lakehouse-dev
    merge feature/x tag: "--no-ff + datalake-ci GREEN"
    checkout main
    merge lakehouse-dev tag: "milestone boundary"
    checkout main
    merge upstream/master tag: "weekly sync"
```

| Rule | Statement |
|---|---|
| B1 | `upstream/master` merges ONLY into `main`. Never into feature branches. |
| B2 | `main` merges into `lakehouse-dev` at milestone boundaries (and for urgent upstream fixes). |
| B3 | Feature branches (`cf5631-port`, `feature/*`) merge ONLY INTO `lakehouse-dev` (`--no-ff`). **Never cherry-pick or paste between feature branches** — if two branches need a shared fix, it merges to `lakehouse-dev` first and both rebase. |
| B4 | A feature branch lives until its slice lands in `lakehouse-dev`, then it is deleted. Long-lived parallel work streams are forbidden — they are how the CI gap (below) and merge rot happen. |
| B5 | Promotion = merge, never rebase, across branch tiers. Rebase is only for a feature branch onto `lakehouse-dev` before ITS OWN merge. |
| B6 | Upstream sync (`git fetch upstream && git merge upstream/master` into `main`) is blocked while `datalake-ci` is red on `lakehouse-dev`. |

**GitHub enforcement (2026-11-21):** branch protection is set via the API
on `lakehouse-dev` and `main` — direct pushes are REJECTED; all landings go
through PRs with **squash or rebase (fast-forward-style) merge** allowed and
merge commits disabled (`allow_merge_commit: false`). 0 required approvals
(single-dev repo — the PR is the audit trail and the CI trigger, not a review
gate); force pushes and deletions denied. Docs-only PRs skip CI by path
filter, so required-status-check enforcement is deliberately OFF until the
CI strategy gains a docs-only sentinel check (doc 10 §1a).

**CI validity rule (closes the incident):** the ONLY signal that counts is the
`datalake-ci` workflow, verified as
`gh run list --workflow datalake-ci`. Upstream's `CI for PostgreSQL` runs are
no-ops on this fork (6–19 s "success" jobs) and must NEVER be cited as green.
`datalake-ci` now triggers on **every branch** (path-filtered), so a feature
branch push is itself gated before it can merge (B3/B5).

## 2. Upstream sync cadence

- Sync `main` from `upstream/master` **at least weekly** and before starting any
  milestone. Rebase `lakehouse-dev` onto it; resolve conflicts immediately — they
  only grow.
- When a conflict touches an allowlist file (§3), fix it in the *allowlist patch
  commit* (via `git rebase`), not as a new compensating commit.

## 3. Core-allowlist (files outside the iceberg feature dirs)

The ONLY core/upstream files we may modify. Each modification is its own commit
prefixed `[core-allowlist]` with a rationale line and a `Spec: specs/lakehouse/…`
reference.

| Area | Files |
|---|---|
| bootstrap catalogs | `src/include/catalog/pg_am.dat` (+ catversion bump) |
| grammar | `src/backend/parser/gram.y` (iceberg `FOR … AS OF` clause, `USING lakehouse FORMAT iceberg`), `src/backend/nodes/*` if a new node needed |
| grammar plumbing | `src/include/parser/kwlist.h`, `src/backend/parser/parse_clause.c` (+ `.h`), `src/backend/parser/parse_relation.c`, `src/backend/parser/analyze.c`, `src/include/nodes/parsenodes.h` (one RTE field), `src/include/nodes/primnodes.h`, `src/include/catalog/catversion.h` (bump with parsed-tree changes) — lakehouse time travel `FOR … AS OF` resolution only (slice 19 amendment 2026-11-19); keep each touch to a small, commented hook |
| reloptions | `src/backend/access/common/reloptions.c`, `src/include/utils/rel.h` |
| GUCs | *(superseded 2026-11-20)*: extension-defined GUCs via `DefineCustom*Variable` in `_PG_init` are the sanctioned seam — no core edit exists or is permitted for lakehouse GUCs. The historical `guc_tables.c`/`postgresql.conf.sample` allowlist entry is RETIRED before first use (spec 15 P2). |
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
  **Enforced locally by the `.githooks/pre-commit` gate** (activate once:
  `git config core.hooksPath .githooks`) — it aborts commits whose staged
  lakehouse C is not pgindent-clean (via `scripts/pgindent_lakehouse.sh`,
  which supplies the fork typedefs that plain pgindent lacks and repairs
  the known UTF-8 string-literal mangling) or whose staged Rust fails
  `cargo fmt`/`clippy` workspace lints (pedantic). Verification only, never
  auto-rewrite; `--no-verify` is the emergency bypass. CI re-checks the
  Rust side; the C side has no CI formatter — the local gate IS the
  enforcement.
- Rust code: `cargo fmt`, `cargo clippy -D warnings` on
  `pgdatalake-core`; `#![forbid(unsafe_code)]` everywhere except the single
  `ffi.rs` boundary module; no panics across the FFI boundary — every exported
  fn returns `Result<_, PGError>`-shaped C structs with message buffers owned by
  the caller-allocated arena.
- Error discipline: core-side uses `ereport(ERROR, (errcode(ERRCODE_…),
  errmsg(…)))` with new SQLSTATE class `LD0` (lakehouse) — register via
  `errcodes.txt` addition (allowlist: `src/backend/utils/errcodes.txt`).
- SQL-visible names: schema `lakehouse` (functions), `pg_lakehouse` (views),
  extension and TableAM `lakehouse` (`USING lakehouse FORMAT iceberg`), GUC
  prefix `lakehouse.`, SQLSTATE class `LD0`. Nothing else may add
  `lakehouse*` or `datalake*` names (collision discipline with future
  extensions); per-format identifiers stay inside the core and its FORMAT
  layer — `iceberg*` SQL-visible names are reserved until a concrete need
  arises, and must not appear ad hoc.
  *(Amended 2026-11-19: originally the schema/extension/GUC surface was
  `iceberg*`-named; generalized to `lakehouse`/`datalake` so Delta, Parquet,
  Avro and engine integrations (DataFusion, Polars) can share the surface.
  Supersedes the naming in the original docs; see 01-architecture.md.)*

## 5. Commit & PR rules (this repo)

- Short imperative subjects: `iceberg: add REST catalog token refresh`,
  `[core-allowlist] gram.y: FOR VERSION AS OF clause`.
- Every commit that changes behavior includes or updates the relevant spec
  section in `specs/lakehouse/` — specs are living docs, not artifacts; a code
  change that contradicts a spec must update the spec in the same commit.
- CI must be green before merge into `lakehouse-dev` (fast tier, doc 10 §1) —
  verified via `gh run list --workflow datalake-ci` (§1a CI validity rule).
- Never force-push `main` or `lakehouse-dev` after they're shared.

## 6. Secrets & data

- No credentials in specs, code, fixtures, logs, or commit messages. Fixture
  buckets use fake creds; real cloud credentials only via CI OIDC federation.
- Don't copy data files or metadata.json from real lakes into the repo —
  generate fixtures with `scripts/gen_fixtures.py` (M1).

## 7. Spec amendment process

Material deviations from specs/lakehouse require: update the doc, note the old
decision as superseded with date and reason in a `## Superseded` section (never
delete history of a decision), reference in the commit message.

## 8. TDD discipline (mandatory, every feature/bugfix — no exceptions)

The Red–Green–Clean loop is the unit of work. Religious observance:

1. **RED** — write the failing test FIRST: a `src/test/regress/lakehouse/*.sql`
   + `.out` pair, an isolation test, a TAP test, or a `cargo test` case. Run it,
   watch it fail, paste the failure into the commit branch notes.
2. **GREEN** — the *minimum* implementation that makes it pass. No speculative
   abstractions, no drive-by refactors.
3. **CLEAN** — `pgindent` the C, `cargo fmt && cargo clippy -D warnings` the
   Rust, re-run the full fast CI tier.
4. **Commit rule**: test and implementation live in the SAME commit. A commit
   that changes behavior without a test in the same diff does not get pushed to
   `lakehouse-dev`. Bug fixes get a test that reproduces the bug first.

Exceptions (documented, not silent): build/bootstrap scaffolding (M0 meson
files, docker fixtures) may ship without tests, but each milestone gate (§9)
must close the coverage.

## 9. Smoke-test ceremony (mandatory at EVERY release / milestone checkpoint)

Nothing is "done" until this ceremony runs, in order, and its transcript is
committed. It happens at each roadmap gate (M0–M5 in `12-roadmap.md`) and at
every tagged release.

1. **Cold-room smoke test** — from a FRESH clone, no warm caches:
   - `git clone` the repo, check out the release/branch ref
   - `make lakehouse-up` (SeaweedFS S3 + Lakekeeper + Azurite + fake-gcs fixtures)
   - `meson setup build -Ddatalake=enabled && ninja`
   - upstream regress suite via `meson test --suite setup && meson test
     --suite regress` (must stay 100% green — a fork that breaks upstream
     behavior fails the smoke test even if iceberg tests pass).
     *(Superseded 2026-11-19: originally this step ran autoconf `make check`;
     the autoconf and meson builds cannot coexist in one source tree — meson
     setup errors on configure-generated files — and meson is the primary
     build system. The upstream `parallel_schedule` suite runs unchanged
     under `meson test --suite regress`; a legacy autoconf tree can still run
     `make check`.)*
   - `make lakehouse-check` + the milestone's conformance subset (`doc 10`)
   - one MANUAL psql transcript per milestone's headline feature (M1: attach +
     query a fixture-S3 table; M2: CREATE USING lakehouse FORMAT iceberg + round-trip DML; …) —
     humans see the feature work, not just CI.
2. **Acceptance-gate audit** — walk the milestone's gate checklist from
   `12-roadmap.md` item by item; each item gets PASS/FAIL with evidence
   (command output, test counts, perf numbers vs the `doc 10 §5` gates).
3. **Spec sync** — reconcile code vs `specs/lakehouse/`: every deviation becomes
   either a code fix or a §7 amendment in this checkpoint. Specs that say what
   code doesn't do are treated as bugs.
4. **Upstream drift check** — `git fetch upstream`, count new upstream commits,
   note any that touch allowlist files (§3); record rebase risk for next cycle.
5. **Tag & release notes** — tag `milestone/mN` (pre-M5) or `vX.Y.Z`
   (releases); release notes MUST state: what works, known gaps, perf-gate
   table, upstream base commit, and the smoke-test transcript hash.
6. **Retrospective** — write `specs/lakehouse/retro/mN.md`: decisions kept vs
   superseded, core-allowlist patch count (target: never grows without a
   §3 review), test counts, what the TDD loop caught late, process changes for
   the next milestone. Commit all artifacts in one
   `checkpoint(mN): smoke ceremony + retro` commit.

Skipping any step voids the checkpoint: the milestone is not done, the release
does not ship, and the next milestone does not start.
