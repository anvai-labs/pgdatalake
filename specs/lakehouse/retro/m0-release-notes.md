# milestone/m0 release notes

**Tag:** `milestone/m0` · **Base:** upstream `postgres/postgres` master
`61f6021b110` (PG 20 dev, `CATALOG_VERSION_NO 202611121`) · **Branch:**
`lakehouse-dev`

## What works

- **Build**: meson feature `-Ddatalake=auto|enabled|disabled` builds the
  `pgdatalake-core` Rust workspace (`src/backend/access/lakehouse/core/`,
  static lib + cbindgen header `pg_datalake_core.h`) and the `lakehouse`
  extension module; `auto` silently disables without cargo/cbindgen.
- **SQL**: `CREATE EXTENSION lakehouse` → `SELECT lakehouse.version()`
  returns the core version string from the Rust C ABI
  (`pg_datalake_core_version()`).
- **Naming**: format-generic lakehouse surface (schema/extension `lakehouse`,
  `USING lakehouse FORMAT iceberg` reserved syntax per specs
  02-sql-surface.md); Iceberg is the first FORMAT layer.
- **Tests** (TDD per RULES §8, RED observed before implementation):
  upstream regress suite **243/243** green under `meson test --suite regress`;
  lakehouse regress group green (`src/test/regress/lakehouse/`);
  `cargo test`/`clippy -D warnings`/`fmt` green.
- **Fixtures**: `make lakehouse-up` brings up SeaweedFS S3 (:9000, bucket
  `warehouse`), Lakekeeper REST catalog (:8181, `/health` ok), Azurite
  (:10000), fake-gcs (:4443) with fixture-local fake credentials.
- **CI**: `.github/workflows/datalake-ci.yml` (build `-Ddatalake=enabled`,
  cargo checks, docker fixtures, upstream + lakehouse suites); upstream
  `pg-ci.yml` untouched.
- **Cold-room smoke ceremony**: fresh clone → fixtures → build → upstream
  suite (243/243) → `make lakehouse-check` → manual psql transcript: ALL
  GREEN (transcript hash in the tag annotation).

## Known gaps

- Storage/catalog functionality starts at M1 (REST client, metadata model,
  FDW read path); `lakehouse.version()` is the only SQL surface.
- `make lakehouse-conformance` (M1) and `make perf-lakehouse` (M4) are stubs.
- CI is ubuntu-24.04 / gcc-14 only; the `running` meson setup is unexercised.
- Fixture S3 store is SeaweedFS (MinIO removed its Docker Hub images
  2026-09); production MinIO support unaffected (doc 10 §1 Superseded note).
- GitHub default branch on origin is still `master` (rename to `main` pending
  a manual settings change; `origin/master` still exists).

## Perf gates (doc 10 §5)

Not measured in M0 — all gates become measurable from M4; no regressions
possible yet (no storage paths exist).

## Upstream base & drift

- Fork base: `61f6021b110`; **4 new upstream commits** at checkpoint time,
  one touching an allowlisted file (`src/backend/postmaster/autovacuum.c`,
  commit `4226f1f01ad`) — zero rebase risk while we don't patch that file;
  scheduled sync per RULES §2 before M1 starts.

## Evidence

- Smoke transcript: `specs/lakehouse/retro/m0-smoke-transcript.txt`
- Retro: `specs/lakehouse/retro/m0.md`
