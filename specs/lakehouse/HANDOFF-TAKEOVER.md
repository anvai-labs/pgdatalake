# TAKEOVER HANDOFF — stability & performant execution (2026-11-21)

This document is the single entry point for a fresh session taking over
the lakehouse fork. Read it top to bottom; every claim links to its
authoritative source.

For release qualification, start with the
[2026-11-21 reassessment](16-release-reassessment-and-scale.md). It records
outstanding COUNT, publication and resource-boundary gates and updates
the performance work order below.

## 1. Vision (one paragraph)

pgdatalake is a Postgres fork that natively speaks Apache Iceberg: FDW
for external Iceberg tables + a `USING lakehouse` TableAM for managed
tables (spec'd in `specs/lakehouse/`, especially `00-goals.md` and
`01-architecture.md`). The near-term mission is **stability and
performant execution**: a green CI gate on every merge, honest perf
receipts against the TPC-DS SF=10 fixture, and the executor-tier work
(doc 08) that removes the row-transport bottleneck. Longer term: M3
close-out (MVCC models, ADLS/GCS, branches/tags) then M4 (compaction,
optimize, ANALYZE). Authoritative roadmap: `specs/lakehouse/12-roadmap.md`;
progress tracker: `specs/lakehouse/PROGRESS.md` (keep it updated in the
same commit as every landing).

## 2. Current state (verified 2026-11-21)

- Branch `origin/lakehouse-dev` = `d91c2599321` ("P0b3: row-group-deferred
  parquet decode (#28)"). Everything below is LANDED and CI-green:
  - parquet/arrow **60.1.0** lockstep (#14), object_store **0.14.2**
    (#15, RUSTSEC cleared) — spec 15 §6.3a/§6.5
  - smart path-filtered CI + cargo-audit gate (#15/#16)
  - **P0b**: ranged/cached/projection-pruned parquet opens (#17)
  - **P0b4 slice A**: FDW COUNT(*) elision from manifest record_counts
    (#20) — 28.8M rows in 22–42 ms, exact, zero data-file access
  - **P0b4 slice B**: ledger `row_count` column, extension v1.2
    (#26 lineage; live-verified fresh-install + 1.1→1.2 upgrade)
  - **P0b4 slice C**: planner-hook COUNT elision for MANAGED tables
    (#27/`count_elision.c`) — offline, MVCC-exact, declines on
    published segments/NULL row_counts (never fabricates)
  - **P0b3**: row-group-deferred decode — lazy batch source (#28)
  - decomposition: fdw.c → fdw_plan/fdw_exec/core (#23);
    publisher.c + lakehouse_tableam.h extracted (#26)
  - guards: DV-decline + MVCC revalidation for elision (#22)
- Extension version now **1.2** (`lakehouse--1.2.sql` base +
  `1.1--1.2` upgrade; both live-verified).
- Open PR: **#29** (docs-only queue sync) — needs a human merge
  (docs-only path has no CI gate; auto-merge unavailable).

## 3. Performance receipts (honest, §5.2 draft)

Measured 2026-11-21, release build (`build-release2`),
TPC-DS SF=10 fixture (SeaweedFS + Lakekeeper, store_sales 28.8M/73
files), fresh instance, serial runs:

| Query | Result | Note |
|---|---|---|
| Elided `count(*)` store_sales | **34.9–37.2 ms** ×3, exact 28,800,991 | manifest record_count Const, no data files read; was **11:29.8** pre-elision |
| q1 solo | **>600 s TIMEOUT**, CPU-bound, no wait events | M1 receipt said 1.2 s — NOT reproduced; whole 2.87M-row aggregation ships through FDW row transport (EXPLAIN receipts in git history / §5.2 below) |
| q2/q3 | measurement in flight (`/tmp/q23_measure.sh`, 600 s caps) | record when done |

Elided COUNT stability: three consecutive fresh runs 34.9/35.7/37.2 ms,
exact every time. EXPLAIN VERBOSE: `Result / Output: '28800991'::bigint`
— no scan node.

## 4. Next steps (priority order)

1. **Record q2/q3 medians** into §5.2 when `/tmp/q23_measure.sh`
   completes (script is running against port 52022; results in the
   task output). Cancel any leftover server-side queries first —
   client-side timeouts do NOT cancel server-side queries, and stacked
   duplicates poison timings (learned the hard way this session).
2. **Slice C follow-through**: the managed-table elision currently
   declines when published segments exist. Next: manifest-based count
   for published rows (open session, read record_counts — online) so
   managed COUNT elides in both states; guarded by the same exactness
   contract.
3. **Aggregate pushdown / exec tiers** (doc 08): the q1/q3 class is
   transport-bound — 2.87M-row aggregations ship every row through the
   FDW per-row path into a hash aggregate. The fix is pushing the
   partial aggregate into the scan (partial-agg batch fill), not more
   transport tuning.
4. **P0b2**: page-index-level pruning via the ranged-GET counter
   (spec 15 §6.3a).
5. **tableam_write.c extraction**: scripts + failure modes documented
   (`/tmp/split_final.py`, `/tmp/build_write_file.py`; decl-vs-def
   anchors, col-0 brace convention, +1 overcut). Needs a fresh session;
   boundaries now explicit.
6. **M3 close-out** → M4 (roadmap §12).

## 5. Operational discipline (hard-learned, non-negotiable)

- **One runner, one instance**: never overlap perf/measure scripts on
  one Postgres instance; server-side queries survive client timeouts
  and stacked duplicates poison every timing.
- **Gated merges**: CI must be green (`datalake-ci` workflow) before
  any squash-merge; use auto-merge. The one ungated merge (#15) shipped
  a broken audit gate caught only by luck.
- **Pre-commit gate**: pgindent (fork typedefs, via
  `scripts/pgindent_lakehouse.sh`) + cargo fmt/clippy run on commit —
  never bypass with --no-verify except emergencies.
- **Smart CI**: path-filtered — docs-only PRs skip code jobs; workflow
  changes exercise their own gates.
- **cwd discipline**: several file-corruption incidents this session
  came from relative-path edits executed from the wrong directory.
  Prefer absolute paths in scripts; verify `pwd` before multi-step
  edits.
- **Extraction mechanics** (for any file split): name-anchored
  def-vs-decl finder (a forward declaration terminates with `;` before
  `{`), col-0 `}` as the top-level close, never `+1` past a col-0
  close (it eats the next block's leading comment). See
  `/tmp/split_final.py` for the working version.

## 6. Spec & doc pointers

- Vision/goals: `specs/lakehouse/00-goals.md`
- Architecture: `specs/lakehouse/01-architecture.md`
- Testing & perf gates: `specs/lakehouse/10-testing.md` (§5 gates,
  §5.1 cross-engine reference, §5.2 internal receipts)
- Elision design: `specs/lakehouse/15-staged-writes-and-caching.md`
  §6.6 (mechanism slices A–D table)
- Roadmap: `specs/lakehouse/12-roadmap.md`
- M1 gate evidence: `src/test/lakehouse/fixtures/gate-evidence-m1-tpcds.md`
- Rules (branching, C style, security): `specs/lakehouse/RULES.md`

## 7. Live fixture stack (for any online test)

`make lakehouse-up` brings up SeaweedFS :9000, Lakekeeper :8181
(health /health), Azurite, fake-gcs. TPC-DS SF=10 tables live in the
`tpcds` namespace (store_sales 28.8M/73 files). pyiceberg access from
the host venv `/tmp/pdi-venv`; **must pass
`s3.addressing-style: path`** in the catalog config (virtual-host
style 403s against SeaweedFS). The docker network is
`pgdatalake_default`; host loops to `localhost:9000` only for PG-side
access — pyiceberg in docker uses `s3:9000`/`lakekeeper:8181`.
