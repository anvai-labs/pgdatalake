# 14 — Iceberg v3 Conformance & v2→v3 Migration

*(2026-11-20, after M2 slice 28. Written from the v3-alignment pass: this
doc is the single authority for what "Iceberg v3 conformance" means for
this fork — which v3 features are required, which are gated loudly until
implemented, and how managed tables migrate v2→v3. Other docs reference
here instead of re-stating v3 policy.)*

## 1. Conformance target and the governing principle

**Target: the current Iceberg v3 table spec.** v2 remains a supported,
fully-tested read surface; v3 is the required end state. The goal doc
(00-goals #7) already commits to v3 — what was missing is the operational
definition. This doc supplies it.

**Governing principle — never misread.** A construct this fork does not
implement must produce a **loud error at plan/decode time**, never a
plausible-looking result. Pushdown bugs that eliminate files lose rows
silently (13-review #4); the same logic applies to format features: a v3
table whose deletion vectors we do not apply must not scan — it must
error. Every gate below is written against that principle.

## 2. Feature status matrix

Tiers: **R** = required for v1 read correctness · **W** = required for
managed writes · **D** = deferred, gated loudly until landed ·
**X** = explicitly out of scope (recorded so absence is a decision).

| # | v3 feature | Tier | Status |
|---|---|---|---|
| 1 | Metadata model: schemas, snapshots (+`first-row-id`), refs, partition specs, sort orders, summary maps | R/W | **Parsed** — `catalog/metadata.rs` decodes `format-version: 3` and the v3 snapshot fields; unit-tested |
| 2 | Read gate: v3 tables before semantics are covered | R | **Loud gate** — `scan_table` rejects format-version 3 with a message naming this doc. Lifted by slice V1/V2 (§3) |
| 3 | Manifest `content` routing: DATA / POSITION_DELETES / EQUALITY_DELETES applied | R | **Done** — slice 20 + 13-review #5 (v2 delete application, live-verified); **unknown content values (e.g. DV = 3) fail the manifest decode loudly** (`manifest.rs` — no silent fallback) |
| 4 | **Deletion vectors, read** — Puffin `deletion-vector-v1` blobs; manifest DV fields (`referenced-data-file`, `content-offset`, `content-size-in-snapshot`); per-file bitmap build, bloom-filter acceleration | R | **Missing — slice V1.** Until then gate #2 keeps v3 tables out |
| 5 | Deletion vectors, write (`merge_on_read` mode, `lakehouse.write_mode`) | W | **Missing — slice V3.** Managed writes are COW-only today (slice 28), which is v3-legal: DVs are optional for writers |
| 6 | Equality deletes — v3 DEPRECATES them (DVs replace) | R (read) | Applied (v2 interop requirement). **Re-scope note:** the planned `equality_delete` write mode (doc 06 §3) is interop-only; DVs are the v3-oriented MoR vehicle |
| 7 | Row lineage (`first-row-id` propagation on writes) | D | Parsed only; the writer does not maintain lineage. v3 writers SHOULD propagate — slice V4 alongside the writer's v3 emission |
| 8 | Schema default values (`initial-default` / `write-default`) | D | Not modeled. Contained: defaults only appear in v3 tables, which gate #2 refuses to read; managed writes are v2-only. Slice V5 |
| 9 | `timestamp_ns` / `timestamp_ntz_ns` | D | Mapped with documented truncation policy (doc 05 §2, `lakehouse.nanosecond_mode`); **loud** when ns ≠ 0 and mode = error |
| 10 | `unknown` type | D | Parsed; opaque column; managed-write type gate rejects (loud) |
| 11 | `variant` | D | Parsed; `jsonb` read via the v2 unshredded path (doc 05 §3); write deferred |
| 12 | `geometry` / `geography` | X→D | **Not parsed — schema decode errors loudly** (safe). Telemetry/GIS is not a fork use case today; landed only if a partner workload demands it |
| 13 | Metadata transport details (gzipped metadata.json — Lakekeeper serves `.gz`) | R/W | **Works** (observed live since M1) |
| 14 | v3 snapshot summary keys (DV counts, lineage fields) | R | Parsed as an opaque map — nothing lost, no action |

## 3. Hardening slices (severity-ranked; entry point = the blockers table in 13)

**V0 — DONE 2026-11-20 (this pass).**
- Loud v3 read gate (#2): `scan_table` rejects format-version 3 before any
  row can be returned (TDD: `v3_tables_are_rejected_loudly_on_read`).
- Loud unknown-manifest-content decode (#3): a v3 DV manifest entry can
  never be planned as data (pre-existing behavior, verified this pass).

**V1 — DV read/apply (the correctness slice).** Parse the v3 manifest DV
fields, load Puffin `deletion-vector-v1` blobs from the snapshot summary's
DV registry, build per-file row-id bitmaps, apply at row-read time
(bloom-filter acceleration per doc 06 §3 uses file stats — later
refinement). Lift gate #2 to accept v3 tables **with DVs** once
pyiceberg-written DV fixtures verify equal ground truth. This is the last
read-correctness hole in the delete matrix (position/equality/DV).

**V2 — v3 fixture matrix.** pyiceberg 0.12 writes `format_version=3`
(pass the flag; it defaults to 2): COW-only v3 tables first (metadata,
plan, read back through our pipeline), then DV tables under V1. Gate #2
lifts to "v3 accepted" when the matrix is green. Cross-engine read gate:
Trino against our v3 fixtures.

**V3 — DV write mode + v3 emission.** `lakehouse.write_mode =
merge_on_read` emits DVs (Puffin blobs + manifest entries); the writer
learns the v3 manifest/list schema additions (`first-row-id` carriers,
DV fields) and honors the `format_version` server/table option (doc 02
§2) at `create_remote`. New managed tables may then be created as v3.

**V4 — row lineage maintenance.** Writer propagates `first-row-id` on
appends/rewrites per the v3 lineage rules; summary fields ride along.
Purely additive; late because nothing reads lineage yet.

**V5 — schema default values.** Model `initial-default`/`write-default`;
managed INSERT honors them only where PG-side column defaults are absent.
Decision recorded here: **PG defaults remain authoritative** for managed
tables; Iceberg defaults are honored on FDW reads of foreign v3 tables.

## 4. v2→v3 migration plan (tables, not code)

Migration is a **catalog-level table property change** (`updateTable`
with `AssertRefSnapshotId` + set `format-version` = 3), forward-only per
the Iceberg spec. Order of operations:

1. **Precondition — reader first.** No table under our management may be
   promoted to v3 before V1+V2 land: our own scanner must read v3 (with
   DVs) better than any other engine we certify, or the promotion creates
   a table our writer cannot safely rewrite (the COW commit path is
   format-sensitive: manifest schemas, DV handling on rewrite).
2. **Who may promote.** `create_remote` gains the table-level
   `format_version` option (doc 02 §2) once V3 lands — new tables can be
   born v3. Existing tables: a `lakehouse.set_format_version(regclass, 3)`
   helper (REST `updateTable` + assert-ref requirement, retry-safe like
   commit_append) is the ONLY sanctioned path — never a raw property
   edit that skips the liveness precondition.
3. **Consequences to verify at promotion time** (the helper asserts):
   - no open managed write transaction on the table (same two-phase rule
     as PREPARE — doc 07);
   - every engine we certify for the table is v3-capable (pyiceberg 0.12
     ✓, Trino ✓, this fork after V1/V2 ✓);
   - the table carries no constructs whose v3 semantics differ (none
     exist today: equality deletes are read-compatible; v3 keeps v2
     manifests readable).
4. **Post-promotion.** Managed writes continue to work unchanged: COW
   commits are format-agnostic modulo the manifest schema additions
   (V3 slice). Reads go through the lifted gate. Time travel unaffected
   (old snapshots keep their own metadata files; forward-only migration
   does not rewrite history).

**Not a migration target:** downgrading v3→v2. Forward-only; the helper
errors on `3 → 2`.

## 5. Cross-references

- Doc 00 #7 — goal statement, now points here for the operational definition.
- Doc 02 §2 — `format_version` option: honored at `create_remote` in slice V3.
- Doc 05 §2–3 — v3 types (nanos/unknown/variant) tiers; geometry/geography X→D.
- Doc 06 §3 — delete-mode matrix; DV rows are slices V1/V3; equality-delete
  write mode re-scoped as interop-only.
- Doc 07 §2 — COW/overwrite commit protocol; format-sensitivity of the
  commit path is the reason migration precondition #1 exists.
- Doc 10 — v3 fixture gate (slice V2) joins the M-gate matrices.
- Doc 12 — slices V1–V5 placed on the roadmap.
- Doc 13 — the "identify v3 deletion vectors before returning rows" item:
  addressed by V0 (loud gates) + V1 (application).
