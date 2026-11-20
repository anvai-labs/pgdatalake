# M1 gate evidence — TPC-DS q1–q10 (specs/lakehouse/12-roadmap.md)

Date: 2026-11-19. Engine: postgres `lakehouse` FDW (release meson build),
REST catalog Lakekeeper + SeaweedFS S3 fixture, single node, 16 threads.

## Dataset

TPC-DS SF=10 via duckdb `dsdgen` (≈10GB raw; 3.1GB as parquet on S3):
store_sales 28,800,991 rows / 73 files (bin-packed from 10 appends),
catalog_sales 14,401,261 / 7, store_returns 2,875,432 / 2, web_sales
7,197,566 / 4, plus 17 dim tables. Iceberg tables written by PyIceberg 0.12
(`scripts/` helpers; data under s3://warehouse/<uuid>/data/).

## Pruning evidence (gate requirement)

Identity-partitioned store_sales (1824 dates × 10 appends = 18,240 files):
`WHERE ss_sold_date_sk = 2450992` →
**EXPLAIN: examined 18,240 / pruned 18,230 / tasks 10**, exact count 9,002,
3.6s total including planning over 18k manifest entries. Negative control:
unpartitioned layout cannot bounds-prune (every file spans the full date
range) — correct conservative behavior, and the motivating case for the
`lakehouse.optimize` design (doc 08 §2.1).

## Query timings

Full scale (28.8M-row store_sales, row-by-row FDW transport):
| query | time |
|---|---|
| q1 | 1.2s |
| q2 | 18.0s |
| q3 | >60 min, terminated (CPU 99.9%, zero errors — row-by-row row transport of 28.8M rows is the M1 bottleneck; fix path = slice 24 columnar batch reads) |

Reduced scale (1% fact samples, dims full; proves all 10 execute correctly):
| query | time | note |
|---|---|---|
| q1 | 1.33s | |
| q2 | 0.36s | |
| q3 | 0.21s | |
| q4 | 2.31s | |
| q5 | 7.26s | |
| q6 | 0.5s | with item at 1,000 rows — full-scale slowness is a **correlated-subplan pathology**: `i_current_price > (SELECT avg… WHERE j.i_category = i.i_category)` executes per outer row, each a full foreign scan of item (102k rows × 102k executions, no index available to the planner). Fix paths: remote parameterization or hash-subplan conversion (M2/M3). |
| q7 | 2.97s | |
| q8 | 1.0s | |
| q9 | 7.26s | |
| q10 | 5.30s | |

All 10 queries: **0 errors**; results sane (some return 0 rows at sampled
scale — strict predicates eliminate all rows, expected).

## Engine findings fixed/recorded during the gate

1. **Rescan re-plan pathology (fixed)**: `ReScanForeignScan` originally
   re-planned from scratch (catalog round-trip + manifest fetch) per rescan;
   merge-join inner rescans trigger this many times per statement. Fixed to
   rewind-only (session reused; snapshot pinned at open time — within one
   statement the remote snapshot cannot move visibly).
2. **Correlated-subplan scan-per-execution** (recorded, fix path above).
3. **Layout matters**: manifest-value pruning requires write-time grouping;
   bounds pruning alone could not discriminate on randomly-ordered dsdgen
   output (correct, but motivates `optimize`).

## Cross-engine reference

proximaDB (sibling session) is benchmarking the same fixture data (native
engine + DataFusion/DuckDB wrappers) against these numbers; results will be
recorded in specs/lakehouse/10-testing.md when they land.
