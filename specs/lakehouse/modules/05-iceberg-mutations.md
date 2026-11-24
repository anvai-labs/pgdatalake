# Anvai Lake direct Iceberg mutations

**Read** [shared contracts](00-integration-contracts.md) and [delete encodings](../18-anvai-lake-dml.md#3-how-deletes-and-updates-reach-iceberg). **Own** `writer.rs`, `snapshot_write.rs`, writer-side FFI and direct COW orchestration extracted from the write path in `tableam.c`. Do not modify local staged-read visibility or define a second publisher. Use the same writer for publisher appends.

## Implementation

1. Separate append from rewrite. Verify table UUID/ref, accepted schema/spec/version, and writer profile before output. Use immutable file names and produce descriptors from completed writer metrics. Retry create-only object uploads using explicit operation evidence when supported.
2. First deliver a fail-closed delete-bearing COW guard, including when the base changes before retry. For supported rewrites, read live rows from the pinned file using the reader's applicable delete set and physical ordinals. Apply local tombstones to those ordinals. An UPDATE's after-image and removal of the old version commit together.
3. For the initial safe profile, use exact expected-base CAS for mutation. Introduce rebase only after proving target paths, schema/spec and their applicable delete sets have not changed. Never replay a user predicate on new rows as an invisible retry. Preserve compatible append manifests and historical files. Do not mark objects published before the catalog verifies the commit.
4. Defer authored position/equality deletes, DV writes, staged updates and distributed row locking behind independent profile tests. Reuse the shared remote commit outcome protocol. Unknown direct completion is surfaced for reconciliation.

## Independent test package

The first guard refuses the whole source snapshot when the scan plan contains delete work. A concurrent change to the source pin aborts the overwrite, including changes that preserve the target data path. The CI interop smoke exercises both rules through SQL and rereads the result in Trino. [Fixture](../../../src/test/lakehouse/fixtures/cow-delete-guard.sh)

- Existing deleted row plus a separate target delete, then a change to the file's delete set with its data path still live. Use `cow-delete-safe`. Verify no resurrection and no change after a declined rewrite.
- Two writes touching the same file, independent-file changes, abort after upload, table replacement and compatible append retry. Check exact historical and current row IDs with an independent reader.
- Width/churn canaries report files/bytes rewritten, retained manifests and logical rows affected. Any future DV writer needs explicit lineage, maintenance, and cross-engine decode gates.

### Boundary contracts (qualified COW profile — implemented)

The routing contract, error-code table and the operations/fragmentation
caveat live in [spec 18 §3a–§3c](../18-anvai-lake-dml.md#3a-direct-cow-routing-contract-qualified-profile--implemented-boundary-tested).
The boundary fixture pins them through SQL against an independent Trino
oracle: [cow-boundary-check.py](../../../src/test/lakehouse/fixtures/cow-boundary-check.py)

| # | Case | Asserts |
|---|---|---|
| T1 | managed DELETE/UPDATE of published rows, no concurrency | exact read-back; Trino agrees at the new snapshot; a pinned older snapshot still returns the original rows |
| T3 | mixed transaction: staged INSERTs + tombstoned published row | one overwrite at commit; every row exactly once; control: mutating a same-transaction row → `55000` |
| T5 | zero-row DELETE/UPDATE | no new snapshot, rows unchanged |
| T6 | FDW write-refusal control | INSERT/UPDATE/DELETE on an external foreign table → core `0A000`, zero catalog requests |
| T7 | savepoint rollback of a COW mutation | row restored, no tombstone residue, no new snapshot |
| T8 | delete-bearing snapshot refusal | `0A000` before any catalog request |
| T9 | committed-unpublished rows (staged append) | direct UPDATE/DELETE refuses `55000` ("cannot resolve row identity") until the publisher drains |

Concurrent-conflict (`40001`) and neighboring-delete-resurrection controls
remain in [cow-delete-guard.sh](../../../src/test/lakehouse/fixtures/cow-delete-guard.sh).

### Future bands (gated, unchanged)

- **DV write (slice V3)** — [spec 14 §3](../14-v3-conformance.md): Rust
  puffin/roaring encoder, first-row-id handling, `write_mode=merge_on_read`
  plumbing, cross-engine decode gates. The reader side (V1) is landed, so a
  DV-writing table is readable before it is writable by us.
- **FDW external writes** — per-statement direct Iceberg COW/DV commits
  without publication coalescing; changes the FDW's read-only-by-
  construction posture and the stock package surface, so it carries its own
  packaging gates ([06-sql-and-packaging](06-sql-and-packaging.md)).
- **Compaction / optimize (M4)** — reclaims the COW fragmentation that
  [spec 18 §3b](../18-anvai-lake-dml.md#3b-operations-fragmentation-and-maintenance-explicit-caveat)
  documents; until then the caveat binds operators, not correctness.
