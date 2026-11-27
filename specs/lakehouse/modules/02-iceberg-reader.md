# Anvai Lake Iceberg reader and batch module

**Read** [shared contracts](00-integration-contracts.md). **Own** `scan.rs`, `manifest.rs`, `parquet.rs`, `puffin.rs`, reader-side Rust `ffi.rs` exports and `batch_fill.c`. The catalog module supplies a pin and authorized storage. FDW and TableAM derive required output/qual columns and own PostgreSQL slot lifetime.

For repeated dashboard work, use the [shared published reader and BI reuse contract](02a-bi-reuse.md).

## Implementation

1. Resolve fields and delete keys by Iceberg ID, including historical schema and per-manifest partition spec. Preserve requested SQL slot order. Empty output projection can still require delete keys and physical row ordinals.
2. Build a deterministic task list for one pin. Establish each applicable delete set before yielding results. Use sequence/partition/path applicability for supported formats. Route unknown required features to a feature/decode error rather than a partial scan. Unsupported pruning evidence retains data and leaves the SQL residual predicate active.
3. Feed projected column ranges incrementally into typed batches. Preserve absolute file ordinals for the COW consumer even when deletes, projection or page pruning skip rows. Expose ROW/BATCH, EOF and ERROR as distinct outcomes. Keep typed nulls and borrowed buffers valid until advance or close. Report cancellation and release resources without panic-based control flow.
4. Share the same fallible cursor behavior for full-column scans, delete inputs and projected scans. Expose row groups decoded, delete inputs applied, bytes consumed and FFI batch counts. Keep semantics fixed while tuning range coalescing, parallel scheduling and page indexes.

## Independent test package

- Exercise the shared `cow-delete-safe` vector using actual Parquet plus a delete fixture. Verify survivor IDs and ordinals. Cover position/equality/DV applicability, rename and nullable evolution, unknown delete features, large integer comparison and empty projection.
- A cold `LIMIT 1` stops before fetching later row-group data. A later decode error fails a full scan and does not invalidate a short scan that never reaches it. Measure peak live decoded bytes as row groups increase.
- Compare a full drain with the reference reader's typed values and multiset. Use the same task plan across worker modes. Test error and cancellation while a batch or native handle is live.


## FDW scan ownership

An FDW scan registers its native session and row reader with the executor
query memory context before opening the session. Normal EndForeignScan
unregisters the callback and releases the same resources. A context reset
after SQL error or cancellation closes the row reader before freeing the
session. Rewind replaces the row reader and retains the selected session.

The stock and fork COUNT fixture forces a SQL projection error and
cancellation with the ordinary row reader open, rolls back to a savepoint
and reads the same pinned rows on the same backend. Debug evidence counts
both context-reset releases. PostgreSQL checks interrupts before and
after ordinary session open, task open and batch decode. Interruption
while a blocking provider request is in flight still depends on the
provider deadline.
