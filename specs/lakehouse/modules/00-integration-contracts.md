# Shared module contracts

These are version 1 implementation contracts for Anvai Lake. Names in the interface tables describe transferred information. An implementation can use existing structs and symbols when they carry the same semantics. Land shared interfaces and tests before a consumer depends on them. [Module ownership](README.md) · [DML and visibility policy](../18-anvai-lake-dml.md)

## Identity and inputs

| Value | Required meaning |
|---|---|
| Bound table | Catalog profile and authority scope, warehouse/namespace/table locator, expected Iceberg UUID, ref, supported format and requested mode. A local PostgreSQL relation OID is resolved at the SQL boundary. It is not a durable cross-system identity. |
| Published pin | Table UUID, ref, concrete snapshot ID or explicit empty state, metadata identity and schema identity. The ref describes how the pin was selected. An existing pin survives ref movement while its retained objects remain readable. Snapshot IDs are opaque signed 64-bit identifiers. |
| Resolved read task | Published pin, immutable data object identity, applicable delete inputs, expected schema and projection by field ID, with requested slot order distinct from file order. Physical row ordinal is zero-based within the entire file. |
| Local operation | Stable ID and immutable payload scoped to a bound table. Committed segment IDs are unique within the ledger. Allocation order is not commit order. Local visibility comes from the PostgreSQL snapshot and command rules. |
| Publication claim | Stable operation ID, exact sorted segment membership and payload digest, table binding and lease generation. Claim persisted before remote work. Each attempt adds a verified base pin and expected ref value. A retry retains the operation ID and membership. |
| Publication receipt | Operation ID, result state, table binding and exact published snapshot if verified. Keep logical record counts separate from file-level row counts. Expose safe diagnostics only. |

A publication ID identifies a batch. Each member retains its own source transaction and command visibility. The ledger supplies the exact mapping from a publication ID to its source segments even when the reader's PG snapshot predates finalization. Proof must cover locally originated effects already in the base, including members invisible to the reader. A list of visible ledger rows alone cannot establish a compatible pin. See `mixed-visibility-claim` in the [shared vectors](fixtures/contract-v1.json).

## Seams and owners

| Provider → consumer | Input → output | Failure and fallback |
|---|---|---|
| Catalog/storage → reader, writer, publisher | Bound table + selector → authorized pin and scoped access. Claim + expected base → classified remote outcome. | Empty is explicit. Unsupported profiles fail before producing partial results. A read failure after rows have started is an error. An ambiguous mutation outcome requires reconciliation. |
| Reader → FDW, managed scan, direct COW | Pin + projection + optional safe pruning → ordered tasks, batches with typed columns/nulls, and source ordinals when required. | Pruning uncertainty retains data. Fallible iteration distinguishes batch, EOF and error. Missing required decode or identity information fails closed. |
| Managed ledger → publisher | Bound table + eligibility/budget → exact claim, durable payload and receipt state. | Stale workers cannot mark a claim complete. A claim survives worker exit and is visible to recovery. |
| Publication membership → managed scan | Published pin + exact operation ID → `included`, `absent` or `unknown`. | Unknown cannot be treated as absence. Try a provable compatible retained pin or fail. Preserve membership across restarts and retention while a supported read can depend on it. |
| Writer → publisher or direct TableAM | Immutable validated input and pin → staged file descriptors or verified commit outcome. | Rebase only compatible proposals. Preserve operation identity on retries. Keep object uploads out of the receipt's “published” state. |
| SQL/package → all native modules | Approved mapping and explicit mode → execution policy, typed inputs and supported result/error mapping. | Use PostgreSQL permissions and statement lifetime. Unsupported modes fail explicitly. A metadata shortcut must have the same result and permissions as its base scan. |

## Invariants at integration

1. A published-only read uses one concrete pin across planning of executable tasks, parallel workers, rescans and any optimization. Cached plans resolve current refs at execution. Include an explicit empty pin.
2. A managed current read pairs a PostgreSQL snapshot with a proven compatible published pin. Apply PG-visible operations absent from that pin exactly once. Suppress only operations proven included. If the pin includes a future local operation or membership is ambiguous, find a compatible retained pin or fail. Own earlier commands are handled under PostgreSQL rules.
3. A remote commit classifies as **committed** with verified snapshot, **rejected** with known no effect, or **unknown**. For staged appends each durable send intent authorizes one submission. After takeover, reconcile even if the worker could have died before sending. An absent marker on a read pin leaves the submission outcome unknown. A new attempt requires proof that the preceding attempt can have no effect. Receipt timeouts leave the operation recoverable. [Recovery decisions and fixtures](04a-publisher-recovery.md#when-a-second-append-is-safe)
4. Preserve PostgreSQL privileges and user mappings. No secrets appear in plans, worker coordination or test receipts. Authorization and cancellation are execution concerns. At the SQL boundary use normal permissions errors, feature errors for unsupported operations, `40001` for verified retryable mutation conflicts and `40003` where a direct caller must reconcile an unknown completion. A staged wait returns a receipt state instead of implying that an already committed local operation rolled back.
5. C owns PostgreSQL memory, interrupts and resource cleanup. Rust owns native handles and buffers. C registers cleanup as soon as it acquires a handle, closes readers before parent sessions, and never retains borrowed batch memory across advance or close. No PostgreSQL longjmp crosses an active Rust frame. Stop and cancel must release request, queue and memory budgets.
6. Track the time from client acknowledgment to claim, verified catalog publication and first fresh external read as separate measures. Batch and object-size limits bound resource use. Counts and performance telemetry never certify data correctness by themselves.

## Change protocol

When a shared seam changes, update this contract, add or revise a shared vector, and update both provider and consumer tests. Use a new manifest/ABI/ledger version when a change would reinterpret existing inputs. Keep the existing version readable or reject it with a clear upgrade path. Only one integration change edits the extension version/upgrade sequence, shared `ffi.rs` exports and `cbindgen.toml` at a time. Individual modules should add private helpers and tests under their assigned paths while waiting for a shared change.

Use [contract-v1.json](fixtures/contract-v1.json) as the first fixture vocabulary. Providers replay the inputs, consumers assert expected rows/outcomes, and qualification repeats them on a real catalog. Integer IDs are small in the fixture. Add Rust/C vectors above 2^53 and at signed boundaries before freezing any wire encoding. Keep product gates in the [roadmap](../12-roadmap.md) and the current implementation/test evidence in [PROGRESS](../PROGRESS.md).
