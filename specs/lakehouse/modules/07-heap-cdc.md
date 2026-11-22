# Anvai Lake heap CDC candidate

**Read** [shared contracts](00-integration-contracts.md) and [core pilot gates](08a-core-release-gates.md). **Own** decisions and tests for a candidate PostgreSQL heap ingestion connector and checkpoint store. The candidate can proceed independently of the fork or TableAM. Confirm catalog/writer dependencies before selecting a reusable upstream component. Choose the runtime and repository only after the first pilot evidence.

## Implementation

1. Start with normal heap transactions and a bounded initial copy tied to a logical-decoding consistent point or an equivalent source outbox. Keep source identity, transaction identity/location, ordinal and schema version. Advance a durable checkpoint only after downstream responsibility has been durably recorded. Define what the checkpoint protects across restarts and failover.
2. Deliver an append-only change stream first. Keep transaction boundaries and order per source stream, preserve deletes as events, and define INSERT/UPDATE images and replica-identity requirements. The event log exposes changes. A separate current-state projection must qualify key uniqueness, update/delete application and schema evolution.
3. Reuse catalog classification, writer validation and idempotent publication receipt semantics. A capture retry must preserve the same operation identity. Do not compete with PostgreSQL heap DML for correctness or redefine source transaction acknowledgment.
4. Monitor slot/outbox lag, WAL/disk retention, backfill progress, throughput, CAS conflicts and remote visibility. Apply bounded buffering and safe backpressure. Preserve pending acknowledged source events until recovery or a documented operator decision.

## Independent test package

- Fake ordered transactions plus initial-copy boundary, abort, reconnect, duplicate delivery and schema changes. Verify exact results and checkpoints.
- Restart after remote success before checkpoint confirmation. Prove idempotent handling with the same receipt contract. Test unsupported replica identities before data loss can occur.
- Compare with customer CDC and a qualified alternative on the same source workload. Measure commit overhead, lag, retained WAL, recovery and setup effort. Launch this lane after pilot demand and source-correctness evidence.
