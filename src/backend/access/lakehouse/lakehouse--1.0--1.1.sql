

-- P3 (spec 15 §7.2, D1-rev): the staged-delta store IS a Postgres
-- table — durability, crash recovery, replication and basebackup are
-- inherited from the heap/WAL machinery; the publish ledger is the
-- row's own columns (published / published_snapshot_id). Payload is a
-- typed batch (the accumulator representation, never re-encoded).
CREATE TABLE lakehouse.lakehouse_delta_segments (
    seg_id                 bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    relid                  oid NOT NULL,
    payload                bytea NOT NULL,
    tombstones             jsonb,
    staged_at              timestamptz NOT NULL DEFAULT now(),
    published              boolean NOT NULL DEFAULT false,
    published_snapshot_id  bigint
);
CREATE INDEX lakehouse_delta_unpublished_idx
    ON lakehouse.lakehouse_delta_segments (relid, staged_at)
    WHERE NOT published;
REVOKE ALL ON lakehouse.lakehouse_delta_segments FROM PUBLIC;
