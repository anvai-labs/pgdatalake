-- lakehouse 1.1 -> 1.2 (P0b4 slice B, spec 15 §6.6): the delta ledger
-- carries each segment's exact row count, captured at INSERT time. COUNT
-- elision (slice A) and the publisher's coalescing read it; existing
-- pre-upgrade rows have NULL and are tolerated (elision declines on NULL).
ALTER TABLE lakehouse.lakehouse_delta_segments
    ADD COLUMN row_count integer;
