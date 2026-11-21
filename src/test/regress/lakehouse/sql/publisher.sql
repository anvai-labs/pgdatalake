CREATE EXTENSION IF NOT EXISTS lakehouse;
SELECT lakehouse.version();
-- P4c (spec 15 §4): the publisher background worker runs (preloaded)
-- and appears as a distinct backend type. Offline acceptance: the
-- worker EXISTS and the loop survives (the live emission behavior was
-- proven by publish_staged in P4 slice 1/2).
SELECT lakehouse.start_publisher() AS registered;
SELECT pg_sleep(2);
SELECT count(*) >= 1 AS publisher_running
    FROM pg_stat_activity
    WHERE backend_type = 'lakehouse publisher';

-- the knobs exist and are postmaster-fixed
SHOW lakehouse.publisher_enabled;
SHOW lakehouse.publish_database;
SET lakehouse.publisher_enabled = false;
