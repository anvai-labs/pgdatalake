-- telemetry policy layers (spec 19 §4a-gate): SUSET = admin policy via
-- config / ALTER SYSTEM / ALTER DATABASE / ALTER ROLE; superusers may
-- flip per session. Users cannot SET it (error is session-authorized).
-- another test may have created the extension; force a library load into
-- THIS backend either way: extension GUCs register per backend at load.
CREATE EXTENSION IF NOT EXISTS lakehouse;
SELECT lakehouse.version();

SHOW lakehouse.telemetry;
SET lakehouse.telemetry = off;
SHOW lakehouse.telemetry;
RESET lakehouse.telemetry;
SHOW lakehouse.telemetry;
SHOW lakehouse.telemetry_retention;
