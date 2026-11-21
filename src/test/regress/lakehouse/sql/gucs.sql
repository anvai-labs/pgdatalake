-- another test may have created it; force a library load into THIS
-- backend either way: extension GUCs register per backend at load time
CREATE EXTENSION IF NOT EXISTS lakehouse;
SELECT lakehouse.version();

-- lakehouse GUC surface (spec 15 §7.2 P2): extension-defined via
-- DefineCustom*Variable in _PG_init — NO core edit (RULES §3 note).
-- Security posture: filesystem-path knobs are SUSET (RULES §6: cache/
-- delta dirs carry table data); tuning knobs are USERSET.

-- defaults
SHOW lakehouse.write_mode;
SHOW lakehouse.target_file_size;
SHOW lakehouse.publish_max_lag;
SHOW lakehouse.cache_max_bytes;

-- session set/reset round-trip
SET lakehouse.write_mode = 'staged';
SHOW lakehouse.write_mode;
RESET lakehouse.write_mode;
SHOW lakehouse.write_mode;

SET lakehouse.target_file_size = '256MB';
SHOW lakehouse.target_file_size;
RESET lakehouse.target_file_size;

SET lakehouse.publish_max_lag = 120;
SHOW lakehouse.publish_max_lag;
RESET lakehouse.publish_max_lag;

SET lakehouse.cache_max_bytes = '512MB';
SHOW lakehouse.cache_max_bytes;
RESET lakehouse.cache_max_bytes;

-- invalid enum value rejected loudly
SET lakehouse.write_mode = 'bogus';

-- invalid numeric rejected loudly (min 32MB, max 4GB — kB bounds)
SET lakehouse.target_file_size = '0';
SET lakehouse.target_file_size = '31MB';
SET lakehouse.cache_max_bytes = '5GB';
SET lakehouse.target_file_size = '4GB';
SHOW lakehouse.target_file_size;
RESET lakehouse.target_file_size;
