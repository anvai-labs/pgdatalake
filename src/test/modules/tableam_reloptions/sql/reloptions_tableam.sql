CREATE EXTENSION tableam_reloptions;

-- the AM's own option, accepted and validated at CREATE
CREATE TABLE relopt_test (a int) USING tableam_reloptions
	WITH (test_option = 42);
SELECT relkind, relname FROM pg_class WHERE relname = 'relopt_test';

-- unknown AM keys are rejected at CREATE, not silently ignored
CREATE TABLE relopt_bad (a int) USING tableam_reloptions
	WITH (test_option = 42, bogus_option = 1);

-- heap-known keys still validate through the delegating parser
CREATE TABLE relopt_ff (a int) USING tableam_reloptions
	WITH (test_option = 1, fillfactor = 90);
DROP TABLE relopt_ff;

-- the same AM-specific key on a PLAIN heap is rejected (effective-AM
-- dispatch: no silent ignore, Alvaro's 2020 concern)
CREATE TABLE relopt_heap (a int) WITH (test_option = 42);

-- relcache round-trip: ALTER SET re-parses through the AM's callback
ALTER TABLE relopt_test SET (fillfactor = 80);
SELECT c.relname, o.option_name, o.option_value
	FROM pg_class c, pg_options_to_table(c.reloptions) o
	WHERE c.relname = 'relopt_test';

-- options must satisfy the (possibly new) AM after SET ACCESS METHOD
CREATE TABLE relopt_swap (a int) WITH (test_option = 42);
ALTER TABLE relopt_swap SET ACCESS METHOD heap;
ALTER TABLE relopt_swap SET (test_option = 1);
DROP TABLE relopt_swap;

DROP TABLE relopt_test;
DROP ACCESS METHOD tableam_reloptions;
DROP EXTENSION tableam_reloptions;
