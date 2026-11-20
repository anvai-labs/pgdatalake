/* src/test/modules/tableam_reloptions/tableam_reloptions--1.0.sql */
CREATE FUNCTION tableam_reloptions_handler(internal)
RETURNS table_am_handler
AS 'MODULE_PATHNAME', 'tableam_reloptions_handler'
LANGUAGE C;
CREATE ACCESS METHOD tableam_reloptions TYPE TABLE HANDLER tableam_reloptions_handler;
