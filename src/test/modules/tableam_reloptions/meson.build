# Copyright (c) 2026, PostgreSQL Global Development Group

tableam_reloptions_sources = files(
  'tableam_reloptions.c',
)

if host_system == 'windows'
  tableam_reloptions_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'tableam_reloptions',
    '--FILEDESC', 'tableam_reloptions - test table AM for the reloptions callback',])
endif

tableam_reloptions = shared_module('tableam_reloptions',
  tableam_reloptions_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += tableam_reloptions

test_install_data += files(
  'tableam_reloptions.control',
  'tableam_reloptions--1.0.sql',
)

tests += {
  'name': 'tableam_reloptions',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'reloptions_tableam',
    ],
  },
}
