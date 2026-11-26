# Copyright (c) 2026, Anvai Labs
#
# Format-generic lakehouse storage engine — C glue directory
# (specs/lakehouse/01-architecture.md).
#
# M0: builds the pgdatalake-core Rust static library, the cbindgen-generated
# header, and the `lakehouse` extension module exposing lakehouse.version().
#
# Note: pg_mod_args and backend_mod_code (src/backend/meson.build) are defined
# after subdir('access'), so this dir assembles its module args from the
# earlier-defined components directly. Keep in sync if pg_mod_args changes.

# Rust core: libpgdatalake_core.a + pg_datalake_core.h (specs/lakehouse/01 §5).
# Rebuild scheduling is coarse (build_always_stale); cargo's own caching makes
# no-op rebuilds cheap. Fine-grained depfile tracking is planned for M1.
pg_datalake_core = custom_target('pg_datalake_core',
  output: ['libpgdatalake_core.a', 'pg_datalake_core.h'],
  command: [
    python, files('rust_build.py'),
    '--manifest-dir', meson.current_source_dir() / 'core',
    '--target-dir', meson.current_build_dir() / 'rust-target',
    '--out-dir', meson.current_build_dir(),
  ],
  build_always_stale: true,
  build_by_default: true,
)

# The generated header must be compiled against by glue sources; declaring it
# as a dependency source keeps the custom_target ordering explicit. The static
# library itself is linked via link_args + link_depends below, because meson
# cannot link_with a custom_target.
pg_datalake_core_dep = declare_dependency(
  sources: pg_datalake_core[1],
)

# --- lakehouse extension module --------------------------------------------

# The stock PGXS package compiles the same reader sources.
lakehouse_sources = files(fs.read('stock/SHARED_SOURCES').split())
lakehouse_sources += files(
  'tableam.c',
  'publisher.c',
  'managed_credentials.c',
  'managed_lookup.c',
  'managed_send.c',
  'managed_count.c',
  'telemetry.c',
  'scan_stats.c',
  'fdw_analyze.c',
)

if host_system == 'windows'
  lakehouse_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'lakehouse',
    '--FILEDESC', 'lakehouse - format-generic lakehouse storage engine support',
  ])
endif

lakehouse = shared_module('lakehouse',
  [lakehouse_sources, generated_backend_headers_stamp],
  c_args: cflags_mod,
  include_directories: [postgres_inc, include_directories('.')],
  link_args: ldflags_sl + ldflags_mod + [
    meson.current_build_dir() / 'libpgdatalake_core.a',
  ],
  link_depends: pg_datalake_core[0],
  dependencies: backend_both_deps + os_deps + [pg_datalake_core_dep],
  kwargs: default_mod_args,
)

install_data(
  ['lakehouse.control', 'lakehouse--1.0.sql',
   'lakehouse--1.1.sql', 'lakehouse--1.0--1.1.sql',
   'lakehouse--1.2.sql', 'lakehouse--1.1--1.2.sql',
   'lakehouse--1.3.sql', 'lakehouse--1.2--1.3.sql', 'lakehouse--1.3--1.4.sql',
   'lakehouse--1.4--1.5.sql', 'lakehouse--1.5--1.6.sql', 'lakehouse--1.6--1.7.sql',
   'lakehouse--1.7--1.8.sql', 'lakehouse--1.8--1.9.sql',
   'lakehouse--1.9--1.10.sql', 'lakehouse--1.10--1.11.sql',
   'lakehouse--1.11--1.12.sql', 'lakehouse--1.12--1.13.sql',
   'lakehouse--1.13--1.14.sql', 'lakehouse--1.14--1.15.sql',
   'lakehouse--1.15--1.16.sql', 'lakehouse--1.16--1.17.sql',
   'lakehouse--1.17--1.18.sql', 'lakehouse--1.18--1.19.sql',
   'lakehouse--1.19--1.20.sql',
   'lakehouse--1.20--1.21.sql', 'lakehouse--1.21--1.22.sql', 'lakehouse--1.22--1.23.sql', 'lakehouse--1.23--1.24.sql'],
  install_dir: dir_data_extension,
)

# --- regression tests (specs/lakehouse/10-testing.md §3) --------------------
# Input/expected pairs live in src/test/regress/lakehouse/{sql,expected}/; the
# upstream parallel_schedule is untouched, so the upstream suite stays green
# and this group runs via `meson test --suite lakehouse`.
tests += {
  'name': 'lakehouse',
  'sd': meson.project_source_root() / 'src/test/regress/lakehouse',
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': ['version', 'scan_files', 'fdw', 'fdw_auth', 'time_travel', 'tableam', 'gucs', 'delta_table', 'staged_insert', 'count_elision', 'publisher'],
    'test_kwargs': {'timeout': 120},
    'deps': [lakehouse],
  },
}
