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

lakehouse_sources = files('lakehouse_core.c')

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
  ['lakehouse.control', 'lakehouse--1.0.sql'],
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
    'sql': ['version'],
    'test_kwargs': {'timeout': 120},
    'deps': [lakehouse],
  },
}
