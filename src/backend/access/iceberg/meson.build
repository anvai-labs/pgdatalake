# Copyright (c) 2026, Anvai Labs
#
# Apache Iceberg storage engine — C glue directory (specs/iceberg/01-architecture.md).
#
# M0: builds the pgiceberg-core Rust static library and the cbindgen-generated
# header. The `iceberg` extension module, its control/SQL files, and the
# regression tests are wired in by the iceberg_version() commit.

# Rust core: libpgiceberg_core.a + pgiceberg_core.h (specs/iceberg/01 §5).
# Rebuild scheduling is coarse (build_always_stale); cargo's own caching makes
# no-op rebuilds cheap. Fine-grained depfile tracking is planned for M1.
pgiceberg_core = custom_target('pgiceberg_core',
  output: ['libpgiceberg_core.a', 'pgiceberg_core.h'],
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
# as a dependency source keeps the custom_target ordering explicit.
pgiceberg_core_dep = declare_dependency(
  link_with: pgiceberg_core,
  sources: pgiceberg_core[1],
)
