# Copyright (c) 2022-2026, PostgreSQL Global Development Group

subdir('brin')
subdir('common')
subdir('gin')
subdir('gist')
subdir('hash')
subdir('heap')
subdir('index')
subdir('nbtree')
subdir('rmgrdesc')
subdir('sequence')
subdir('spgist')
subdir('table')
subdir('tablesample')
subdir('transam')

# Apache Iceberg (specs/iceberg): optional feature; 'auto' disables itself
# when the Rust toolchain (cargo, cbindgen) is unavailable.
iceberg_opt = get_option('iceberg')
iceberg_build = iceberg_opt != 'disabled'
if iceberg_build
  iceberg_cargo = find_program('cargo', required: iceberg_opt == 'enabled')
  iceberg_cbindgen = find_program('cbindgen', required: iceberg_opt == 'enabled')
  iceberg_build = iceberg_cargo.found() and iceberg_cbindgen.found()
endif
if iceberg_build
  subdir('iceberg')
endif
