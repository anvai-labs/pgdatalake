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

# Format-generic lakehouse storage engine (specs/lakehouse): optional
# feature; 'auto' disables itself when the Rust toolchain (cargo, cbindgen)
# is unavailable.
datalake_opt = get_option('datalake')
datalake_build = datalake_opt != 'disabled'
if datalake_build
  datalake_cargo = find_program('cargo', required: datalake_opt == 'enabled')
  datalake_cbindgen = find_program('cbindgen', required: datalake_opt == 'enabled')
  datalake_build = datalake_cargo.found() and datalake_cbindgen.found()
endif
if datalake_build
  subdir('lakehouse')
endif
