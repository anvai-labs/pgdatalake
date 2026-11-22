#!/bin/sh
# clang-tidy for fork-authored lakehouse C code (specs/lakehouse RULES.md:
# upstream and core-allowlist files are never linted/reformatted).
#
# Requires a configured meson build dir (default: ../build relative to repo
# root) for generated headers (pg_config.h, cbindgen's pg_datalake_core.h).
#
# It does NOT read meson's compile_commands.json (gcc-flavored: -fpch-deps
# and friends are clang-incompatible); instead it hardcodes the same
# -D/-I flag set meson uses for lakehouse, plus the two build-dir include
# paths meson adds for generated headers (pg_config.h, cbindgen's
# pg_datalake_core.h). Headers are linted via their .c includes — add
# files to the list below when a new lakehouse source lands.
set -eu

repo=$(cd "$(dirname "$0")/.." && pwd)
build=${LAKEHOUSE_BUILD_DIR:-"$repo/build"}
dir=src/backend/access/lakehouse

files="
$dir/batch_fill.c
$dir/fdw.c
$dir/lakehouse_sql.c
$dir/tableam.c
"

cd "$build"
status=0
for f in $files; do
	echo "== clang-tidy $f"
	clang-tidy "$repo/$f" -- \
		-D_GNU_SOURCE -D_FILE_OFFSET_BITS=64 \
		-I"$build/src/include" \
		-I"$repo/src/include" \
		-I"$repo/$dir" \
		-I"$build/$dir" \
		-I/usr/include/libxml2 || status=1
done
exit $status
