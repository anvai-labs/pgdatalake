#!/bin/sh
# clang-tidy for fork-authored lakehouse C code (specs/lakehouse RULES.md:
# upstream and core-allowlist files are never linted/reformatted).
#
# Requires a configured meson build dir (default: ../build relative to repo
# root) for generated headers (pg_config.h, cbindgen's pg_datalake_core.h).
#
# The meson compile_commands.json is gcc-flavored: gcc-only flags (-fpch-deps,
# -MD/-MF/-MQ/-o) and gcc-specific include paths are stripped and the missing
# build-dir includes are prepended, then clang-tidy runs with the repo's
# curated profile in src/backend/access/lakehouse/.clang-tidy.
set -eu

repo=$(cd "$(dirname "$0")/.." && pwd)
build=${LAKEHOUSE_BUILD_DIR:-"$repo/build"}
dir=src/backend/access/lakehouse

files="
$dir/batch_fill.c
$dir/batch_fill.h
$dir/fdw.c
$dir/lakehouse.h
$dir/lakehouse_core.c
$dir/tableam.c
"

cd "$build"
status=0
for f in $files; do
	case $f in
		*.h) continue ;;	# linted via their .c includes
	esac
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
