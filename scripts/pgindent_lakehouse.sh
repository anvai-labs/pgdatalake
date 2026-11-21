#!/bin/sh
# pgindent wrapper for the lakehouse fork (RULES §4: pgindent before every
# commit touching C).
#
# Why a wrapper: upstream pgindent needs two fork-specific fixes —
#   1. fork typedefs are not in src/tools/pgindent/typedefs.list, and an
#      unknown type makes pg_bsd_indent split `Type *ptr` into
#      `Type * ptr` (it reads the `*` as multiplication). We extract the
#      fork typedefs mechanically from the lakehouse sources and pass
#      them via --list-of-typedefs.
#   2. pg_bsd_indent 2.1.3 mangles multi-byte UTF-8 inside STRING
#      LITERALS (em-dash -> e2<digits>94) — scripts/repair_pgindent_utf8.py
#      repairs the known sites after every pass.
#
# Usage:
#   scripts/pgindent_lakehouse.sh file.c [more.c/.h]   format in place
#   scripts/pgindent_lakehouse.sh --check file.c ...   exit 1 if any file
#                                                      would change (hook mode)
# With no files: formats/checks the fork-authored lakehouse set.
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
CHECK=0
if [ "${1:-}" = "--check" ]; then
	CHECK=1
	shift
fi

DEFAULT_FILES="
src/backend/access/lakehouse/tableam.c
src/backend/access/lakehouse/fdw.c
src/backend/access/lakehouse/lakehouse_core.c
src/backend/access/lakehouse/batch_fill.c
src/backend/access/lakehouse/batch_fill.h
src/backend/access/lakehouse/lakehouse.h
"
if [ $# -gt 0 ]; then
	FILES="$*"
else
	FILES=$DEFAULT_FILES
fi

PGINDENT_SCRIPT="$ROOT/src/tools/pgindent/pgindent"
# the perl wrapper reads the BINARY from $PGINDENT/$INDENT (default PATH)
INDENT_BIN=${PG_BSD_INDENT:-pg_bsd_indent}
export PGINDENT="$INDENT_BIN"

# --- fork typedef extraction ------------------------------------------------
# Three sources, two shapes each (`} TypeName;` closer, `typedef ... TypeName;`):
#   1. lakehouse *.c/*.h        the fork's own C types
#   2. the cbindgen-generated FFI header (build tree) — PgDatalakeRowBatch,
#      WriteSession etc.; without these pg_bsd_indent mangles `Type *ptr`
#      (it reads the `*` as multiplication)
#   3. ffi.rs `pub struct` names as a fallback when the build header is absent
TMPD=$(mktemp -d)
trap 'rm -rf "$TMPD"' EXIT
FFI_HDR=build/src/backend/access/lakehouse/pg_datalake_core.h
{
	cat src/backend/access/lakehouse/*.c src/backend/access/lakehouse/*.h
	[ -f "$FFI_HDR" ] && cat "$FFI_HDR"
	[ -f "$FFI_HDR" ] || cat src/backend/access/lakehouse/core/pgdatalake-core/src/ffi.rs
} | grep -hE '(^\}[[:space:]]*[A-Za-z_][A-Za-z0-9_]*[[:space:]]*;)|(^[[:space:]]*(typedef[[:space:]].*[[:space:]])?pub struct [A-Za-z_][A-Za-z0-9_]*)|(^[[:space:]]*typedef[[:space:]]+struct[[:space:]]+[A-Za-z_][A-Za-z0-9_]*[[:space:]]+[A-Za-z_][A-Za-z0-9_]*[[:space:]]*;)' \
	| sed -E 's/^\}[[:space:]]*([A-Za-z_][A-Za-z0-9_]*)[[:space:]]*;/\1/;
		s/^[[:space:]]*(typedef[[:space:]].*[[:space:]])?pub struct ([A-Za-z_][A-Za-z0-9_]*)/\2/;
		s/^[[:space:]]*typedef[[:space:]]+struct[[:space:]]+([A-Za-z_][A-Za-z0-9_]*)[[:space:]]+[A-Za-z_][A-Za-z0-9_]*[[:space:]]*;/\1/' \
	| grep -E '^[A-Za-z_][A-Za-z0-9_]*$' | sort -u > "$TMPD/fork_typedefs"

STATUS=0
for f in $FILES; do
	[ -f "$ROOT/$f" ] || continue
	# pgindent filters by extension — keep the basename in the tmp copy
	WORK="$TMPD/$(basename "$f")"
	cp "$ROOT/$f" "$TMPD/original"
	cp "$ROOT/$f" "$WORK"
	(cd "$ROOT" && perl "$PGINDENT_SCRIPT" \
		--list-of-typedefs="$(cat "$TMPD/fork_typedefs")" \
		"$WORK" > /dev/null)
	# UTF-8 string-literal repair (pg_bsd_indent mangling, see helper)
	python3 "$ROOT/scripts/repair_pgindent_utf8.py" "$WORK" > /dev/null
	if ! cmp -s "$TMPD/original" "$WORK"; then
		if [ "$CHECK" = 1 ]; then
			echo "pgindent: $f is NOT pgindent-clean" >&2
			STATUS=1
		else
			cp "$WORK" "$ROOT/$f"
			echo "pgindent: reformatted $f"
		fi
	fi
done

if [ "$CHECK" = 1 ] && [ "$STATUS" = 0 ]; then
	echo "pgindent: all files clean"
fi
exit $STATUS
