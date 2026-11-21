#!/usr/bin/env python3
# Repair pg_bsd_indent's UTF-8 string-literal mangling in fork-authored
# lakehouse C files. indent 2.1.3 preserves UTF-8 in COMMENTS but rewrites
# multi-byte characters inside STRING LITERALS as literal octal-ish digits
# (em-dash e2 80 94 -> e2 <digits> 94), regardless of locale. Run this AFTER
# every pgindent pass over src/backend/access/lakehouse/ (the two errmsg
# em-dashes are user-visible text and regress-expected exact).
import re
import sys

FILES = [
    "src/backend/access/lakehouse/tableam.c",
    "src/backend/access/lakehouse/fdw.c",
    "src/backend/access/lakehouse/lakehouse_core.c",
    "src/backend/access/lakehouse/batch_fill.c",
    "src/backend/access/lakehouse/batch_fill.h",
    "src/backend/access/lakehouse/lakehouse.h",
]
EM = "—".encode("utf-8")

# the two known pgindent-mangled string sites and their canonical bytes;
# extend when a new non-ASCII string literal lands
EXPECTED = {
    "src/backend/access/lakehouse/tableam.c": [
        b'lakehouse.managed_tables is missing \xe2\x80\x94 "',
        b' error \xe2\x80\x94 the rows would be silently',
    ],
}

for path in [p for p in (sys.argv[1:] or FILES)]:
    data = open(path, "rb").read()
    # invalid utf-8 sequences (lead e2 eaten, digits + trailing 94 remain)
    n = 0
    while True:
        try:
            data.decode("utf-8")
            break
        except UnicodeDecodeError as e:
            data = data[:e.start] + EM + data[e.end:]
            n += 1
    # collapsed forms left by earlier partial repairs: —<digits>—
    data, m = re.subn(EM + rb"[0-9]+" + EM, EM, data)
    open(path, "wb").write(data)
    print(f"{path}: repaired {n} sequences, collapsed {m}")
    expected = EXPECTED.get(path, [])
    text = data.decode("utf-8")
    for want in expected:
        if want not in data:
            print(f"WARNING: {path}: expected site {want!r} not intact — "
                  "inspect manually; a blind em-dash substitution may have "
                  "corrupted different bytes", file=sys.stderr)
    if n > len(expected):
        print(f"WARNING: {path}: {n} em-dash insertions but only "
              f"{len(expected)} known sites — verify each", file=sys.stderr)
