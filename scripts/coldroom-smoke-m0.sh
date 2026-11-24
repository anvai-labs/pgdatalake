#!/bin/bash
set -x
COLDROOM="${COLDROOM:-/tmp/coldroom-pg}"
cd "$COLDROOM"
# 1. fixtures from the fresh clone (tear down the dev stack first)
docker compose -p pgdatalake down --volumes --remove-orphans >/dev/null 2>&1
make lakehouse-up || { echo "FAIL: lakehouse-up"; exit 1; }
# 2. build
CC=/usr/bin/gcc ~/.local/bin/meson setup build -Ddatalake=enabled -Dcassert=true > /tmp/cr-setup.log 2>&1 || { echo "FAIL: meson setup"; tail -5 /tmp/cr-setup.log; exit 1; }
~/.local/bin/ninja -C build > /tmp/cr-ninja.log 2>&1 || { echo "FAIL: ninja"; tail -5 /tmp/cr-ninja.log; exit 1; }
echo "PASS: meson setup + ninja"
# 3. upstream suite
~/.local/bin/meson test -C build --suite setup > /tmp/cr-setup-suite.log 2>&1 || { echo "FAIL: setup suite"; exit 1; }
~/.local/bin/meson test -C build --suite regress > /tmp/cr-regress.log 2>&1 || { echo "FAIL: upstream regress"; exit 1; }
grep -E "subtests passed" /tmp/cr-regress.log | tail -1
# 4. lakehouse suites + cargo
make lakehouse-check > /tmp/cr-check.log 2>&1 || { echo "FAIL: lakehouse-check"; tail -20 /tmp/cr-check.log; exit 1; }
echo "PASS: make lakehouse-check"
echo "COLDROOM ALL GREEN"
