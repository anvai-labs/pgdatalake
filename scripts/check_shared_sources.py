"""Fail CI when a shared (stock-safe) source grows a fork-only dependency.

The stock PGXS package compiles the shared C modules with
-DLAKEHOUSE_STOCK_FDW; fork-only mechanisms (planner hook state, the
managed ledger, tableam internals) must never be referenced there. The
fork build compiles the same files, so any drift silently changes one
product's behavior. This guard runs in CI on both build paths.
"""
import re
import sys
from pathlib import Path

LAKE = Path(__file__).resolve().parents[1] / "src/backend/access/lakehouse"
FORK_ONLY = [
    r"lakehouse_tableam\.h",
    r"lakehouse_snapshot",
    r"lakehouse_delta_segments",
    r"install_lakehouse_count_exec",
    r"publish_staged",
    r"lakehouse_fdw_handler",  # declared in lakehouse_fdw.h? keep: shared
]
FORK_ONLY = FORK_ONLY[:-1]  # handler IS shared; drop the over-broad pattern
FORBIDDEN = [
    (r"lakehouse_tableam\.h", "managed TableAM internals"),
    (r"rte->lakehouse_snapshot", "fork-only RTE field"),
    (r"lakehouse_delta_segments", "managed delta ledger"),
    (r"install_lakehouse_count_exec", "managed planner hook"),
    (r"pg_lakehouse_publish_staged|publish_staged\(", "publisher entry point"),
]


def main():
    manifest = LAKE / "stock/SHARED_SOURCES"
    shared = [l.strip() for l in manifest.read_text().splitlines()
              if l.strip() and not l.strip().startswith("#")]
    failures = []
    for name in shared:
        text = (LAKE / name).read_text(encoding="utf-8", errors="replace")
        for pattern, why in FORBIDDEN:
            for m in re.finditer(pattern, text):
                line = text.count("\n", 0, m.start()) + 1
                failures.append(f"{name}:{line}: {why} ({m.group(0)})")
    if failures:
        print("shared-source contract violations:", file=sys.stderr)
        print("\n".join(failures), file=sys.stderr)
        raise SystemExit(1)
    print(f"shared-source contract clean: {len(shared)} modules")


if __name__ == "__main__":
    main()
