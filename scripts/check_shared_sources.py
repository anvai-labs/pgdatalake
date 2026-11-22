"""Fail CI when a shared (stock-safe) source grows a fork-only dependency.

The stock PGXS package and the fork compile the same C modules; any
drift silently changes one product's behavior. The shared set is read
from stock/SHARED_SOURCES and cross-checked against the stock
Makefile's OBJS, so the two lists cannot silently diverge. Comments are
stripped before scanning; fork-only mechanisms (planner hook state, the
managed ledger, tableam internals) must never appear in stock-active
code. The ACTUAL authority is the stock PG19 PGXS build itself; this
guard is the cheap tripwire that runs on every push before that build.

Run `--self-test` (wired into CI) to prove the guard still fails on a
deliberate build-list mismatch, an empty manifest, a fork-only symbol
in stock-active code, and a missing manifest entry.
"""
import re
import sys
from pathlib import Path

def lake_dir(root=None):
    base = (Path(root).resolve() if root
            else Path(__file__).resolve().parents[1])
    return base / "src/backend/access/lakehouse"

# token-level patterns, applied to comment-stripped source
FORBIDDEN = [
    (r"\bwhatever_placeholder\b", ""),
]


def strip_comments(text):
    text = re.sub(r"/\*.*?\*/", " ", text, flags=re.S)
    text = re.sub(r"//.*", " ", text)
    return text


def strip_fork_branches(text):
    """Drop the fork side of every '#ifdef LAKEHOUSE_STOCK_FDW' block:
    from the '#else' to the matching '#endif' the fork-only code is
    compiled out of the stock build, so the guard must not see it."""
    out = []
    depth = 0          # nesting depth inside a stock-ifdef block
    dropping = False
    for line in text.splitlines():
        stripped = line.strip()
        if re.match(r"#\s*ifdef\s+LAKEHOUSE_STOCK_FDW\b", stripped):
            depth += 1
            dropping = False
            out.append("")
            continue
        if depth > 0:
            if re.match(r"#\s*if\b", stripped) and not re.match(r"#\s*ifdef\s+LAKEHOUSE_STOCK_FDW", stripped):
                depth += 1
            elif re.match(r"#\s*endif", stripped):
                depth -= 1
                dropping = False
                out.append("")
                continue
            elif re.match(r"#\s*else", stripped) and depth == 1:
                dropping = True
            if dropping:
                out.append("")
                continue
        out.append(line)
    return "\n".join(out)


def shared_modules(lake):
    manifest = lake / "stock/SHARED_SOURCES"
    shared = [l.strip() for l in manifest.read_text().splitlines()
              if l.strip() and not l.strip().startswith("#")]
    if not shared:
        raise SystemExit("shared-source contract: manifest is empty")
    mk = (lake / "stock/Makefile").read_text()
    objs = re.search(r"^OBJS\s*=\s*(.+)$", mk, re.M)
    if not objs:
        raise SystemExit("shared-source contract: no OBJS in stock Makefile")
    stock_srcs = sorted(o.removesuffix(".o") + ".c"
                        for o in objs.group(1).split())
    listed = sorted(shared)
    only_stock = [f for f in stock_srcs if f not in listed]
    only_manifest = [f for f in listed if f not in stock_srcs]
    if only_stock or only_manifest:
        raise SystemExit(
            "shared-source contract: manifest/Makefile OBJS diverge; "
            f"only in OBJS: {only_stock}; only in manifest: {only_manifest}")
    return shared


def main(root=None):
    lake = lake_dir(root)
    shared = shared_modules(lake)
    patterns = [
        (r"\blakehouse_snapshot\b", "fork-only RTE field"),
        (r"\blakehouse_tableam\.h\b|\blakehouse_tableam\b", "managed TableAM internals"),
        (r"\blakehouse_delta_segments\b", "managed delta ledger"),
        (r"\binstall_lakehouse_count_exec\b", "managed planner hook"),
        (r"\bpublish_staged\b", "publisher entry point"),
    ]
    failures = []
    for name in shared:
        path = lake / name
        if not path.exists():
            raise SystemExit(f"shared-source contract: {name} not found")
        text = strip_comments(path.read_text(encoding="utf-8", errors="replace"))
        text = strip_fork_branches(text)
        for pattern, why in patterns:
            for m in re.finditer(pattern, text):
                line = text.count("\n", 0, m.start()) + 1
                failures.append(f"{name}:{line}: {why} ({m.group(0)})")
    if failures:
        print("shared-source contract violations:", file=sys.stderr)
        print("\n".join(failures), file=sys.stderr)
        raise SystemExit(1)
    print(f"shared-source contract clean: {len(shared)} modules")


def self_test():
    """Prove each contract failure mode actually fires on a temp tree."""
    import tempfile

    def run_in(tmp, manifest_lines, objs, body):
        base = tmp / "src/backend/access/lakehouse"
        (base / "stock").mkdir(parents=True)
        (base / "stock/SHARED_SOURCES").write_text(
            "\n".join(manifest_lines) + "\n")
        (base / "stock/Makefile").write_text("OBJS = %s\n" % objs)
        for name in manifest_lines:
            if name.startswith("#") or not name.strip():
                continue
            if body is not None:
                (base / name).write_text(body)
        import subprocess
        r = subprocess.run(
            [sys.executable, str(Path(__file__).resolve()),
             "--root", str(tmp)],
            capture_output=True, text=True)
        return r.returncode

    cases = [
        ("obbs/manifest mismatch fails", 1,
         ["fdw.c"], "fdw.o fdw_plan.o", "int main(void){return 0;}"),
        ("empty manifest fails", 1,
         ["# nothing"], "fdw.o", "int main(void){return 0;}"),
        ("fork symbol in stock-active code fails", 1,
         ["fdw.c"], "fdw.o",
         "void f(void *r) { (void) ((RangeTblEntry *) r)->lakehouse_snapshot; }"),
        ("fork symbol inside guarded #else branch passes", 0,
         ["fdw.c"], "fdw.o",
         "#ifdef LAKEHOUSE_STOCK_FDW\nreturn NULL;\n#else\n"
         "return rte->lakehouse_snapshot;\n#endif\n"),
        ("missing manifest entry fails loudly", 1,
         ["gone.c"], "gone.o", None),
    ]
    for name, want, manifest, objs, body in cases:
        with tempfile.TemporaryDirectory() as td:
            got = run_in(Path(td), manifest, objs, body)
        if got != want:
            raise SystemExit(f"self-test FAILED: {name} "
                             f"(want exit {want}, got {got})")
    print(f"self-test clean: {len(cases)} failure modes fire correctly")


if __name__ == "__main__":
    args = sys.argv[1:]
    if "--self-test" in args:
        self_test()
    else:
        root = args[args.index("--root") + 1] if "--root" in args else None
        main(root)
