"""Check the shared C source list and named fork dependencies in a stock PGXS build.

Run with PG_CONFIG pointing to the installed PostgreSQL 19 pg_config. PGXS
supplies the compiler, flags and generated ABI header for the build being
checked. --check-list and --self-test also work before PostgreSQL is built.
The stock compile, link, SQL install and live smoke qualify the resulting module.
"""

import argparse
from pathlib import Path
import re
import subprocess
import sys


FORK_NAMES = re.compile(
    r"\b(lakehouse_snapshot|lakehouse_tableam|lakehouse_delta_segments|"
    r"install_lakehouse_count_exec|(?:pg_lakehouse_)?publish_staged)\b"
)
LINE_MARKER = re.compile(r'^# (\d+) "([^"]+)"')


def shared_sources(lake):
    names = (lake / "stock/SHARED_SOURCES").read_text().split()
    if not names:
        raise SystemExit("shared source manifest is empty")
    if len(names) != len(set(names)):
        raise SystemExit("duplicate source in shared source manifest")
    for name in names:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*\.c", name):
            raise SystemExit(f"invalid shared C filename: {name}")
        if not (lake / name).is_file():
            raise SystemExit(f"shared source missing: {name}")
    return names


def check_compiled_source(source, compiler):
    # The same stock flags apply to nested conditionals and included headers.
    # Leave strings intact because they can contain SQL references to the ledger.
    result = subprocess.run([*compiler, "-E", source], stdout=subprocess.PIPE)
    if result.returncode:
        raise SystemExit(f"could not preprocess {source.name}")
    filename = str(source)
    number = 1
    for line in result.stdout.decode("utf-8", errors="replace").splitlines():
        marker = LINE_MARKER.match(line)
        if marker:
            number = int(marker[1])
            filename = marker[2]
            if Path(filename).name == "lakehouse_tableam.h":
                raise SystemExit(f"{source.name} includes managed header {filename}")
            continue
        match = FORK_NAMES.search(line)
        if match:
            raise SystemExit(f"{filename}:{number}: stock code references {match[0]}")
        number += 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--self-test", action="store_true")
    mode.add_argument("--check-list", action="store_true")
    mode.add_argument("--cc", nargs=argparse.REMAINDER, help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.self_test:
        test = Path(__file__).with_name("test_shared_sources.py")
        raise SystemExit(subprocess.call([sys.executable, test]))

    lake = args.root.resolve() / "src/backend/access/lakehouse"
    names = shared_sources(lake)
    if args.check_list:
        print(f"{len(names)} shared C source files listed")
    elif args.cc is not None:
        if not args.cc:
            parser.error("--cc requires the stock compiler")
        for name in names:
            check_compiled_source(lake / name, args.cc)
        print(f"{len(names)} shared C sources checked with the stock preprocessor")
    else:
        raise SystemExit(subprocess.call(["make", "-C", lake / "stock", "check-shared-sources"]))


if __name__ == "__main__":
    main()
