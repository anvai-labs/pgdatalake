#!/usr/bin/env python3
"""Build pgiceberg-core for the meson build (specs/iceberg/01-architecture.md §5).

Invoked by a meson custom_target in src/backend/access/iceberg/meson.build.
Runs `cargo build --release` for the workspace and `cbindgen` for the C header,
then copies both artifacts into the meson build directory. Rebuild scheduling
is coarse (build_always_stale on the meson side); cargo's own caching makes the
no-op case cheap. Fine-grained dependency tracking (depfile) is planned for M1.
"""

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--manifest-dir', required=True,
                    help='directory of the cargo workspace (core/)')
parser.add_argument('--target-dir', required=True,
                    help='cargo target directory inside the meson build tree')
parser.add_argument('--out-dir', required=True,
                    help='meson build dir receiving lib + header outputs')
args = parser.parse_args()

manifest_dir = Path(args.manifest_dir)
target_dir = Path(args.target_dir)
out_dir = Path(args.out_dir)


def run(cmd):
    print(' '.join(str(c) for c in cmd), flush=True)
    subprocess.run([str(c) for c in cmd], check=True, cwd=manifest_dir)


run(['cargo', 'build', '--release',
     '--manifest-path', manifest_dir / 'Cargo.toml',
     '--target-dir', target_dir])

run(['cbindgen',
     '--crate', 'pgiceberg-core',
     '--config', manifest_dir / 'cbindgen.toml',
     '--output', out_dir / 'pgiceberg_core.h'])

shutil.copy2(target_dir / 'release' / 'libpgiceberg_core.a',
             out_dir / 'libpgiceberg_core.a')
