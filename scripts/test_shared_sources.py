"""Exercise the shared-source guard with the C preprocessor on tiny fixtures."""

import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import unittest


GUARD = Path(__file__).with_name("check_shared_sources.py")


class SharedSourceTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.root = Path(self.tempdir.name)
        self.lake = self.root / "src/backend/access/lakehouse"
        stock = self.lake / "stock"
        stock.mkdir(parents=True)
        self.manifest = stock / "SHARED_SOURCES"
        self.manifest.write_text("fdw.c\n")
        self.source = self.lake / "fdw.c"

    def check(self, expected, diagnostic):
        compiler = shlex.split(os.environ.get("CC", "cc"))
        result = subprocess.run(
            [sys.executable, GUARD, "--root", self.root, "--cc", *compiler,
             "-DLAKEHOUSE_STOCK_FDW", "-I", self.lake],
            capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
        self.assertIn(diagnostic, result.stderr + result.stdout)

    def test_nested_stock_branch(self):
        self.source.write_text(
            "#ifdef LAKEHOUSE_STOCK_FDW\n#ifdef OPTIONAL\nint optional;\n"
            '#else\nconst char *ledger = "lakehouse_delta_segments";\n#endif\n'
            "#else\nint fork_only;\n#endif\n"
        )
        self.check(1, "lakehouse_delta_segments")

    def test_url_before_sql(self):
        self.source.write_text('read("https://catalog", "lakehouse_delta_segments");\n')
        self.check(1, "lakehouse_delta_segments")

    def test_header_and_macro(self):
        (self.lake / "helper.h").write_text(
            "#define LEDGER(rte) (rte)->lakehouse_snapshot\n"
        )
        self.source.write_text('#include "helper.h"\nint v = LEDGER(rte);\n')
        self.check(1, "lakehouse_snapshot")

    def test_managed_header(self):
        (self.lake / "lakehouse_tableam.h").write_text("/* empty */\n")
        self.source.write_text('#include "lakehouse_tableam.h"\n')
        self.check(1, "lakehouse_tableam.h")

    def test_fork_branch_and_comments(self):
        self.source.write_text(
            "#if !defined(LAKEHOUSE_STOCK_FDW)\n#include \"lakehouse_tableam.h\"\n"
            "#elif defined(LAKEHOUSE_STOCK_FDW)\n"
            "/* lakehouse_snapshot */ int ok; // publish_staged()\n"
            "#else\nreturn rte->lakehouse_snapshot;\n#endif\n"
        )
        self.check(0, "1 shared")

    def test_missing_file(self):
        self.check(1, "fdw.c")

    def test_empty_manifest(self):
        self.manifest.write_text("")
        self.check(1, "empty")

    def test_duplicate_source(self):
        self.source.touch()
        self.manifest.write_text("fdw.c\nfdw.c\n")
        self.check(1, "duplicate")

    def test_preprocessor_failure(self):
        self.source.write_text('#include "missing_header.h"\n')
        self.check(1, "missing_header.h")


if __name__ == "__main__":
    unittest.main()
