import hashlib
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from pinned_receipt import (
    TPCDS_TABLES,
    compare_result,
    load_oracle,
    result_digest,
    summarize_samples,
    verify_attachments,
)


class PinnedReceiptTest(unittest.TestCase):
    def test_digest_keeps_duplicates_nulls_and_types(self):
        rows = [(20, Decimal("3.00")), (10, None), (20, Decimal("3.0"))]
        expected = hashlib.sha256(b'[10,null]\n[20,{"numeric":"3"}]\n[20,{"numeric":"3"}]\n')
        self.assertEqual(result_digest(rows), (3, expected.hexdigest()))
        self.assertEqual(result_digest(reversed(rows)), result_digest(rows))
        self.assertNotEqual(result_digest(rows[:-1]), result_digest(rows))
        self.assertNotEqual(result_digest([("20",)]), result_digest([(20,)]))

    def test_equal_cardinality_does_not_hide_wrong_result(self):
        count, digest = result_digest([(10,), (20,), (20,)])
        expected = {"row_count": count, "sha256": digest}
        self.assertEqual(compare_result(expected, [(20,), (10,), (20,)]),
                         {"status": "PASS", "row_count": count, "sha256": digest})
        self.assertEqual(compare_result(expected, [(20,), (10,), (30,)])["status"],
                         "RESULT_MISMATCH")

    def test_pin_and_locator_must_both_match_before_timing(self):
        expected = {"orders": {"table": "fixture.orders", "snapshot_id": "9223372036854775807"}}
        verify_attachments(expected, {"orders": dict(expected["orders"])})
        for changed in ({}, {"table": "fixture.orders"},
                        {"table": "fixture.old_orders", "snapshot_id": "9223372036854775807"},
                        {"table": "fixture.orders", "snapshot_id": "1"}):
            with self.subTest(changed=changed), self.assertRaisesRegex(ValueError, "orders"):
                verify_attachments(expected, {"orders": changed})

    def test_oracle_locks_query_bytes_and_integer_pins_as_strings(self):
        raw = b"SELECT 1;\n"
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            query_file = directory / "query1.sql"
            query_file.write_bytes(raw)
            oracle_file = directory / "oracle.json"
            row_count, digest = result_digest([(1,)])
            oracle = {"version": 1, "dataset": "fixture", "reference_engine": "oracle",
                      "tables": {name: {"table": "lake." + name, "snapshot_id": "9223372036854775807"}
                                 for name in TPCDS_TABLES},
                      "queries": {"query1": {"row_count": row_count, "sha256": digest,
                                             "sql_sha256": hashlib.sha256(raw).hexdigest()}}}
            oracle_file.write_text(json.dumps(oracle))
            self.assertEqual(load_oracle(oracle_file, ["query1"], directory)[1],
                             {"query1": raw.decode()})
            query_file.write_text("SELECT 2;\n")
            with self.assertRaisesRegex(ValueError, "reference SQL"):
                load_oracle(oracle_file, ["query1"], directory)
            oracle["tables"]["store"]["snapshot_id"] = 9223372036854775807
            oracle_file.write_text(json.dumps(oracle))
            with self.assertRaisesRegex(ValueError, "store: .*snapshot ID"):
                load_oracle(oracle_file, ["query1"], directory)

    def test_percentiles_need_samples_and_no_failed_attempts(self):
        def samples(n):
            return [{"status": "PASS", "elapsed_seconds": i} for i in range(1, n + 1)]

        self.assertIsNone(summarize_samples(samples(4))["median_seconds"])
        self.assertEqual(summarize_samples(samples(5))["median_seconds"], 3)
        self.assertIsNone(summarize_samples(samples(99))["p99_seconds"])
        self.assertEqual(summarize_samples(samples(100))["p95_seconds"], 95)
        self.assertEqual(summarize_samples(samples(100))["p99_seconds"], 99)
        failed = samples(100) + [{"status": "TIMEOUT", "elapsed_seconds": 101}]
        self.assertEqual(summarize_samples(failed)["attempts"], 101)
        self.assertIsNone(summarize_samples(failed)["median_seconds"])


if __name__ == "__main__":
    unittest.main()
