"""Check the synthetic governed-read SQL oracle, independently of a provider."""

import json
from pathlib import Path
import sqlite3
import unittest


class GovernedReadOracle(unittest.TestCase):
    def test_pinned_rows_and_aggregates(self):
        fixture = json.loads(Path(__file__).with_name("governed-read-v1.json").read_text())
        self.assertEqual(fixture["version"], 1)
        expected = fixture["expected"]
        pairs = [(case["pin"], case["viewer"]) for case in expected]
        self.assertCountEqual(pairs, [(p, v) for p in fixture["pins"] for v in fixture["viewers"]])
        db = sqlite3.connect(":memory:")
        self.addCleanup(db.close)
        db.execute("CREATE TABLE source (region text, item integer, amount integer)")
        db.execute("CREATE TABLE viewer (region text, mask_amount integer)")
        db.execute("""
            CREATE VIEW governed AS
            SELECT s.region, s.item, CASE WHEN v.mask_amount THEN NULL ELSE s.amount END AS amount
            FROM source s JOIN viewer v ON s.region = v.region
        """)
        for case in expected:
            with self.subTest(pin=case["pin"], viewer=case["viewer"]):
                viewer = fixture["viewers"][case["viewer"]]
                db.execute("DELETE FROM source")
                db.execute("DELETE FROM viewer")
                db.executemany("INSERT INTO source VALUES (?, ?, ?)", fixture["pins"][case["pin"]])
                db.execute("INSERT INTO viewer VALUES (?, ?)", (viewer["region"], viewer["mask_amount"]))
                self.assertCountEqual(db.execute("SELECT * FROM governed"), map(tuple, case["rows"]))
                self.assertEqual(
                    db.execute("SELECT count(*), count(amount), sum(amount) FROM governed").fetchone(),
                    (case["count_star"], case["count_amount"], case["sum_amount"]),
                )


if __name__ == "__main__":
    unittest.main()
