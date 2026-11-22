"""Check the published spec examples. Runtime conformance uses the same inputs."""

import json
from pathlib import Path
import unittest


FIXTURE = (Path(__file__).resolve().parents[3]
           / "specs/lakehouse/modules/fixtures/contract-v1.json")


class ContractVectors(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.vectors = json.loads(FIXTURE.read_text(encoding="utf-8"))
        if cls.vectors["version"] != 1:
            raise ValueError("expected contract version 1")

    def test_managed_overlay(self):
        vector = self.vectors["managed_overlay"]
        for case in vector["cases"]:
            with self.subTest(case=case["id"]):
                visible = case["operation_pg_visible"]
                membership = case["membership"]
                self.assertIn(membership, ("unknown", "included", "absent"))
                if membership == "unknown":
                    self.assertEqual(case["expected"], {"status": "unproven_membership"})
                elif membership == "included" and not visible:
                    self.assertEqual(case["expected"], {"status": "incompatible_pin"})
                else:
                    rows = list(case["base_row_ids"])
                    if visible and membership == "absent":
                        rows.extend(vector["operation"]["row_ids"])
                    self.assertEqual(case["expected"]["status"], "rows")
                    self.assertCountEqual(case["expected"]["row_ids"], rows)

    def test_mixed_visibility_claim(self):
        vector = self.vectors["mixed_visibility_claim"]
        segments = {s["id"]: s["row_ids"] for s in vector["segments"]}
        pins = {p["id"]: p for p in vector["pins"]}
        for case in vector["cases"]:
            with self.subTest(case=case):
                visible = set(case["visible_segments"])
                for candidate in case["candidate_pins"]:
                    pin = pins[candidate]
                    included = set(pin["included_segments"])
                    if included <= visible:
                        rows = list(pin["base_row_ids"])
                        for segment in visible - included:
                            rows.extend(segments[segment])
                        self.assertEqual(case["expected"]["pin"], candidate)
                        self.assertCountEqual(case["expected"]["row_ids"], rows)
                        break
                else:
                    self.assertEqual(case["expected"], {"status": "incompatible_pin"})

    def test_reply_lost(self):
        vector = self.vectors["publish_reply_lost"]
        steps = vector["steps"]
        sends = [s for s in steps if s["action"] == "submit"]
        lookups = [s for s in steps if s["action"] == "lookup"]
        added = [s["segment_id"] for s in steps if s["action"] == "add_local_segment"]
        expected = vector["expected"]
        self.assertEqual(len(sends), expected["submit_count"])
        self.assertEqual(lookups[-1]["result"], "committed")
        self.assertEqual(lookups[-1]["snapshot_id"], sends[0]["committed_snapshot_id"])
        self.assertEqual(expected["published_snapshot_id"], lookups[-1]["snapshot_id"])
        self.assertCountEqual(expected["completed_segment_ids"], vector["claimed_segment_ids"])
        self.assertCountEqual(expected["pending_segment_ids"], added)

    def test_recovery_checkpoints(self):
        vector = self.vectors["publish_recovery_cases"]
        members = self.vectors[vector["setup_from"]]["claimed_segment_ids"]
        for case in vector["cases"]:
            sent, accepted = [], []
            receipt = None
            decision = "prepare_same_claim"
            for step in case["steps"]:
                with self.subTest(case=case["id"], step=step):
                    action = step["action"]
                    if action == "submit":
                        self.assertEqual(decision, "prepare_same_claim")
                        sent.append(step["expected_parent"])
                        if step["remote_effect"] == "commit":
                            accepted.append(step["committed_snapshot_id"])
                        if step["result"] == "committed":
                            receipt = step["committed_snapshot_id"]
                            decision = "complete"
                        elif step["result"] == "unknown":
                            decision = "lookup_only"
                        else:
                            self.assertEqual(step["result"], "rejected")
                    elif action == "crash_after_durable_send_intent":
                        decision = "lookup_only"
                    elif action == "deliver_deferred":
                        accepted.append(step["committed_snapshot_id"])
                    elif action == "lookup":
                        self.assertEqual(decision, "lookup_only")
                        if step["result"] == "committed":
                            receipt = step["snapshot_id"]
                            self.assertIn(receipt, accepted)
                            decision = "complete"
                        else:
                            self.assertEqual(step["result"], "no_evidence")
                    elif action == "checkpoint":
                        self.assertEqual(step["expected"], {
                            "sent_parents": sent,
                            "accepted_for_operation": len(accepted),
                            "completed_segment_ids": members if receipt is not None else [],
                            "receipt_snapshot": receipt,
                            "next": decision,
                        })
                    else:
                        self.assertIn(action, ("external_compatible_append", "takeover"))

    def test_cow_delete_safe(self):
        vector = self.vectors["cow_delete_safe"]
        live = [i for i in range(len(vector["physical_row_ids"]))
                if i not in vector["already_deleted_ordinals"]]
        self.assertEqual(live, vector["expected_reader_ordinals"])
        rows = [vector["physical_row_ids"][i] for i in live
                if i not in vector["mutation_deleted_ordinals"]]
        self.assertCountEqual(rows, vector["expected_rewrite_row_ids"])


class ServiceContractVectors(unittest.TestCase):
    """Example oracles for the proposed service interfaces, independent of any cloud."""

    @classmethod
    def setUpClass(cls):
        path = FIXTURE.with_name("service-v1.json")
        cls.vectors = json.loads(path.read_text(encoding="utf-8"))
        if cls.vectors["version"] != 1:
            raise ValueError("expected service contract version 1")

    def test_receipt_at_selected_pin(self):
        case = self.vectors["receipt_at_pin"]
        receipt = case["receipt"]
        for check in case["checks"]:
            with self.subTest(check=check["id"]):
                if check["tenant"] != receipt["tenant"]:
                    result = "denied"
                elif check["table_uuid"] != receipt["table_uuid"] or check["ref"] != receipt["ref"]:
                    result = "binding_changed"
                else:
                    parents = check["retained_parents"]
                    pin = check["selected"]
                    seen = set()
                    while pin in parents and pin not in seen and pin != receipt["introducing"]:
                        seen.add(pin)
                        pin = parents[pin]
                    if pin == receipt["introducing"]:
                        result = "included"
                    elif pin is None:
                        result = "outside_lineage"
                    else:
                        result = "insufficient_history"
                self.assertEqual(check["expect"], result)


class ScopedReadVectors(unittest.TestCase):
    def test_exact_pin_resolver_model(self):
        """Model the provider boundary before connecting it to PG privilege checks and I/O."""
        vectors = json.loads(FIXTURE.with_name("scoped-read-v1.json").read_text())
        self.assertEqual(vectors["version"], 1)
        grant = vectors["grant"]
        cases = vectors["cases"]
        self.assertEqual(len(cases), len({case["id"] for case in cases}))
        for case in cases:
            with self.subTest(case=case["id"]):
                session = case.get("session_role", grant["mapping_role"])
                role = case.get("check_as_role", session)
                catalog_calls = 0
                if not case.get("select_granted", True) or role != grant["mapping_role"]:
                    status = "denied"
                elif case.get("available_generation", grant["generation"]) != grant["generation"]:
                    status = "changed_binding"
                else:
                    catalog_calls += 1
                    state = case.get("catalog_state", "available")
                    if state != "available":
                        status = {"replacement": "changed_binding", "pin_absent": "pin_unavailable",
                                  "unavailable": "unavailable"}[state]
                    elif case.get("object_scope", "within_both_roots") != "within_both_roots":
                        status = "denied"
                    else:
                        status = "pin"
                self.assertEqual((status, catalog_calls), (case["expect_core"], case["catalog_calls"]))


if __name__ == "__main__":
    unittest.main()
