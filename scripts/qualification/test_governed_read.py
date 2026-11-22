"""Synthetic recorder output for replay checks. This fixture makes no remote requests."""

import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from governed_read import FIXTURE, verify


def sample(fixture):
    header = dict(version=1, run_id="run-23", profile="synthetic", authority="catalog-a",
                  installation="install-a")
    request = dict(header, source_uuid="eb3558ea-b940-48d7-a17d-e4f832793dd0", ref="main",
                   pins={"9007199254740993": "81", "9007199254740991": "73"},
                   viewers={}, attempts={})
    observed = dict(header, sql=[], trace=[], reference=[])
    for viewer in fixture["viewers"]:
        request["viewers"][viewer] = dict(principal="principal-" + viewer, mapping_role="reader",
                                         binding_id="binding-" + viewer, generation=1,
                                         provider_scope_id="scope-" + viewer)

    def record(case_id, viewer, label, rows, status):
        pin = request["pins"][label]
        attempt = "a-" + str(len(request["attempts"]))
        request["attempts"][case_id] = attempt
        context = dict(request["viewers"][viewer], source_uuid=request["source_uuid"],
                       ref="main", requested_pin=pin)
        base = dict(attempt_id=attempt, context=context)
        sql = dict(base, status=status, rows=rows)
        trace = dict(base, complete=True, raw_requests=0, remote_requests=0, governed_plans=0, fault=case_id)
        if status == "ok":
            sql["plan_id"] = "p-" + attempt
            trace.update(governed_plans=1, remote_requests=2, fault=None, source_pin=pin,
                         effective_scope_id=context["provider_scope_id"], plan_id=sql["plan_id"],
                         output=dict(kind="protected_object", plan_id=sql["plan_id"], consumed_bytes=32))
            observed["reference"].append(dict(base, source_pin=pin, rows=copy.deepcopy(rows),
                                              effective_scope_id=context["provider_scope_id"]))
        elif case_id != "revoked-before-dispatch":
            trace.update(governed_plans=1, remote_requests=1)
        if case_id == "generation-changed-during-fill":
            trace["fill"] = dict(started_seq=0, changed_seq=2, refused_seq=3, new_generation=2)
        if case_id == "pin-expired":
            trace["pin_lookup"] = dict(source_pin=pin, status="absent")
        observed["sql"].append(sql)
        observed["trace"].append(trace)

    for case in fixture["expected"]:
        queries = {"rows": list(reversed(case["rows"])), "count": [[case["count_star"]]],
                   "aggregates": [[case[key] for key in ("count_star", "count_amount", "sum_amount")]]}
        for name, rows in queries.items():
            record("/".join((case["viewer"], case["pin"], name)), case["viewer"],
                   case["pin"], rows, "ok")
    for case in fixture["refusals"]:
        record(case["case"], "viewer-a", "9007199254740993", [], case["status"])
    return request, observed


class GovernedReadEvidence(unittest.TestCase):
    def setUp(self):
        self.fixture = json.loads(FIXTURE.read_text())
        self.request, self.observed = sample(self.fixture)

    def test_complete_unordered_evidence(self):
        for key in ("sql", "trace", "reference"):
            self.observed[key].reverse()
        self.assertEqual(verify(self.fixture, self.request, self.observed),
                         dict(status="observations_match", run_id="run-23", profile="synthetic", attempts=18))

    def test_provider_inline_result(self):
        trace = self.observed["trace"][0]
        trace["remote_requests"] = 1
        trace["output"]["kind"] = "protected_inline"
        verify(self.fixture, self.request, self.observed)

    def test_handle_reused_by_different_principal_or_pin(self):
        for other in (3, 6):
            with self.subTest(other=other):
                obs = copy.deepcopy(self.observed)
                handle = obs["sql"][0]["plan_id"]
                obs["sql"][other]["plan_id"] = handle
                obs["trace"][other]["plan_id"] = handle
                obs["trace"][other]["output"]["plan_id"] = handle
                with self.assertRaises(ValueError):
                    verify(self.fixture, self.request, obs)

    def test_failures(self):
        # Each mutation targets the recorded result or one independently captured signal.
        changes = [
            ("profile", lambda r, o: o.update(profile="other")),
            ("same provider scope", lambda r, o: r["viewers"]["viewer-a"].update(provider_scope_id="scope-viewer-b")),
            ("wrong provider scope", lambda r, o: o["trace"][0].update(effective_scope_id="scope-viewer-b")),
            ("wrong oracle scope", lambda r, o: o["reference"][0].update(effective_scope_id="scope-viewer-b")),
            ("output not used", lambda r, o: o["trace"][1]["output"].update(consumed_bytes=0)),
            ("output scope", lambda r, o: o["trace"][1]["output"].update(plan_id="unrelated")),
            ("output source", lambda r, o: o["trace"][1]["output"].update(kind="unknown")),
            ("unrecorded output GET", lambda r, o: o["trace"][1].update(remote_requests=1)),
            ("no observed cold fill dispatch", lambda r, o: o["trace"][13].update(governed_plans=0)),
            ("fill never started", lambda r, o: o["trace"][13].pop("fill")),
            ("late grant change", lambda r, o: o["trace"][13]["fill"].update(changed_seq=5)),
            ("unchanged grant", lambda r, o: o["trace"][13]["fill"].update(new_generation=1)),
            ("missing expiry proof", lambda r, o: o["trace"][15].pop("pin_lookup")),
            ("expiry of other pin", lambda r, o: o["trace"][15]["pin_lookup"].update(source_pin="74")),
            ("missing reference", lambda r, o: o["reference"].pop()),
            ("missing refusal", lambda r, o: o["trace"].pop()),
            ("duplicate SQL", lambda r, o: o["sql"].append(o["sql"][0])),
            ("case coverage", lambda r, o: r["attempts"].popitem()),
            ("pin precision", lambda r, o: r["pins"].update({"9007199254740993": 81})),
            ("equal source pins", lambda r, o: r["pins"].update({"9007199254740993": "73"})),
            ("same principal", lambda r, o: r["viewers"]["viewer-a"].update(principal="principal-viewer-b")),
            ("swapped reference context", lambda r, o: o["reference"][0].update(context=o["reference"][3]["context"])),
            ("source pin", lambda r, o: o["trace"][0].update(source_pin="73")),
            ("wrong handle", lambda r, o: o["sql"][0].update(plan_id="other-handle")),
            ("trace gap", lambda r, o: o["trace"][6].update(complete=False)),
            ("raw GET", lambda r, o: o["trace"][0].update(raw_requests=1)),
            ("missing counter", lambda r, o: o["trace"][3].pop("raw_requests")),
            ("false numeric counter", lambda r, o: o["trace"][0].update(raw_requests=False)),
            ("locally fabricated count", lambda r, o: o["trace"][1].update(governed_plans=0)),
            ("missing duplicate", lambda r, o: o["sql"][0]["rows"].pop()),
            ("boolean count", lambda r, o: o["sql"][4].update(rows=[[True]])),
            ("masked count", lambda r, o: o["sql"][5].update(rows=[[1, 1, 70]])),
            ("reference count", lambda r, o: o["reference"][2].update(rows=[[4, 4, 180]])),
            ("undelivered refusal", lambda r, o: o["sql"][-1].update(rows=[["US", 2, 70]])),
            ("wrong refusal", lambda r, o: o["sql"][-1].update(status="unavailable")),
            ("wrong fault", lambda r, o: o["trace"][-1].update(fault="source-pin-unproven")),
            ("request after pre-dispatch refusal", lambda r, o: o["trace"][12].update(remote_requests=1)),
        ]
        for label, change in changes:
            with self.subTest(label=label):
                req, obs = copy.deepcopy((self.request, self.observed))
                change(req, obs)
                with self.assertRaises(ValueError):
                    verify(self.fixture, req, obs)

    def test_file_cli(self):
        script = Path(__file__).with_name("governed_read.py")
        with tempfile.TemporaryDirectory() as d:
            req, obs = Path(d, "request.json"), Path(d, "observations.json")
            req.write_text(json.dumps(self.request))
            obs.write_text(json.dumps(self.observed))
            run = subprocess.run([sys.executable, script, req, obs], capture_output=True, text=True)
            self.assertEqual(run.returncode, 0, run.stderr)
            self.assertEqual(json.loads(run.stdout)["status"], "observations_match")
            obs.write_text('{"version":1,"version":2}')
            run = subprocess.run([sys.executable, script, req, obs], capture_output=True, text=True)
            self.assertNotEqual(run.returncode, 0)
            self.assertIn("duplicate", run.stderr)
