"""Check externally collected governed-read observations against the public v1 fixture."""

import argparse
from collections import Counter
import json
from pathlib import Path
import re
import sys
from uuid import UUID


FIXTURE = Path(__file__).resolve().parents[2] / "specs/lakehouse/modules/fixtures/governed-read-v1.json"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def text(value):
    return isinstance(value, str) and bool(value.strip())


def number(value):
    return type(value) is int and value >= 0


def pin(value):
    return (isinstance(value, str) and re.fullmatch(r"(?:0|-?[1-9][0-9]{0,18})", value)
            and -(2**63) <= int(value) < 2**63)


def cases(fixture):
    """One ordinary and two distinct aggregate statements for each viewer and pin."""
    result = {}
    for case in fixture["expected"]:
        viewer, label = case["viewer"], case["pin"]
        results = {"rows": case["rows"], "count": [[case["count_star"]]],
                   "aggregates": [[case[key] for key in ("count_star", "count_amount", "sum_amount")]]}
        for query, rows in results.items():
            result[f"{viewer}/{label}/{query}"] = (viewer, label, "ok", rows)
    for case in fixture["refusals"]:
        result[case["case"]] = ("viewer-a", "9007199254740993", case["status"], [])
    return result


def index_stream(observed, name, expected):
    records = observed.get(name)
    require(isinstance(records, list), f"{name}: missing observations")
    indexed = {}
    for record in records:
        require(isinstance(record, dict), f"{name}: malformed record")
        attempt = record.get("attempt_id")
        require(text(attempt) and attempt not in indexed, f"{name}: invalid or duplicate attempt ID")
        indexed[attempt] = record
    require(indexed.keys() == expected, f"{name}: attempt IDs differ from the run assignment")
    return indexed


def row_bag(rows):
    require(isinstance(rows, list), "expected a JSON array of SQL result rows")
    # Canonical JSON preserves the difference between true, 1.0, 1 and "1".
    return Counter(json.dumps(row, sort_keys=True, allow_nan=False) for row in rows)


def verify(fixture, request, observations):
    """Join three recorder streams to an independent run assignment or raise ValueError."""
    for document in (fixture, request, observations):
        require(isinstance(document, dict) and type(document.get("version")) is int
                and document["version"] == 1, "expected a version 1 object")
    for key in ("run_id", "profile", "authority", "installation"):
        require(text(request.get(key)) and observations.get(key) == request[key], f"run header: {key}")
    source_uuid = request.get("source_uuid")
    require(text(source_uuid) and str(UUID(source_uuid)) == source_uuid, "invalid source UUID")
    require(request.get("ref") == fixture["ref"], "unexpected source ref")
    pins = request.get("pins")
    require(isinstance(pins, dict) and pins.keys() == fixture["pins"].keys()
            and all(pin(value) for value in pins.values())
            and len(set(pins.values())) == len(pins), "invalid source pin mapping")

    viewers = request.get("viewers")
    require(isinstance(viewers, dict) and viewers.keys() == fixture["viewers"].keys(),
            "viewer coverage")
    for binding in viewers.values():
        require(isinstance(binding, dict)
                and set(binding) == {"principal", "binding_id", "mapping_role", "generation", "provider_scope_id"}
                and all(text(binding[key]) for key in ("principal", "binding_id", "mapping_role", "provider_scope_id"))
                and number(binding["generation"]) and binding["generation"] > 0, "invalid viewer binding")
    require(len({v["principal"] for v in viewers.values()}) == len(viewers), "ambiguous viewer mapping")

    require(len({v["provider_scope_id"] for v in viewers.values()}) == len(viewers),
            "ambiguous provider scope mapping")

    expected = cases(fixture)
    attempts = request.get("attempts")
    require(isinstance(attempts, dict) and expected.keys() == attempts.keys()
            and all(text(a) for a in attempts.values())
            and len(set(attempts.values())) == len(attempts), "invalid case assignment")
    all_ids = set(attempts.values())
    sql = index_stream(observations, "sql", all_ids)
    trace = index_stream(observations, "trace", all_ids)
    positive_ids = {attempts[k] for k, (_, _, status, _) in expected.items() if status == "ok"}
    reference = index_stream(observations, "reference", positive_ids)
    plan_scopes = {}
    for case, (viewer, label, status, rows) in expected.items():
        attempt = attempts[case]
        query, calls = sql[attempt], trace[attempt]
        source_pin = pins[label]
        context = dict(viewers[viewer], source_uuid=source_uuid, ref=request["ref"], requested_pin=source_pin)
        records = [query, calls]
        if status == "ok":
            records.append(reference[attempt])
        for record in records:
            actual = record.get("context")
            require(isinstance(actual, dict) and type(actual.get("generation")) is int
                    and actual == context, f"{case}: viewer, binding or pin mismatch")
        require(query.get("status") == status, f"{case}: unexpected terminal status")
        require(row_bag(query.get("rows")) == row_bag(rows), f"{case}: SQL rows differ")
        require(calls.get("complete") is True, f"{case}: incomplete transport trace")
        for key in ("raw_requests", "remote_requests", "governed_plans"):
            require(number(calls.get(key)), f"{case}: invalid {key} counter")
        require(calls["raw_requests"] == 0, f"{case}: reader requested raw data")
        require(calls["remote_requests"] >= calls["governed_plans"], f"{case}: inconsistent counters")
        if status == "ok":
            oracle = reference[attempt]
            require("fault" in calls and calls["fault"] is None
                    and calls["governed_plans"] > 0, f"{case}: missing governed request")
            require(text(query.get("plan_id")) and query["plan_id"] == calls.get("plan_id"),
                    f"{case}: protected result handle mismatch")
            scope = context["provider_scope_id"]
            require(calls.get("effective_scope_id") == scope and oracle.get("effective_scope_id") == scope,
                    f"{case}: provider or reference observed a different effective scope")
            plan_id = query["plan_id"]
            require(plan_scopes.setdefault(plan_id, (scope, source_pin)) == (scope, source_pin),
                    f"{case}: protected plan handle reused across scopes or source pins")
            output = calls.get("output")
            require(isinstance(output, dict) and output.get("plan_id") == plan_id
                    and output.get("kind") in ("protected_object", "protected_inline")
                    and number(output.get("consumed_bytes")) and output["consumed_bytes"] > 0,
                    f"{case}: no authorized output consumed from this plan")
            if output["kind"] == "protected_object":
                require(calls["remote_requests"] > calls["governed_plans"],
                        f"{case}: missing protected output request")
            require(calls.get("source_pin") == source_pin and oracle.get("source_pin") == source_pin,
                    f"{case}: missing independent source-pin evidence")
            require(row_bag(oracle.get("rows")) == row_bag(rows), f"{case}: reference rows differ")
        else:
            require(calls.get("fault") == case, f"{case}: fault injection mismatch")
            if case == "revoked-before-dispatch":
                require(calls["remote_requests"] == 0, f"{case}: unexpected remote request")
            if case == "generation-changed-during-fill":
                fill = calls.get("fill")
                require(isinstance(fill, dict) and all(number(fill.get(key)) for key in
                        ("started_seq", "changed_seq", "refused_seq", "new_generation"))
                        and fill["started_seq"] < fill["changed_seq"] < fill["refused_seq"]
                        and fill["new_generation"] > context["generation"],
                        f"{case}: missing ordered fill, grant change and refusal")
            if case == "pin-expired":
                require(calls.get("pin_lookup") == {"source_pin": source_pin, "status": "absent"},
                        f"{case}: missing authoritative lookup of the requested pin")
            if case in {"generation-changed-during-fill", "pin-expired", "source-pin-unproven",
                        "governed-plan-unavailable", "raw-only-on-protected-table"}:
                require(calls["governed_plans"] > 0, f"{case}: provider refusal was never exercised")

    return {"status": "observations_match", "run_id": request["run_id"], "profile": request["profile"],
            "attempts": len(expected)}


def unique_keys(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, f"duplicate JSON field {key}")
        result[key] = value
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("request", type=Path)
    parser.add_argument("observations", type=Path)
    args = parser.parse_args()
    try:
        inputs = [json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=unique_keys)
                  for path in (FIXTURE, args.request, args.observations)]
        print(json.dumps(verify(*inputs)))
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"governed-read verification failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
