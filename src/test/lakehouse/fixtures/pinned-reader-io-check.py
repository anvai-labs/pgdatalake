"""Measure range amplification for the same pinned TPC-DS date_dim read."""

import argparse
import json
import re
import time
import uuid
from collections import Counter, defaultdict
from importlib import import_module
from pathlib import Path

from publication_fixture import sql

scoped = import_module("scoped-read-check")
DDL = Path(__file__).resolve().parents[4] / "scripts/perf/tpcds/ddl.sql"
TABLE = "tpcds.date_dim"
KEY = 2450992


def measure(requests):
    counts = Counter()
    sizes = Counter()
    intervals = Counter()
    coverage = defaultdict(list)
    files = set()
    for target, method, path, status, size, byte_range in requests:
        assert status in (200, 206), (target, method, path, status)
        data = path.endswith(".parquet")
        kind = ("catalog" if target == "catalog" else
                "data_head" if data and method == "HEAD" else
                "data_range" if data and byte_range else
                "data_whole" if data else
                "metadata_head" if method == "HEAD" else "metadata_whole")
        counts[kind] += 1
        sizes[kind] += size
        if data:
            files.add(path)
        if byte_range and data:
            match = re.fullmatch(r"bytes=([0-9]+)-([0-9]+)", byte_range)
            assert match and status == 206, (path, status, byte_range)
            start, end = int(match[1]), int(match[2]) + 1
            assert end - start == size, (path, byte_range, size)
            coverage[path].append((start, end))
            intervals[path, byte_range] += 1
    unique_bytes = 0
    for ranges in coverage.values():
        covered_until = 0
        for start, end in sorted(ranges):
            unique_bytes += max(0, end - max(start, covered_until))
            covered_until = max(covered_until, end)
    kinds = ("catalog", "metadata_head", "metadata_whole", "data_head", "data_range", "data_whole")
    return {
        "requests": {kind: counts[kind] for kind in kinds},
        "response_bytes": {kind: sizes[kind] for kind in kinds},
        "unique_data_files": len(files), "unique_data_intervals": len(intervals),
        "repeated_data_intervals": sum(n - 1 for n in intervals.values()),
        "unique_ranged_data_bytes": unique_bytes,
        "overlapping_ranged_data_bytes": sizes["data_range"] - unique_bytes,
        "forbidden_root_requests": sum(target == "forbidden" for target, *_ in requests),
        "range_sizes": dict(Counter(size for target, method, path, status, size, byte_range
                                    in requests if byte_range and path.endswith(".parquet"))),
    }


def exercise(db, pin, samples):
    prefix, catalog_path, enrolled = scoped.catalog_identity("tpcds", "date_dim")
    root = enrolled["metadata"]["location"]
    scope = "/" + root.removeprefix("s3://") + "/"
    uuid_text = enrolled["metadata"]["table-uuid"]
    reference = scoped.trino(
        f"SELECT d_date_sk, d_year FROM iceberg.{TABLE} FOR VERSION AS OF {pin} WHERE d_date_sk={KEY}"
    )
    expected = reference.replace("\t", "|")
    assert expected == "2450992|1998", expected
    expected_count = scoped.trino(f"SELECT count(*) FROM iceberg.{TABLE} FOR VERSION AS OF {pin}")
    expected_details = scoped.trino(
        f"SELECT d_date_id, CAST(d_date AS varchar), d_day_name FROM iceberg.{TABLE} "
        f"FOR VERSION AS OF {pin} WHERE d_date_sk={KEY}"
    ).split("\t")
    requests = []

    def record(target, upstream, method, path, headers):
        status, response_headers, body = scoped.upstream(upstream, method, path, dict(headers))
        requests.append((target, method, path, status, len(body), headers.get("Range")))
        return status, response_headers, body

    def catalog_response(method, path, headers):
        assert path == "/catalog" + catalog_path
        return record("catalog", "127.0.0.1:8181", method, path, headers)

    def storage_response(method, path, headers):
        if not path.startswith(scope):
            requests.append(("forbidden", method, path, 403, 0, None))
            return 403, [], b""
        return record("object", "127.0.0.1:9000", method, path, headers)

    settings = "SET max_parallel_workers_per_gather=0; SET jit=off;"
    with scoped.recorder(catalog_response) as catalog, scoped.recorder(storage_response) as storage:
        sql(db, f"""
            CREATE SERVER bench FOREIGN DATA WRAPPER lakehouse_fdw OPTIONS
                (endpoint '{catalog}/catalog', warehouse 'smoke', catalog_prefix '{prefix}',
                 s3_endpoint '{storage}', s3_region 'us-east-1',
                 storage_root '{root}', expected_table_uuid '{uuid_text}');
            CREATE USER MAPPING FOR CURRENT_USER SERVER bench OPTIONS
                (s3_access_key 'lakehouse-test', s3_secret_key 'lakehouse-test-password');
            CREATE FOREIGN TABLE narrow (d_date_sk bigint, d_year bigint) SERVER bench
                OPTIONS (table '{TABLE}', snapshot_id '{pin}');
        """)
        ddl = re.search(r"CREATE FOREIGN TABLE date_dim \(.+?\) SERVER smoke_server",
                        DDL.read_text(), re.DOTALL)[0]
        sql(db, ddl.replace("SERVER smoke_server", "SERVER bench")
            + f" OPTIONS (table '{TABLE}', snapshot_id '{pin}');")
        query = f"SELECT d_date_sk, d_year FROM date_dim WHERE d_date_sk={KEY}"
        sql(db, f"{settings} PREPARE parse_only AS {query}; DEALLOCATE parse_only;")
        assert not requests, requests
        explain = json.loads(sql(db, settings + " EXPLAIN (FORMAT JSON, VERBOSE) " + query))[0]["Plan"]
        explain_trace = measure(requests)
        assert explain_trace["unique_data_files"] == 0, explain_trace

        trials = {"date_dim": [], "narrow": []}
        for _ in range(samples):
            for table, observations in trials.items():
                requests.clear()
                started = time.monotonic()
                rows = sql(db, f"{settings} SELECT d_date_sk, d_year FROM {table} WHERE d_date_sk={KEY}")
                elapsed = time.monotonic() - started
                assert rows == expected, (table, rows)
                observations.append(measure(requests) | {"elapsed_seconds": elapsed})

        requests.clear()
        assert sql(db, f"{settings} SELECT d_year FROM date_dim WHERE d_date_sk={KEY}") == "1998"
        qual_only = measure(requests)
        requests.clear()
        row = json.loads(sql(db, f"{settings} SELECT row_to_json(d) FROM date_dim d WHERE d_date_sk={KEY}"))
        assert row["d_date_sk"] == KEY and row["d_year"] == 1998 and len(row) == 28, row
        assert [row["d_date_id"], row["d_date"], row["d_day_name"]] == expected_details, row
        whole_row = measure(requests)
        assert whole_row["unique_data_files"] > 0, whole_row
        requests.clear()
        assert sql(db, f"{settings} SELECT tableoid::regclass FROM date_dim WHERE d_date_sk={KEY}") == "date_dim"
        system_column = measure(requests)
        requests.clear()
        assert sql(db, settings + "SELECT count(*) FROM (SELECT 1 FROM date_dim OFFSET 0) q") == expected_count
        empty_projection = measure(requests)
        requests.clear()
        with scoped.login(db, sql(db, "SELECT current_user")) as session:
            assert not session(settings + "SET plan_cache_mode=force_generic_plan")
            assert not session(
                "PREPARE pinned(bigint) AS SELECT d_year, d_date_sk FROM date_dim WHERE d_date_sk=$1"
            )
            assert session(f"EXECUTE pinned({KEY})") == ["1998|2450992"]
            assert not session("EXECUTE pinned(-1)")
            assert session(f"EXECUTE pinned({KEY})") == ["1998|2450992"]
            assert not session("ALTER FOREIGN TABLE date_dim DROP COLUMN d_date_id")
            assert session(f"EXECUTE pinned({KEY})") == ["1998|2450992"]
        prepared_reordered_and_dropped = measure(requests)
        requests.clear()
        assert sql(db, f"""
            {settings}
            SELECT g.key, (SELECT d_year FROM date_dim d WHERE d.d_date_sk=g.key OFFSET 0)
            FROM (VALUES ({KEY}::bigint), (-1::bigint), ({KEY}::bigint)) g(key);
        """) == f"{KEY}|1998\n-1|\n{KEY}|1998"
        rescans = measure(requests)

    return {
        "snapshot_id": str(pin), "table_uuid": uuid_text, "storage_root": root,
        "server": sql(db, "SELECT version()"), "server_settings": settings,
        "rows": [[KEY, 1998]], "samples_per_projection": samples, "parse_only_requests": 0,
        "explain": {"plan": explain, "trace": explain_trace},
        "execution": trials, "whole_row": whole_row, "filter_column_only": qual_only,
        "system_column": system_column, "empty_projection": empty_projection,
        "prepared_reordered_and_dropped": prepared_reordered_and_dropped,
        "rescans": rescans,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", required=True, type=int)
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--require-projection", action="store_true")
    parser.add_argument("--projection-baseline", type=Path,
                        help="qualification receipt from the projection-only variant on the same release configuration")
    args = parser.parse_args()
    if not __debug__ or args.samples < 1:
        parser.error("requires Python assertions and at least one sample")
    db = "reader_io_" + uuid.uuid4().hex
    sql("postgres", f"CREATE DATABASE {db}")
    try:
        sql(db, "CREATE EXTENSION lakehouse")
        evidence = exercise(db, args.snapshot, args.samples)
        with args.output.open("x") as output:
            json.dump(evidence, output, indent=2, sort_keys=True)
            output.write("\n")
        if args.require_projection:
            for wide, narrow in zip(evidence["execution"]["date_dim"], evidence["execution"]["narrow"]):
                for key in ("requests", "response_bytes", "range_sizes"):
                    assert wide[key] == narrow[key], (key, wide[key], narrow[key])
        if args.projection_baseline:
            baseline = json.loads(args.projection_baseline.read_text())
            for key in ("snapshot_id", "table_uuid", "storage_root", "rows", "server", "server_settings"):
                assert evidence[key] == baseline[key], (key, evidence[key], baseline[key])
            for table, observations in evidence["execution"].items():
                before = baseline["execution"][table]
                assert len(observations) == len(before)
                for current, prior in zip(observations, before):
                    assert current["requests"]["data_whole"] == prior["requests"]["data_whole"] == 0
                    assert current["requests"]["data_range"] <= 64
                    assert current["requests"]["data_range"] < prior["requests"]["data_range"]
                    assert 4 * current["response_bytes"]["data_range"] <= 5 * prior["response_bytes"]["data_range"]
    finally:
        sql("postgres", f"DROP DATABASE {db}")


if __name__ == "__main__":
    main()
