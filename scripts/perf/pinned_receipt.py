"""Collect correctness-qualified timings for the shipped TPC-DS canaries."""

import hashlib
import json
import re
import statistics
import time
from contextlib import closing
from decimal import Decimal
from pathlib import Path
from uuid import UUID

TPCDS_TABLES = {
    "catalog_sales", "customer", "date_dim", "item", "store", "store_returns",
    "store_sales", "web_sales",
}
SERVER_OPTIONS = {"endpoint", "warehouse", "catalog_prefix", "expected_table_uuid",
                  "storage_root", "s3_endpoint", "s3_region"}


def result_digest(rows):
    def encode(value):
        if isinstance(value, Decimal) and value.is_finite():
            number = format(value, "f") if value else "0"
            if "." in number:
                number = number.rstrip("0").rstrip(".")
            return {"numeric": number}
        raise TypeError(f"Unsupported result type: {type(value).__name__}")

    lines = sorted(json.dumps(row, ensure_ascii=False, separators=(",", ":"),
                              default=encode, allow_nan=False).encode("utf-8")
                   for row in rows)
    digest = hashlib.sha256()
    for line in lines:
        digest.update(line + b"\n")
    return len(lines), digest.hexdigest()


def compare_result(expected, rows):
    count, digest = result_digest(rows)
    matches = count == expected["row_count"] and digest == expected["sha256"]
    return {"status": "PASS" if matches else "RESULT_MISMATCH",
            "row_count": count, "sha256": digest}


def verify_attachments(expected, actual):
    for name, target in expected.items():
        if actual.get(name) != target:
            raise ValueError(f"{name}: attachment differs from the oracle locator, pin or server binding")


def summarize_samples(samples):
    elapsed = sorted(s["elapsed_seconds"] for s in samples if s["status"] == "PASS")
    n = len(elapsed)
    complete = n == len(samples)
    return {
        "attempts": len(samples), "matches": n,
        "median_seconds": statistics.median(elapsed) if complete and n >= 5 else None,
        "p95_seconds": elapsed[(95 * n + 99) // 100 - 1] if complete and n >= 100 else None,
        "p99_seconds": elapsed[(99 * n + 99) // 100 - 1] if complete and n >= 100 else None,
    }


def load_oracle(path, query_names, query_dir):
    data = json.loads(Path(path).read_text())
    if (type(data["version"]) is not int or data["version"] != 1
            or not all(isinstance(data[key], str) and data[key].strip()
                       for key in ("reference_engine", "dataset"))):
        raise ValueError("Expected v1 with a reference engine and dataset")
    if set(data["tables"]) != TPCDS_TABLES or set(data["queries"]) != set(query_names):
        raise ValueError("Oracle must cover every shipped canary and its tables")
    for name, target in data["tables"].items():
        pin = target["snapshot_id"]
        if (set(target) != {"table", "snapshot_id", "server"} or not isinstance(target["table"], str)
                or not target["table"].strip() or not isinstance(pin, str)
                or not re.fullmatch(r"0|-?[1-9][0-9]{0,18}", pin)
                or not -(2**63) <= int(pin) < 2**63):
            raise ValueError(f"{name}: expected a catalog locator and canonical Iceberg snapshot ID")
        server = target["server"]
        if (set(server) != SERVER_OPTIONS
                or any(not isinstance(v, str) or (not v and k != "catalog_prefix")
                       for k, v in server.items())
                or str(UUID(server["expected_table_uuid"])) != server["expected_table_uuid"]):
            raise ValueError(f"{name}: expected a canonical bound REST/S3 server")
    queries = {}
    for name, expected in data["queries"].items():
        raw = (Path(query_dir) / f"{name}.sql").read_bytes()
        if (set(expected) != {"sql_sha256", "row_count", "sha256"}
                or hashlib.sha256(raw).hexdigest() != expected["sql_sha256"]):
            raise ValueError(f"{name}: query differs from reference SQL")
        if type(expected["row_count"]) is not int or expected["row_count"] < 0:
            raise ValueError(f"{name}: invalid expected row count")
        if not re.fullmatch(r"[0-9a-f]{64}", expected["sha256"]):
            raise ValueError(f"{name}: invalid reference result digest")
        queries[name] = raw.decode("utf-8")
    return {key: data[key] for key in ("version", "reference_engine", "dataset", "tables", "queries")}, queries


def preflight(cur, schema, tables, timeout):
    from psycopg2 import sql

    cur.execute(sql.SQL("SET LOCAL search_path TO pg_catalog, {}, pg_temp").format(
        sql.Identifier(schema)))
    cur.execute("SELECT set_config('statement_timeout', %s, true)", (f"{timeout}s",))

    # PREPARE parses and analyzes, taking relation locks without initializing
    # an FDW executor. Deallocation leaves the locks held by this transaction.
    for name in sorted(tables):
        cur.execute(sql.SQL("PREPARE pin_preflight AS SELECT 1 FROM ONLY {}").format(
            sql.Identifier(schema, name)))
        cur.execute("DEALLOCATE pin_preflight")
    cur.execute("""
        SELECT c.relname, t.ftoptions, s.srvoptions
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_foreign_table t ON t.ftrelid = c.oid
        JOIN pg_foreign_server s ON s.oid = t.ftserver
        JOIN pg_foreign_data_wrapper w ON w.oid = s.srvfdw
        WHERE n.nspname = %s AND c.relname = ANY(%s)
          AND NOT c.relhassubclass
          AND w.fdwhandler = 'lakehouse.lakehouse_fdw_handler()'::regprocedure
    """, (schema, sorted(tables)))
    actual = {}
    for name, table_options, server_options in cur.fetchall():
        table = dict(option.split("=", 1) for option in table_options or [])
        server = dict(option.split("=", 1) for option in server_options or [])
        actual[name] = {"table": table.get("table"), "snapshot_id": table.get("snapshot_id"),
                        "server": {key: server.get(key) for key in SERVER_OPTIONS}}
    verify_attachments(tables, actual)


def attempt(cur, query, expected):
    from psycopg2 import Error

    cur.execute("SAVEPOINT benchmark_attempt")
    started = time.perf_counter()
    try:
        cur.execute(query)
        rows = cur.fetchall()
        elapsed = time.perf_counter() - started
    except Error as exc:
        elapsed = time.perf_counter() - started
        canceled = exc.pgcode == "57014"
        timed_out = canceled and exc.diag.message_primary == "canceling statement due to statement timeout"
        status = "TIMEOUT" if timed_out else "CANCELED" if canceled else "ERROR"
        result = {"status": status,
                  "sqlstate": exc.pgcode, "elapsed_seconds": elapsed}
        if not cur.connection.closed:
            try:
                cur.execute("ROLLBACK TO SAVEPOINT benchmark_attempt")
                cur.execute("RELEASE SAVEPOINT benchmark_attempt")
            except Error as cleanup:
                result["cleanup_error"] = {"sqlstate": cleanup.pgcode}
        return result
    try:
        result = compare_result(expected, rows)
    except (ValueError, TypeError) as exc:
        result = {"status": "RESULT_ENCODING_ERROR", "detail": str(exc)}
    try:
        cur.execute("RELEASE SAVEPOINT benchmark_attempt")
    except Error as cleanup:
        result["observed_status"] = result["status"]
        result["status"] = "CLEANUP_ERROR"
        result["cleanup_error"] = {"sqlstate": cleanup.pgcode}
    result["elapsed_seconds"] = elapsed
    return result


def run_pinned(connect, args, query_names, database_error):
    # Reserve a fresh destination even if loading the oracle or connecting fails.
    with Path(args.receipt).open("x") as output:
        return write_receipt(connect, args, query_names, database_error, output)


def write_receipt(connect, args, query_names, database_error, output):
    receipt = {
        "version": 1, "status": "FAIL", "source_sha": args.source_sha,
        "package_sha256": args.package_sha256, "configuration": args.configuration,
        "cache_state": "after_warmup" if args.warmup else "uncontrolled_no_warmup",
        "workload": args.workload,
        "fixture_schema": args.fixture_schema, "timeout_seconds": args.timeout,
        "requested_samples_per_query": args.samples,
        "queries": {name: {"status": "NOT RUN", "samples": []} for name in query_names},
    }
    try:
        oracle, queries = load_oracle(args.oracle, query_names, args.queries)
        receipt["oracle"] = oracle
        with closing(connect()) as conn, conn.cursor() as cur:
            conn.set_session(readonly=True, autocommit=False, isolation_level="READ COMMITTED")
            preflight(cur, args.fixture_schema, oracle["tables"], args.timeout)
            cur.execute("SELECT version(), lakehouse.version(), current_setting('work_mem'), "
                        "current_setting('max_parallel_workers_per_gather'), current_setting('jit')")
            keys = ["postgres", "lakehouse", "work_mem", "max_parallel_workers_per_gather", "jit"]
            receipt["observed_settings"] = dict(zip(keys, cur.fetchone()))
            for name in query_names:
                record = receipt["queries"][name]
                record["status"] = "FAIL"
                if args.warmup:
                    record["warmup"] = attempt(cur, queries[name], oracle["queries"][name])
                    if conn.closed:
                        break
                    if record["warmup"]["status"] != "PASS":
                        continue
                for _ in range(args.samples):
                    sample = attempt(cur, queries[name], oracle["queries"][name])
                    record["samples"].append(sample)
                    if sample["status"] != "PASS":
                        break
                record["summary"] = summarize_samples(record["samples"])
                if record["summary"]["matches"] == args.samples:
                    record["status"] = "PASS"
                if conn.closed:
                    break
        if all(q["status"] == "PASS" for q in receipt["queries"].values()):
            receipt["status"] = "PASS"
    except database_error as exc:
        receipt["database_error"] = {"sqlstate": exc.pgcode}
    except (ValueError, TypeError, KeyError, OSError) as exc:
        receipt["validation_error"] = str(exc)
    finally:
        output.write(json.dumps(receipt, indent=2) + "\n")
    return receipt["status"] == "PASS"
