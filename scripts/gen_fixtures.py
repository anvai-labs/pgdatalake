#!/usr/bin/env python3
"""Generate fixture data for the lakehouse test tables (specs/lakehouse/
10-testing.md §2 interop: fixtures are WRITTEN by PyIceberg and read by us).

Requires `make lakehouse-up` and the fixture venv:
    python3 -m venv build/venv
    build/venv/bin/pip install 'pyiceberg[pyarrow]' s3fs
Run:
    # host run (s3 override needed — the served endpoint is container-scoped):
    #   build/venv/bin/python scripts/gen_fixtures.py --s3-endpoint http://localhost:9000
    # inside the compose network (recommended; s3:9000 resolves natively):
    #   docker run --rm --network pgdatalake_default -v "$PWD":/work -w /work \
    #     python:3.12-slim bash -c "pip install -q 'pyiceberg[pyarrow]' s3fs && \
    #       python scripts/gen_fixtures.py --uri http://lakekeeper:8181/catalog"

Writes `--runs` appends of `--rows` rows each into analytics.events in the
`smoke` warehouse (created by lakekeeper-setup.sh). The catalog is the
Lakekeeper REST fixture; storage goes to the SeaweedFS S3 fixture with the
fake fixture credentials (RULES §6 — never real cloud credentials).

Client-side s3.endpoint is http://localhost:9000 (host view): per doc 03 §2
merge order the client wins over the warehouse's container-scoped endpoint,
exactly the override our Rust reader also applies.
"""

import argparse
import sys
import time

from pyiceberg.catalog import load_catalog
import pyarrow as pa


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--rows", type=int, default=1000)
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--uri", default="http://localhost:8181/catalog")
    # Only pass a client-side s3 endpoint when running OUTSIDE the compose
    # network (host runs). Inside, the served container-scoped endpoint
    # (s3:9000) is authoritative — it beats client config per doc 03 §2.
    ap.add_argument("--s3-endpoint", default=None)
    args = ap.parse_args()

    props = {
        "type": "rest",
        "uri": args.uri,
        "warehouse": "smoke",
        # fixture-local fake credentials (RULES §6); dotted pyiceberg keys
        "s3.access-key-id": "lakehouse-test",
        "s3.secret-access-key": "lakehouse-test-password",
        "s3.region": "us-east-1",
    }
    if args.s3_endpoint:
        props["s3.endpoint"] = args.s3_endpoint
    catalog = load_catalog("fixture", **props)
    table = catalog.load_table("analytics.events")

    base_ms = 1_700_000_000_000
    for run in range(args.runs):
        ids = pa.array(range(run * args.rows + 1, (run + 1) * args.rows + 1),
                        pa.int64())
        ats = pa.array(
            [base_ms + (run * args.rows + i) * 1000 for i in range(args.rows)],
            pa.timestamp("ms", tz="UTC"),
        ).cast(pa.timestamp("us", tz="UTC"))
        schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),  # table field is required
            pa.field("at", pa.timestamp("us", tz="UTC")),
        ])
        data = pa.Table.from_arrays([ids, ats], schema=schema)
        table.append(data)
        print(f"run {run + 1}/{args.runs}: appended {args.rows} rows -> "
              f"snapshot {table.current_snapshot().snapshot_id}")

    scanned = table.scan().to_arrow()
    print(f"pyiceberg self-check: scanned {scanned.num_rows} rows, "
          f"snapshots={len(table.metadata.snapshots)}")
    if scanned.num_rows != args.runs * args.rows:
        print("row count mismatch after appends", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
