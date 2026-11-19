#!/usr/bin/env python
"""Generate Iceberg conformance fixtures (specs/lakehouse/10-testing.md §2,
M1 read-half gate) written by PyIceberg and read by us:

  analytics.types_all   full primitive type matrix, 3 rows with NULLs mixed
  analytics.parts_multi partitioned by year(day), month(day), bucket[8](id),
                        truncate[2](name), identity(region)

RUN INSIDE the compose network (lakekeeper serves container-scoped s3
endpoints that beat client config in pyiceberg):

  docker run --rm --network pgdatalake_default \\
    -v <repo>/build/venv:/home/<user>/code/postgres/build/venv \\
    -v <repo>/anaconda3:/home/<user>/anaconda3 \\
    --entrypoint /home/<user>/code/postgres/build/venv/bin/python \\
    anvai-wsl-runner:ubuntu24-20261008 scripts/gen_conformance_fixtures.py

Env: LK_URI (default http://lakekeeper:8181/catalog), S3_EP (http://s3:9000).
Tables are dropped first, so the script is idempotent.
"""
import os
import sys

from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import BucketTransform, DayTransform, IdentityTransform, MonthTransform, TruncateTransform
from pyiceberg.types import (
    BinaryType, BooleanType, DateType, DoubleType, FixedType, FloatType,
    IntegerType, LongType, NestedField, StringType, TimeType,
    TimestampType, TimestamptzType, UUIDType, DecimalType,
)
import pyarrow as pa

catalog = load_catalog(
    "conformance",
    type="rest",
    uri=os.environ.get("LK_URI", "http://lakekeeper:8181/catalog"),
    warehouse="smoke",
    **{
        "s3.access-key-id": "lakehouse-test",
        "s3.secret-access-key": "lakehouse-test-password",
        "s3.region": "us-east-1",
    },
)


def recreate(name, schema, spec=None, props=None):
    try:
        catalog.drop_table(name)
    except Exception:
        pass
    return catalog.create_table(
        name,
        schema=schema,
        partition_spec=spec if spec is not None else PartitionSpec(),
        properties=props or {},
    )


# --- types_all: every read-side primitive (doc 05 type matrix) -------------
schema = Schema(
    NestedField(1, "b", BooleanType(), required=False),
    NestedField(2, "i", IntegerType(), required=False),
    NestedField(3, "l", LongType(), required=False),
    NestedField(4, "f", FloatType(), required=False),
    NestedField(5, "d", DoubleType(), required=False),
    NestedField(6, "dec", DecimalType(12, 2), required=False),
    NestedField(7, "day", DateType(), required=False),
    NestedField(8, "t", TimeType(), required=False),
    NestedField(9, "ts", TimestampType(), required=False),
    NestedField(10, "tsz", TimestamptzType(), required=False),
    NestedField(11, "s", StringType(), required=False),
    NestedField(12, "u", UUIDType(), required=False),
    NestedField(13, "fx", FixedType(8), required=False),
    NestedField(14, "bin", BinaryType(), required=False),
    NestedField(15, "id", IntegerType(), required=True),
)
arrow_schema = pa.schema([
    pa.field("b", pa.bool_()),
    pa.field("i", pa.int32()),
    pa.field("l", pa.int64()),
    pa.field("f", pa.float32()),
    pa.field("d", pa.float64()),
    pa.field("dec", pa.decimal128(12, 2)),
    pa.field("day", pa.date32()),
    pa.field("t", pa.time64("us")),
    pa.field("ts", pa.timestamp("us")),
    pa.field("tsz", pa.timestamp("us", tz="UTC")),
    pa.field("s", pa.string()),
    pa.field("u", pa.binary(16)),
    pa.field("fx", pa.binary(8)),
    pa.field("bin", pa.binary()),
    pa.field("id", pa.int32(), nullable=False),
])


def days(y, m, d):
    import datetime
    return (datetime.date(y, m, d) - datetime.date(1970, 1, 1)).days


def micros(h, mi, s, f=0):
    return ((h * 60 + mi) * 60 + s) * 1_000_000 + f


def uuid16(hexstr):
    return bytes.fromhex(hexstr.replace("-", ""))


rows = [
    # full-values row
    {"b": True, "i": -42, "l": 9_000_000_000_000, "f": 1.5, "d": -2.25,
     "dec": __import__("decimal").Decimal("1234567890.12"),
     "day": days(2026, 11, 19), "t": micros(10, 10, 10, 123456),
     "ts": micros(12, 0, 0) + (days(2026, 11, 19) * 86_400_000_000),
     "tsz": micros(13, 30, 5) + (days(2026, 11, 19) * 86_400_000_000),
     "s": "héllo wörld", "u": uuid16("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
     "fx": b"\x01\x02\x03\x04\x05\x06\x07\x08", "bin": b"\xde\xad\xbe\xef",
     "id": 1},
    # NULLs everywhere nullable
    {"b": None, "i": None, "l": None, "f": None, "d": None, "dec": None,
     "day": None, "t": None, "ts": None, "tsz": None, "s": None, "u": None,
     "fx": None, "bin": None, "id": 2},
    # boundary-ish row
    {"b": False, "i": 2_147_483_647, "l": -9_000_000_000_000, "f": -0.5,
     "d": 3.141592653589793, "dec": __import__("decimal").Decimal("-5.00"),
     "day": days(1970, 1, 1), "t": 0,
     "ts": days(1970, 1, 2) * 86_400_000_000, "tsz": None,
     "s": "zz", "u": uuid16("00000000-0000-1200-0000-000000000000"),
     "fx": bytes(8), "bin": None, "id": 3},
]
table = recreate("analytics.types_all", schema)
table.append(pa.Table.from_pylist(rows, schema=arrow_schema))
print("types_all:", table.scan().to_arrow().num_rows, "rows")

# --- parts_multi: one spec, four transform kinds ---------------------------
schema_p = Schema(
    NestedField(1, "id", IntegerType(), required=True),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "region", StringType(), required=False),
    NestedField(4, "day", DateType(), required=False),
    NestedField(5, "ts", TimestampType(), required=False),
)
spec = PartitionSpec(
    PartitionField(source_id=4, field_id=1000, transform=DayTransform(), name="day_day"),
    PartitionField(source_id=5, field_id=1001, transform=MonthTransform(), name="ts_month"),
    PartitionField(source_id=1, field_id=1002, transform=BucketTransform(8), name="id_bucket"),
    PartitionField(source_id=2, field_id=1003, transform=TruncateTransform(2), name="name_trunc"),
    PartitionField(source_id=3, field_id=1004, transform=IdentityTransform(), name="region"),
)
tp = recreate("analytics.parts_multi", schema_p, spec)
prows = [
    {"id": i, "name": f"n{i}", "region": "eu" if i % 2 == 0 else "us",
     "day": days(2025 + (i % 2), 1 + (i % 12), 1 + (i % 28)),
     "ts": days(2025 + (i % 2), 1 + (i % 12), 1 + (i % 28)) * 86_400_000_000}
    for i in range(1, 13)
]
tp.append(pa.Table.from_pylist(prows, schema=pa.schema([
    pa.field("id", pa.int32(), nullable=False),
    pa.field("name", pa.string()),
    pa.field("region", pa.string()),
    pa.field("day", pa.date32()),
    pa.field("ts", pa.timestamp("us")),
])))
print("parts_multi:", tp.scan().to_arrow().num_rows, "rows,",
      len(tp.scan().plan_files()), "files")
print("ok")
sys.exit(0)
