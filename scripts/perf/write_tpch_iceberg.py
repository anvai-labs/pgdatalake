"""Write TPC-H parquet (/tmp/tpch by default) as Iceberg tables into the
smoke warehouse for the perf suite. Run INSIDE the fixture docker network
with the parquet dir mounted (lakekeeper per-table s3.endpoint gotcha),
same protocol as the TPC-DS loader:

  docker run --rm -i --network pgdatalake_default \
      -v /tmp/pdi-venv:/venv -v /tmp:/hosttmp \
      -e PYTHONPATH=/venv/lib/python3.12/site-packages \
      python:3.12-slim python3 - < scripts/perf/write_tpch_iceberg.py

Env: TPCH_SRC (default /hosttmp/tpch), TPCH_NS (default tpch),
     LK_URI (default http://lakekeeper:8181/catalog).
"""
import os
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.types import (BooleanType, DateType, DecimalType, DoubleType,
                             FloatType, IntegerType, LongType, NestedField,
                             StringType, TimestampType, TimestamptzType, TimeType)
import pyarrow as pa
import pyarrow.parquet as pq

src = os.environ.get("TPCH_SRC", "/hosttmp/tpch")
ns = os.environ.get("TPCH_NS", "tpch")

catalog = load_catalog(
    "tpch", type="rest",
    uri=os.environ.get("LK_URI", "http://lakekeeper:8181/catalog"),
    warehouse="smoke",
    **{"s3.access-key-id": "lakehouse-test",
       "s3.secret-access-key": "lakehouse-test-password",
       "s3.region": "us-east-1"})

try:
    catalog.create_namespace(ns)
except Exception:
    pass

def arrow_to_iceberg(f):
    t = f.type
    if pa.types.is_boolean(t): return BooleanType()
    if pa.types.is_int32(t): return IntegerType()
    if pa.types.is_int64(t): return LongType()
    if pa.types.is_float32(t): return FloatType()
    if pa.types.is_float64(t): return DoubleType()
    if pa.types.is_string(t) or pa.types.is_large_string(t): return StringType()
    if pa.types.is_date(t): return DateType()
    if pa.types.is_time(t): return TimeType()
    if pa.types.is_decimal(t): return DecimalType(t.precision, t.scale)
    if pa.types.is_timestamp(t):
        return TimestamptzType() if t.tz else TimestampType()
    raise SystemExit(f"unsupported arrow type for {f.name}: {t}")

from pyiceberg.schema import Schema

for t in sorted(os.listdir(src)):
    d = os.path.join(src, t)
    files = sorted(os.path.join(d, x) for x in os.listdir(d))
    name = f"{ns}.{t}"
    try:
        catalog.drop_table(name)
    except Exception:
        pass
    fields = [NestedField(i, f.name, arrow_to_iceberg(f), required=not f.nullable)
              for i, f in enumerate(pq.read_schema(files[0]), start=1)]
    tb = catalog.create_table(
        name, schema=Schema(*fields), partition_spec=PartitionSpec(),
        properties={"write.target-file-size-bytes": "134217728"})
    n = 0
    for f in files:
        tbl_arrow = pq.read_table(f)
        tb.append(tbl_arrow)
        n += tbl_arrow.num_rows
        del tbl_arrow
    print(f"{name}: {n} rows", flush=True)
print("done")
