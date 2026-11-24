"""Generate TPC-H SF=1 tables to /tmp/tpch/<table>/*.parquet (duckdb)."""
import duckdb, os, sys
sf = sys.argv[1] if len(sys.argv) > 1 else "1"
con = duckdb.connect()
con.execute("SET threads TO 16")
con.execute("INSTALL tpch; LOAD tpch;")
con.execute(f"CALL dbgen(sf = {sf})")
tables = ["nation", "region", "part", "supplier", "partsupp",
          "customer", "orders", "lineitem"]
out = "/tmp/tpch"
os.makedirs(out, exist_ok=True)
for t in tables:
    d = os.path.join(out, t)
    os.makedirs(d, exist_ok=True)
    for x in os.listdir(d):
        os.remove(os.path.join(d, x))
    n = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
    con.execute(f"COPY (SELECT * FROM {t}) TO '{d}' (FORMAT PARQUET, FILE_SIZE_BYTES 134217728, ROW_GROUP_SIZE 262144)")
    print(f"{t}: {n} rows -> {len(os.listdir(d))} file(s)", flush=True)
print("done")
