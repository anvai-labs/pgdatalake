"""Generate TPC-DS SF=10 tables to /tmp/tpcds/<table>/*.parquet (duckdb)."""
import duckdb, os, sys
con = duckdb.connect()
con.execute("SET threads TO 16")
con.execute("INSTALL tpcds; LOAD tpcds;")
con.execute("CALL dsdgen(sf = 10)")
tables = [
    "call_center","catalog_page","catalog_returns","catalog_sales","customer",
    "customer_address","customer_demographics","date_dim","household_demographics",
    "income_band","item","promotion","reason","store","store_returns",
    "store_sales","time_dim","warehouse","web_page","web_returns","web_sales",
    "web_site",
]
out = "/tmp/tpcds"
os.makedirs(out, exist_ok=True)
for t in tables:
    d = os.path.join(out, t)
    os.makedirs(d, exist_ok=True)
    n = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
    con.execute(f"COPY (SELECT * FROM {t}) TO '{d}' (FORMAT PARQUET, FILE_SIZE_BYTES 134217728, ROW_GROUP_SIZE 262144)")
    print(f"{t}: {n} rows -> {len(os.listdir(d))} file(s)", flush=True)
print("done")
