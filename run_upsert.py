import os, sys, psycopg, pathlib
from psycopg.rows import dict_row

dsn=os.environ["DB_DSN"]
sql=pathlib.Path("upsert.sql").read_text(encoding="utf-8")
params={"b":os.environ["BATCH_ID"],"ws":os.environ["WINDOW_START"],"we":os.environ["WINDOW_END"]}

with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor(row_factory=dict_row) as cur:
    cur.execute(sql, params)
    print({"affected": cur.rowcount or 0})
