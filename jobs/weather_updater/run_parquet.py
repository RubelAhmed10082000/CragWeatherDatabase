import sys, os
from datetime import datetime, timezone
import argparse
import pandas as pd
from jobs.weather_updater.app import db
from jobs.weather_updater.app.db import get_connection
from jobs.weather_updater.app.db import (
    ensure_weather_table,
    ensure_staging_exists,  
    load_to_staging,          
    merge_batch,              
    delete_staging_batch,     
    delete_old_staging,       
    count_unmatched_staging,  
    log_run_start,
    log_run_finish,
)




EXPECTED_COLS = [
    "date",
    "precipitation_percentage",
    "temperature_c",
    "longitude",
    "latitude",
    "relative_humidity_percentage",
]

def parse_args():
    p = argparse.ArgumentParser(description="Load weather parquet -> staging -> merge (batch-scoped).")
    p.add_argument("parquet_path", help="Local path or gs:// URI to a parquet file")
    p.add_argument("--dp", type=int, default=6, help="Rounding precision for lat/lon join (default: 6)")
    p.add_argument("--retention-days", type=int, default=7, help="Delete staging rows older than N days (default: 7)")
    return p.parse_args()


def main():
    args = parse_args()

    ensure_weather_table()
    ensure_staging_exists()

    df = pd.read_parquet(args.parquet_path)
    if df["date"].dtype == "object":
        df["date"] = pd.to_datetime(df["date"], utc=True)

    missing = [c for c in EXPECTED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Parquet missing columns: {missing}")

    rows = df[EXPECTED_COLS].to_dict(orient="records")

    batch_id = f"{os.path.basename(args.parquet_path)}__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    print(f"Batch ID: {batch_id}")

    run_id = log_run_start(batch_id, args.dp)

    try:
        staged = load_to_staging(rows, load_batch_id=batch_id)
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM public.stg_weather_route")
            print("Total rows now in staging:", cur.fetchone()["n"])

            cur.execute("""
            SELECT COUNT(*) AS n
            FROM public.stg_weather_route
            WHERE load_ts > now() - interval '5 minutes'
            """)
            print("Rows inserted in last 5 minutes (any batch):", cur.fetchone()["n"])

            cur.execute("""
            SELECT load_batch_id, COUNT(*) AS n
            FROM public.stg_weather_route
            GROUP BY 1 ORDER BY n DESC LIMIT 5
            """)
            print("Top batch_ids present:", cur.fetchall())

        unmatched = count_unmatched_staging(dp=args.dp, load_batch_id=batch_id)
        print("Unmatched rows (this batch):", unmatched)

        with get_connection() as conn, conn.cursor() as cur:
            upserted = db.upsert_dim_hourly(cur, dp=args.dp, batch_id=batch_id)
            conn.commit()
        print(f"Upserted rows: {upserted}")

        deleted = delete_staging_batch(batch_id)
        print(f"Deleted staging rows for this batch: {deleted}")

        pruned = delete_old_staging(days=args.retention_days)
        print(f"Retention cleanup (> {args.retention_days} days): {pruned} rows removed")

        log_run_finish(run_id, staged=staged, unmatched=unmatched, upserted=upserted, status="success")

    except Exception as e:
        log_run_finish(run_id, staged=0, unmatched=0, upserted=0, status=f"failed: {e}")
        raise

if __name__ == "__main__":
    main()