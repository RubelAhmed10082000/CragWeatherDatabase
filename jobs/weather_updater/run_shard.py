import os 
from datetime import datetime, timezone
import pandas as pd
import time

# Importing all functions from db.py
from jobs.weather_updater.app.db import (
    get_connection, ensure_weather_table, ensure_staging_exists,
    fetch_crag_ids_for_shard, fetch_coords_for_crags,
    load_to_staging, count_unmatched_staging, delete_staging_batch,
    delete_old_staging, log_run_start, log_run_finish, upsert_dim_hourly,
) 

# Importing both fetch and clean functions for weather data
from jobs.weather_updater.fetch.openmeteo import fetch_weather_data_inmem

# Importing env variables
DP             = int(os.getenv("DP", "6"))
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "7"))
TOTAL_SHARDS   = int(os.getenv("TOTAL_SHARDS", "16"))
SHARD_INDEX    = int(os.getenv("CLOUD_RUN_TASK_INDEX", os.getenv("SHARD_INDEX", "0")))
CHUNK_SIZE     = int(os.getenv("CHUNK_SIZE", "150"))
MAX_POINTS     = int(os.getenv("MAX_POINTS_PER_SHARD", "0")) 

                     
# Column expectations 
EXPECTED_COLS = [
    "date", "precipitation_percentage",'temperature_c',
    "longitude", 'latitude', 'relative_humidity_percentage',
]

# Creating chunks 
def chunk(lst, n):
    for i in range(0, len(lst),n):
        yield lst[i:i+n]

def conform(df: pd.DataFrame, dp: int) -> pd.DataFrame:
    """
    Ensuring PD matches staging/upsert expectaions exactly
    """

    # Ensure required columns exists
    # Create if not existing
    for col in EXPECTED_COLS:
        if col not in df.columns:
            df[col] = pd.NA

    # Making sure date is correct type for upsert
    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")

    # Numeric coercions
    for col in ['precipitation_percentage','temperature_c','relative_humidity_percentage']:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Rounding coordinate precision to match DB
    df["latitude"] = pd.to_numeric(df['latitude'], errors="coerce").round(dp)
    df["longitude"] = pd.to_numeric(df['longitude'], errors="coerce").round(dp)

    # Dropping rows that miss critical field e.g. date and coords
    df = df.dropna(subset=["date","latitude","longitude"])
    return df

def log_free_tier_usage(elapsed_seconds: float):
    """
    Prints whether this task's runtime fits in Cloud Run's monthly free tier.
    """
    runs_per_month = 720  # hourly update * 30 days
    tasks = int(os.getenv("TOTAL_SHARDS", "16"))
    mem_gib = float(os.getenv("MEMORY_GIB", "1"))  

    # Cloud Run free tier per month:
    CPU_FREE = 240_000       # vCPU-seconds
    MEM_FREE = 450_000       # GiB-seconds

    cpu_allow_per_task = CPU_FREE / (tasks * runs_per_month)                # seconds
    mem_allow_per_task = MEM_FREE / (tasks * runs_per_month * mem_gib)      # seconds

    print({
        "task_elapsed_s": round(elapsed_seconds, 3),
        "cpu_free_allow_s_per_task": round(cpu_allow_per_task, 1),
        "mem_free_allow_s_per_task": round(mem_allow_per_task, 1),
        "under_cpu_free": elapsed_seconds <= cpu_allow_per_task,
        "under_mem_free": elapsed_seconds <= mem_allow_per_task,
        "tasks": tasks,
        "mem_gib": mem_gib
    })


# Main function which runs everything
def main():
    

    # Ensuring both staging and weather table exists
    # Otherwise creates them
    ensure_weather_table()
    ensure_staging_exists()

    # Fetch crag_ids as well as coordinate for each crag_id
    crag_ids = fetch_crag_ids_for_shard(TOTAL_SHARDS, SHARD_INDEX)
    coords_by_id = fetch_coords_for_crags(crag_ids)
    coords = list(coords_by_id.values())

    if not coords:
        print(f"No coords in shard {SHARD_INDEX}")
        return 
    
    if MAX_POINTS > 0:
        coords = coords[:MAX_POINTS]
    
    # Runs API call, clean and conform for each chunk
    parts = []
    for group in chunk(coords, CHUNK_SIZE):
        df = fetch_weather_data_inmem(group)
        if not df.empty:
            parts.append(df)
    
    if not parts:
        print("No rows after cleaning/conforming")
        return

    df = pd.concat(parts, ignore_index=True)
    
    # Creates batch id and run id for monitoring purposes
    batch_id = f"shard{SHARD_INDEX}_of_{TOTAL_SHARDS}__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    run_id = log_run_start(batch_id, DP)
    t0 = time.perf_counter()

    try:
        # Stages data into stg_weatheroutes
        # Counts rows where coordinates and crag_id is unmatched 
        staged = load_to_staging(df.to_dict(orient="records"), load_batch_id=batch_id)
        unmatched = count_unmatched_staging(dp=DP, load_batch_id=batch_id)

        # Creates connection to DB
        # Upserts data to dim_hourly
        with get_connection() as conn,conn.cursor() as cur:
            upserted = upsert_dim_hourly(cur, dp=DP, batch_id=batch_id)
            conn.commit()

        # Deletes staging values
        deleted = delete_staging_batch(batch_id)
        # Deletes any old staging values from previous upserts
        pruned = delete_old_staging(days=RETENTION_DAYS)
        
        # Logging run
        log_run_finish(run_id, staged=staged, unmatched=unmatched, upserted=upserted, status="success")
        print({"staged": staged, "unmatched": unmatched, "upserted": upserted, "deleted": deleted, "pruned": pruned})
        

    except Exception as e:
        log_run_finish(run_id, staged=0, unmatched=0, upserted=0, status=f"failed: {e}")
        raise
    finally:
        # Ensure run stays in free-tier territory
        elapsed = time.perf_counter() - t0
        log_free_tier_usage(elapsed)

if __name__ == "__main__":
    main()

