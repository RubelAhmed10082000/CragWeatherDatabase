import os 
from datetime import datetime, timezone
import pandas as pd
import time

# Importing all functions from db.py
from jobs.weather_updater.app.db import (
    ensure_schema,
    fetch_crag_ids_for_shard,
    fetch_coords_for_crags,
    load_to_staging,
    delete_staging_batch,
    delete_old_staging,
    upsert_fact_window,
    log_run_start,
    log_run_finish,
)

# Importing both fetch and clean functions for weather data
from jobs.weather_updater.fetch_weather_data.openmeteo_upsert import fetch_weather_for_crags_staging

# Importing env variables
DP             = int(os.getenv("DP", "6"))
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "7"))
TOTAL_SHARDS   = int(os.getenv("TOTAL_SHARDS", "16"))
SHARD_INDEX    = int(os.getenv("CLOUD_RUN_TASK_INDEX", os.getenv("SHARD_INDEX", "0")))
CHUNK_SIZE     = int(os.getenv("CHUNK_SIZE", "150"))
MAX_POINTS     = int(os.getenv("MAX_POINTS_PER_SHARD", "0")) 
WINDOW_HOURS   = int(os.getenv("WINDOW_HOURS", "12")) 
                     
# Creating chunks 
def chunk(lst, n):
    for i in range(0, len(lst),n):
        yield lst[i:i+n]

def log_free_tier_usage(elapsed_seconds: float):
    """
    Prints whether this task's runtime fits in Cloud Run's monthly free tier.
    """
    runs_per_month = 720 
    tasks = int(os.getenv("TOTAL_SHARDS", "16"))
    mem_gib = float(os.getenv("MEMORY_GIB", "1"))  

    CPU_FREE = 240_000      
    MEM_FREE = 450_000       

    cpu_allow_per_task = CPU_FREE / (tasks * runs_per_month)              
    mem_allow_per_task = MEM_FREE / (tasks * runs_per_month * mem_gib)      

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
    ensure_schema()

    # Fetch crag_ids as well as coordinate for each crag_id
    crag_ids = fetch_crag_ids_for_shard(TOTAL_SHARDS, SHARD_INDEX)
    coords_by_id = fetch_coords_for_crags(crag_ids)
    coords = list(coords_by_id.values())

    if not coords:
        print(f"No coords in shard {SHARD_INDEX}")
        return 
    
    crag_tuples = [(cid, lat, lon) for cid, (lat,lon) in coords_by_id.items()]
    
    if MAX_POINTS > 0:
        crag_tuples = crag_tuples[:MAX_POINTS]
    
    # Creates batch id and run id for monitoring purposes
    batch_id = f"shard{SHARD_INDEX}_of_{TOTAL_SHARDS}__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    run_id = log_run_start(batch_id, DP)
    t0 = time.perf_counter()

    try:
        # Fetch weather rows into staging-ready DataFrames in chunks
        parts = []
        for group in chunk(crag_tuples, CHUNK_SIZE):
            df = fetch_weather_for_crags_staging(group, load_batch_id=batch_id, max_points=None)
            if df is not None and not df.empty:
                parts.append(df)

        if not parts:
            print("No rows fetched for this shard.")
            log_run_finish(run_id, staged=0, unmatched=0, upserted=0, status="no_data")
            return
        
        df_all = pd.concat(parts, ignore_index=True)

        staged = load_to_staging(df_all.to_dict(orient="records"), load_batch_id=batch_id)
        
        upserted, deleted = upsert_fact_window(load_batch_id=batch_id, hours=WINDOW_HOURS)

        deleted_staging = delete_staging_batch(batch_id)
        pruned = delete_old_staging(days=RETENTION_DAYS)
        RU_PER_K = float(os.getenv("RU_PER_K", "18000"))
        rows_inserted = upserted
        rows_deleted  = deleted 
        rows_updated  = 0  

        ru_estimate = int(((rows_inserted + rows_deleted + rows_updated) / 1000.0) * RU_PER_K)


        log_run_finish(run_id, staged=staged, unmatched=0, upserted=upserted, status="success",
                       rows_inserted=rows_inserted, rows_deleted=rows_deleted, rows_updated=rows_updated,
                       ru_estimate=ru_estimate, ru_per_k=RU_PER_K)
        print({
            "staged": staged,
            "upserted": upserted,
            "deleted_in_window": deleted,
            "deleted_staging": deleted_staging,
            "pruned_staging_older_days": pruned
        })

    except Exception as e:
        log_run_finish(run_id, staged=0, unmatched=0, upserted=0, status=f"failed: {e}")
        raise
    finally:
        elapsed = time.perf_counter() - t0
        log_free_tier_usage(elapsed)

       

if __name__ == "__main__":
    main()

