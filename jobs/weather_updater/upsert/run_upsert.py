import os 
from datetime import datetime, timezone
import time
import itertools

print("CERT_DIR_CONTENTS", os.listdir("/certs") if os.path.exists("/certs") else "NO_CERTS")

from jobs.weather_updater.app.db import (
    ensure_schema,
    fetch_crag_ids_for_shard,
    fetch_coords_for_crags,
    load_to_staging,
    upsert_from_staging,
    log_run_start,
    log_run_finish
)

from jobs.weather_updater.geo.quantize import quantized_fetch_to_df


# Importing both fetch and clean functions for weather data
from jobs.weather_updater.fetch_weather_data.openmeteo_upsert import fetch_weather_for_crags_staging

# Importing env variables
DP             = int(os.getenv("DP", "6"))
TOTAL_SHARDS   = int(os.getenv("TOTAL_SHARDS", "16"))
SHARD_INDEX    = int(os.getenv("CLOUD_RUN_TASK_INDEX", os.getenv("SHARD_INDEX", "0")))
CHUNK_SIZE     = int(os.getenv("CHUNK_SIZE", "150"))
MAX_POINTS     = int(os.getenv("MAX_POINTS_PER_SHARD", "0")) 
WINDOW_HOURS   = int(os.getenv("WINDOW_HOURS", "12")) 
MAX_ROWS_PER_RUN = int(os.getenv("MAX_ROWS_PER_RUN", "0") or 0) 
WINDOW_SAFETY_MIN   = int(os.getenv("WINDOW_SAFETY_MIN", "10"))  
DEL_CHUNK_SIZE      = int(os.getenv("DEL_CHUNK_SIZE", "5000"))
DEL_CHUNK_SLEEP_S   = float(os.getenv("DEL_CHUNK_SLEEP_S", "0.02"))

                     
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

import os, hashlib, sys

print("=== DEBUG START ===", file=sys.stderr)
dburl = os.environ.get("DATABASE_URL", "")
print("DBURL_SHA256", hashlib.sha256(dburl.encode()).hexdigest(), file=sys.stderr)

for k in ("PGPASSWORD","PGUSER","PGHOST","PGPORT","PGDATABASE","DATABASE_URL"):
    if k in os.environ:
        val = os.environ[k]
        shown = val if k == "DATABASE_URL" else val[:4] + "…"
        print(f"WARN_PGVAR {k}={shown}", file=sys.stderr)

print("=== DEBUG END ===", file=sys.stderr)

# Main function which runs everything
def main():
    

    # Ensuring both staging and weather table exists
    # Otherwise creates them
    try:
        ensure_schema()
    except Exception as e:
        print(f"Schema failed {e}")

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
    batch_id = os.getenv(
    "BATCH_ID",
    f"shard{SHARD_INDEX}_of_{TOTAL_SHARDS}__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
)
    run_id = log_run_start(batch_id, DP)
    t0 = time.perf_counter()

    try:
        # Fetch weather rows into staging-ready DataFrames in chunks
        cell_deg = float(os.getenv("CELL_DEG", "0.25"))               # ~28km cells
        max_cells = int(os.getenv("MAX_CELLS_PER_SHARD", "0"))        # 0 = all cells

        df_all, cells_hit, crags_covered = quantized_fetch_to_df(
            coords_by_id=coords_by_id,
            batch_id=batch_id,
            fetch_fn=fetch_weather_for_crags_staging,  # existing helper
            chunk_size=CHUNK_SIZE,
            cell_deg=cell_deg,
            max_cells=max_cells,
        )

        if df_all.empty:
            print(f"No rows fetched (cells={cells_hit}, crags_covered={crags_covered}).")
            log_run_finish(run_id, staged=0, unmatched=0, upserted=0, status="no_data")
            return
        
        original_count = len(df_all)
        cap_hit = False
        if MAX_ROWS_PER_RUN > 0 and original_count > MAX_ROWS_PER_RUN:
            df_all = df_all.head(MAX_ROWS_PER_RUN).copy()
            cap_hit = True

        staged = load_to_staging(df_all.to_dict(orient="records"), load_batch_id=batch_id)
        
        res = upsert_from_staging(
            load_batch_id=batch_id,
            hours=WINDOW_HOURS,
            safety_min=WINDOW_SAFETY_MIN,
            chunk_size=DEL_CHUNK_SIZE,
            sleep_seconds=DEL_CHUNK_SLEEP_S,
        )
        upserted = res.get("upserted", 0)
        deleted_in_staging= res.get("staging_deleted", 0)
        RU_PER_K = float(os.getenv("RU_PER_K", "18000"))
        ru_estimate = int(((upserted) / 1000.0) * RU_PER_K)

        log_run_finish(
            run_id,
           staged=staged,
           unmatched=0,
            upserted=upserted,
            status="success",
            rows_inserted=upserted,
            rows_deleted=0,              
            rows_updated=0,             
            ru_estimate=ru_estimate,
            ru_per_k=RU_PER_K,
        )
        
        print({
             "staged": staged,
             "upserted": upserted,
             "deleted_in_window": 0,
             "deleted_staging": deleted_in_staging,
             "hard_cap_rows": MAX_ROWS_PER_RUN,
             "hard_cap_hit": cap_hit
         })
        
    except Exception as e:
       
        try:
                log_run_finish(run_id, staged=0, unmatched=0, upserted=0, status="failed", notes=str(e))
        except Exception:
                pass
        raise

    finally:
        elapsed = time.perf_counter() - t0
        try:
            log_free_tier_usage(elapsed)
        except Exception:
            pass


if __name__ == "__main__":
    main()


