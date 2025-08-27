import os 
from datetime import datetime, timezone
import pandas as pd

# Importing all functions from db.py
from jobs.weather_updater.app.db import (
    get_connection, ensure_weather_table, ensure_staging_exists,
    fetch_crag_ids_for_shard, fetch_coords_for_crags,
    load_to_staging, count_unmatched_staging, delete_staging_batch,
    delete_old_staging, log_run_start, log_run_finish, upsert_dim_hourly,
)

# Importing both fetch and clean functions for weather data
# from modules packages
from modules.fetch_weather_data import fetch_weather_data
from modules.clean_weather_data import clean_weather_data

# Importing GCS IO helpers, read/write crag shards

from modules.gcs_io import gcs_url, read_parquet, write_parquet

DP              = int(os.getenv("DP", "6"))
RETENTION_DAYS  = int(os.getenv("RETENTION_DAYS", "7"))
TOTAL_SHARDS    = int(os.getenv("TOTAL_SHARDS", "16"))
SHARD_INDEX     = int(os.getenv("CLOUD_RUN_TASK_INDEX", os.getenv("SHARD_INDEX", "0")))
CHUNK_SIZE      = int(os.getenv("CHUNK_SIZE", "150"))

CRAG_SRC = os.getenv("CRAG_SRC", "cleaned/crag/crag_df.parquet")
WEATHER_DST_PREFIX = os.getenv("WEATHER_DST_PREFIX", "processed/weather")
CLEAN_DST_PREFIX    = os.getenv("CLEAN_DST_PREFIX",   "cleaned/weather")
MAX_POINTS_OVERRIDE = os.getenv("MAX_POINTS_PER_SHARD")

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
    df["latitude"] = pd.to_numeric(df['longitude'], errors="coerce").round(dp)

    # Dropping rows that miss critical field e.g. date and coords
    df = df.dropna(subset=["date","latitude","longitude"])

def make_paths():
    ts = datetime.new(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    shard_tag = f"shard{SHARD_INDEX}_of_{TOTAL_SHARDS}"

    # Per-shard crag list 
    crag_src_shard_blob = f"tmp/shards/{shard_tag}/{ts}/crag_df.parquet"
    
    # Writes fetched weather data to GCS blob
    weather_dst_blob = f"{WEATHER_DST_PREFIX}/{shard_tag}/{ts}.parquet"

    # Cleaned data writes here
    clean_dst_blob = f"{CLEAN_DST_PREFIX}/{shard_tag}/{ts}.parquet"
    return shard_tag, ts, crag_src_shard_blob, weather_dst_blob, clean_dst_blob

def build_shard_crag_parquet(global_crag_blob: str, 
                             shard_crag_blob: str, crag_ids_for_shard) -> int:
    
    """
    Read full crag_df.parquet from GCS 
    """
    crag_df = read_parquet(gcs_url(*global_crag_blob.split("/")))
    if crag_df is None or crag_df.empty:
        return 0
    
    id_col = "crag_id"
    assert id_col in crag_df.columns, f"{id_col} missing in {global_crag_blob}"

    shard_df = crag_df[crag_df[id_col].isin(crag_ids_for_shard)].copy()
    if shard_df.empty:
        return 0
    
    # Writes shard to GCS
    write_parquet(shard_df, gcs_url(*shard_crag_blob.split('/')))
    return len(shard_df)


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
    
    # Runs API call, clean and conform for each chunk
    parts = []
    for group in chunk(coords, CHUNK_SIZE):
        raw = fetch_weather_data(group)
        cln = clean_weather_data(raw)
        fin = conform(cln, DP)
        if not fin.empty:
            parts.append(fin)
    
    if not parts:
        print("No rows after cleaning.")

    # Concatanates each chunk to a dataframe
    df = pd.concat(parts, ignore_index=True)
    
    # Creates batch id and run id for monitoring purposes
    batch_id = f"shard{SHARD_INDEX}_of_{TOTAL_SHARDS}__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    run_id = log_run_start(batch_id, DP)

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

if __name__ == "__main__":
    main()

