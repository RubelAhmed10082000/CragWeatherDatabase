import argparse
import asyncio
import math
import os
from datetime import datetime, timezone, timedelta
from typing import Iterable, Dict, Tuple, List, Any
import httpx

# Importing DB helpers
from jobs.weather_updater.app.db import (
    ensure_schema,
    fetch_crag_ids_for_shard,
    fetch_coords_for_crags,
    load_to_staging,
    upsert_fact_window,
    delete_staging_batch,
    log_run_start,
    log_run_finish,
)

from jobs.weather_updater.fetch_weather_data.openmeteo_seed import (
    build_url,
    rows_from_response,
)


def floor_hour_utc(dt: datetime) -> datetime:
    """
    Rounds datetime objects (e.g. date column) to nearest hour
    """
    return dt.replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
 
async def fetch_one(client: httpx.AsyncClient, crag_id: str, lat: float,
                     lon: float, t0:datetime, hours:int) -> List[Dict[str, Any]]:
    """
    Aysnchronously fetches weather for one crag location
    Filters data, using neareast our as time floor
    """
    url = build_url(lat,lon,t0, hours)
    r = await client.get(url, timeout=30.0)
    r.raise_for_status()
    return list(rows_from_response(crag_id, lat, lon, t0, hours, r.json()))

async def fetch_shard_rows(crag_ids: Iterable[str], coords: Dict[str, Tuple[float,float]],
                            t0: datetime, hours:int, concurrency: int) -> List[Dict[str,Any]]:
    sem = asyncio.Semaphore(concurrency)
    rows: List[Dict[str, Any]] = []

    async with httpx.AsyncClient(http2=True) as client:
        async def run_one(cid: str):
            lat, lon = coords[cid]
            async with sem:
                try:
                    rs = await fetch_one(client, cid, lat, lon, t0, hours)
                    return rs
                except Exception:
                    return []
    tasks = [asyncio.create_task(run_one(cid)) for cid in crag_ids if cid in coords]
    for coro in asyncio.as_completed(tasks):
        rows.extend(await coro)
        return rows

def chunked(iterable, n):
    """
    Breaks down down iterables in chunks of size n.
    Will be used for chunking upserts for our seed run.
    """
    buf = []
    for x in iterable:
        buf.append(x)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf

def main():
    ap = argparse.ArgumentParser(description="Seed initial 7-day hourly weather for a shard of crags.")
    ap.add_argument("--hours", type=int, default=168, help="Hours ahead to seed (default 168 = 7 days)")
    ap.add_argument("--total-shards", type=int, default=8, help="Total number of shards")
    ap.add_argument("--shard-index", type=int, default=0, help="This process shard index [0..total_shards-1]")
    ap.add_argument("--concurrency", type=int, default=16, help="Concurrent API calls")
    ap.add_argument("--stage-batch-size", type=int, default=5000, help="Insert batch size for staging")
    ap.add_argument("--dry-run", action="store_true", help="Fetch & count but do not write to DB")
    args = ap.parse_args()

    ensure_schema()

    # Floors datetime to the nearest hour and becomes start time for the batch
    t0 = floor_hour_utc(datetime.now(timezone.utc))
    # Builds a string that contains the seed, the forecast time and the shard index
    load_batch_id = f"seed_{t0.strftime('%Y%m%d%H')}_sh{args.shard_index}"

    crag_ids = fetch_crag_ids_for_shard(args.total_shards, args.shard_index)
    coords = fetch_coords_for_crags(crag_ids)
     
    if not coords:
        print("No coordinates found for this shard; nothing to seed.")
        return 
    
    print(f"Shard {args.shard_index}/{args.total_shards}: {len(coords)} crags with coords")

    rows = asyncio.run(fetch_shard_rows(crag_ids, coords, t0, args.hours, args.concurrency))
    print(f"Fetched {len(rows)} hourly rows for staging")

    if args.dry_run:
        return
    
    run_id = log_run_start(load_batch_id, dp=0)

    staged = 0
    for part in chunked(rows, args.stage_batch_size):
        staged += load_to_staging(part, load_batch_id, batch_size=args.stage_batch_size)
    print(f"Staged rows: {staged}")

     # Upsert into fact on the same window
    upserted, deleted = upsert_fact_window(load_batch_id, hours=args.hours)
    print(f"Upserted={upserted}, DeletedInWindow={deleted}")

    # Clean staging for this batch
    deleted_stg = delete_staging_batch(load_batch_id)
    print(f"Deleted {deleted_stg} rows from staging for batch {load_batch_id}")

    log_run_finish(run_id, staged=staged, unmatched=0, upserted=upserted, status="ok")

if __name__ == "__main__":
    main()
