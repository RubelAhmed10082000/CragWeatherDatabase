# --- Keep near your other imports and helpers ---
from datetime import timezone, datetime, timedelta
import time
import os

WINDOW_SAFETY_MIN = int(os.getenv("WINDOW_SAFETY_MIN", "10"))
WINDOW_SAFETY_MIN = max(0, min(WINDOW_SAFETY_MIN, 30))
DISABLE_WRITES = os.getenv("DISABLE_WRITES", "0") == "1"

def _hour_floor(ts: datetime) -> datetime:
    return ts.replace(minute=0, second=0, microsecond=0)

def _delete_staging_chunk_in_tx(cur, batch_id: str, chunk_size: int) -> int:
    """Delete N rows for batch_id inside the current transaction, return rows deleted."""
    cur.execute(f"""
        WITH d AS (
          DELETE FROM public.stg_weather_route
          WHERE load_batch_id = %s
          LIMIT {chunk_size}
          RETURNING 1
        )
        SELECT count(*) FROM d;
    """, (batch_id,))
    return int(cur.fetchone()[0])

def upsert_from_staging(
    load_batch_id: str,
    hours: int,
    *,
    safety_min: int = WINDOW_SAFETY_MIN,
    chunk_size: int = 5000,       
    sleep_seconds: float = 0.02,  
) -> dict:
    """
    Single-source-of-truth UPSERT:
      - Window = [end - hours + 1, end], end = now() - safety_min (hour aligned)
      - effective_keys = DISTINCT (crag_id, date) for this batch within window
      - INSERT ... ON CONFLICT DO UPDATE (change-only)
      - Chunked delete of staging rows for this batch (same transaction)
    Returns: {'window_start': ts, 'window_end': ts, 'upserted': int, 'staging_deleted': int}
    """
    if DISABLE_WRITES:
        return {'window_start': None, 'window_end': None, 'upserted': 0, 'staging_deleted': 0}

    now_utc = datetime.now(timezone.utc)
    end = _hour_floor(now_utc - timedelta(minutes=max(0, safety_min)))
    start = end - timedelta(hours=max(1, hours) - 1)
    run_ts = _hour_floor(now_utc)  # forecast_run_ts basis

    effective_keys_cte = """
    WITH effective_keys AS (
      SELECT DISTINCT s.crag_id, s.date
      FROM public.stg_weather_route s
      WHERE s.load_batch_id = %(b)s
        AND s.date >= %(start_ts)s
        AND s.date <  %(end_ts)s
    )
    """

    with get_connection() as conn, conn.cursor() as cur:
        # 1) UPSERT change-only
        upsert_sql = f"""
        {effective_keys_cte}
        INSERT INTO public.fact_crag_hourly_weather (
            crag_id, date, temperature_c, relative_humidity_percentage,
            precipitation_mm, windspeed_ms, load_batch_id, forecast_run_ts, horizon_hours
        )
        SELECT
            s.crag_id, s.date, s.temperature_c, s.relative_humidity_percentage,
            s.precipitation_mm, s.windspeed_ms, %(b)s AS load_batch_id,
            %(run_ts)s AS forecast_run_ts,
            CAST(EXTRACT(EPOCH FROM (s.date - %(run_ts)s)) / 3600 AS INT) AS horizon_hours
        FROM public.stg_weather_route s
        JOIN effective_keys ek
          ON ek.crag_id = s.crag_id AND ek.date = s.date
        WHERE s.load_batch_id = %(b)s
        ON CONFLICT (crag_id, date) DO UPDATE SET
              temperature_c = EXCLUDED.temperature_c,
              relative_humidity_percentage = EXCLUDED.relative_humidity_percentage,
              precipitation_mm = EXCLUDED.precipitation_mm,
              windspeed_ms = EXCLUDED.windspeed_ms,
              forecast_run_ts = EXCLUDED.forecast_run_ts,
              horizon_hours   = EXCLUDED.horizon_hours,
              load_batch_id   = EXCLUDED.load_batch_id
        WHERE (
               fact_crag_hourly_weather.temperature_c                IS DISTINCT FROM EXCLUDED.temperature_c
            OR fact_crag_hourly_weather.relative_humidity_percentage IS DISTINCT FROM EXCLUDED.relative_humidity_percentage
            OR fact_crag_hourly_weather.precipitation_mm             IS DISTINCT FROM EXCLUDED.precipitation_mm
            OR fact_crag_hourly_weather.windspeed_ms                 IS DISTINCT FROM EXCLUDED.windspeed_ms
        )
        RETURNING 1;
        """
        params = {"b": load_batch_id, "start_ts": start, "end_ts": end + timedelta(hours=1), "run_ts": run_ts}
        cur.execute(upsert_sql, params)
        upserted = len(cur.fetchall())

        # 2) Chunked cleanup (same transaction)
        staged_deleted = 0
        while True:
            n = _delete_staging_chunk_in_tx(cur, load_batch_id, chunk_size)
            staged_deleted += n
            if n == 0:
                break
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

        conn.commit()

    return {
        'window_start': start,
        'window_end': end,
        'upserted': upserted,
        'staging_deleted': staged_deleted
    }
