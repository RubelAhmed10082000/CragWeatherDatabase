from __future__ import annotations
import os
from contextlib import contextmanager
import psycopg
from psycopg.rows import dict_row
from typing import Iterable, Mapping, Any
from datetime import timezone, datetime,timedelta
import os
import time
from contextlib import contextmanager
from typing import Optional


WINDOW_SAFETY_MIN = int(os.getenv("WINDOW_SAFETY_MIN", "10"))
WINDOW_SAFETY_MIN = max(0, min(WINDOW_SAFETY_MIN, 30))
DISABLE_WRITES = os.getenv("DISABLE_WRITES", "0") == "1"


ROCK_TYPES = ['Gritstone',
            'Limestone',
            'Sandstone (hard)',
            'Granite',
            'Grit (quarried)',
            'Sandstone (soft)',
            'Rhyolite',
            'UNKNOWN',
            'Artificial',
            'Culm',
            'Slate',
            'Greenstone',
            'Volcanic tuff',
            'Dolerite',
            'Andesite',
            'Gabbro',
            'Killas slate',
            'Mica schist',
            'Shale',
            'Pillow lava',
            'Conglomerate',
            'Chalk',
            'Schist',
            'Amphibiolite & S',
            'Welded Tuff',
            'Quartzite',
            'Crumbly rubbish',
            'Hornstone',
            'Basalt',
            'Diorites',
            'Welsh igneous',
            'Ice',
            'Serpentine',
            'Iron Rock',
            'Ignimbrite',
            'Microgranite',
            'Psammite',
            'Other']

CLIMBING_STYLES =['Bouldering',
            'Trad',
            'Sport',
            'Top Rope',
            'Winter',
            'DWS',
            'Scrambling',
            'Mixed',
            'Boulder Circuit',
            'Aid',
            'Ice',
            'Alpine',
            'Via Ferrata',
            'Other']

UNKNOWNS = {"UNKNOWN", "UNK", "N/A", "NA", ""}

ROCK_TYPES_ALLOWED = [v for v in ROCK_TYPES if v.strip().upper() not in UNKNOWNS]
STYLES_ALLOWED      = [v for v in CLIMBING_STYLES if v.strip().upper() not in UNKNOWNS]

SCHEMA_VERSION_ID = "2025-08-28_optA_text_checks_v3"

def _ensure_ssl_params(dsn: str) -> str:
    if 'sslmode=' not in dsn:
        dsn += ('&' if '?' in dsn else '?') + 'sslmode=verify-full'
    if 'sslrootcert=' not in dsn:
        dsn += '&sslrootcert=/certs/root.crt'
    return dsn

def _one_int(row, key: str | None = None) -> int:
    """Return an int from a single-row/single-col fetchone() for tuple or dict rows."""
    if row is None:
        return 0
    if key and isinstance(row, dict) and key in row:
        return int(row[key])
    return int(next(iter(row.values())) if isinstance(row, dict) else row[0])


@contextmanager
def get_connection():
    """
    Creates connection to Cockroach DB
    """
    dsn = _ensure_ssl_params(os.environ['DATABASE_URL'])
    conn = psycopg.connect(dsn, autocommit=False, row_factory=dict_row)
    try:
        yield conn
    finally:
        conn.close()

def run_sql(conn, sql:str, params:dict | None = None):
  """
  Executes SQL commands
  """

  with conn.cursor() as cur:
      cur.execute(sql, params or {})


def ensure_schema():
    """
    Updates CragCast schema if exists
    Creates if it doesn't exist
    """
    # Crating connetion to DB
    with get_connection() as conn:
          _ensure_primitives(conn)
          _ensure_tables(conn)
          _ensure_indexes(conn)
          _record_version(conn, SCHEMA_VERSION_ID)
          conn.commit()
          
          if os.getenv("SKIP_VIEWS", "1") != "1":
              try:
                with get_connection() as vconn:
                  _ensure_views(vconn)   
                  vconn.commit()
              except Exception as e:
                print(f"WARN: skipping views (non-blocking): {e}")


def _sql_in_list(values: list[str]) -> str:
    return ", ".join("'" + v.replace("'", "''") + "'" for v in values)

def _ensure_primitives(conn):
    """
    Creates a schema version table.
    Acts as a changelog and tracks the schema version we are using.
    Allows for easier migration and changing of schema
    """
    try:
        run_sql(conn, "CREATE EXTENSION IF NOT EXISTS pgcrypto;")
    except Exception:
        pass
    
    run_sql(conn, """
    CREATE TABLE IF NOT EXISTS public.schema_version (
            version_id TEXT PRIMARY KEY,
            applied_at TIMESTAMPTZ DEFAULT now()
            );
""")
    


def _ensure_tables(conn):
    """
    Ensures all the tables in schema exist, alongisde PK, FK and indexes
    Creates them if they don't exist
    """

    rock_in  = _sql_in_list(ROCK_TYPES_ALLOWED)
    style_in = _sql_in_list(STYLES_ALLOWED)
    
    run_sql(conn, f"""
    CREATE TABLE IF NOT EXISTS public.dimcrags (
            crag_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            crag_name TEXT NOT NULL,
            county TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            rocktype TEXT,
            climbing_style TEXT,
            CONSTRAINT rocktype_chk
              CHECK(rocktype IS NULL OR rocktype IN ({rock_in})),
            CONSTRAINT climbing_style_chk
              CHECK (climbing_style IS NULL OR climbing_style IN ({style_in}))
            );
          """ )
    
    # Dimroutes
    run_sql(conn, """
    CREATE TABLE IF NOT EXISTS public.dimroutes(
    route_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    crag_id UUID NOT NULL,
    route_name TEXT NOT NULL,
    grade TEXT,
    safety_grade TEXT,
    CONSTRAINT dimroutes_crag_fk FOREIGN KEY (crag_id) REFERENCES public.dimcrags (crag_id)
    );
""")
    
    disable_fk = os.getenv("DISABLE_FACT_FK", "1") == "1"
    fk_clause = "" if disable_fk else \
    ", CONSTRAINT fact_crag_hourly_weather_crag_fk " \
    "FOREIGN KEY (crag_id) REFERENCES public.dimcrags (crag_id)"

    # Fact table at crag x hour grain
    run_sql(conn, f"""
    CREATE TABLE IF NOT EXISTS public.fact_crag_hourly_weather (
        crag_id UUID NOT NULL,
        date TIMESTAMPTZ NOT NULL,
        temperature_c REAL,
        relative_humidity_percentage REAL,
        precipitation_mm NUMERIC(6,2),
        windspeed_ms REAL,
        load_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
        last_updated_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
        load_batch_id TEXT NOT NULL,
        forecast_run_ts TIMESTAMPTZ,
        horizon_hours INT,
        CONSTRAINT fact_crag_hourly_weather_pk PRIMARY KEY (crag_id, date)
        {fk_clause}
      );
      """)
    
    if not disable_fk:
      run_sql(conn,"""
        ALTER TABLE public.fact_crag_hourly_weather
        ADD CONSTRAINT IF NOT EXISTS fact_crag_hourly_weather_crag_fk
        FOREIGN KEY (crag_id) REFERENCES public.dimcrags (crag_id)
      """)
    
    # Staging for weather data
    run_sql(conn, """
    CREATE TABLE IF NOT EXISTS public.stg_weather_route(
            date TIMESTAMPTZ NOT NULL,
            precipitation_mm NUMERIC(6,2),
            temperature_c REAL,
            relative_humidity_percentage REAL,
            windspeed_ms REAL,
            crag_id UUID NOT NULL,
            longitude DOUBLE PRECISION NOT NULL,
            latitude DOUBLE PRECISION NOT NULL,
            load_batch_id TEXT NOT NULL,
            staged_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT stg_weather_unique_per_batch UNIQUE (crag_id, date, load_batch_id)
            );
            """)
    
    # Run logs for Cloud Run jobs
    run_sql(conn, """
      CREATE TABLE IF NOT EXISTS public.crag_runs_logs (
          run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          job_name TEXT NOT NULL,
          status TEXT NOT NULL,  
          notes TEXT,
          started_at TIMESTAMPTZ DEFAULT now(),
          finished_at TIMESTAMPTZ
      );
      """)
    
    run_sql(conn, """
    ALTER TABLE public.crag_runs_logs
    ADD COLUMN IF NOT EXISTS rows_inserted INT,
    ADD COLUMN IF NOT EXISTS rows_updated  INT,
    ADD COLUMN IF NOT EXISTS rows_deleted  INT,
    ADD COLUMN IF NOT EXISTS ru_estimate   BIGINT,
    ADD COLUMN IF NOT EXISTS ru_per_k      NUMERIC(12,2);
""")
    
    run_sql(conn, """
    CREATE TABLE IF NOT EXISTS public.crag_last_rain_state (
      crag_id UUID PRIMARY KEY,
      last_rained_ts TIMESTAMPTZ,
      last_rain_severity TEXT,
      updated_at TIMESTAMPTZ DEFAULT now()
      );    
      """)

    
def _ensure_indexes(conn):
    """ 
    Ensures indexes exists, if not it creates tem
    """
    run_sql(conn, "CREATE INDEX IF NOT EXISTS fact_weather_date_idx ON public.fact_crag_hourly_weather (date);")
    run_sql(conn, "CREATE INDEX IF NOT EXISTS idx_stg_load_batch_id ON public.stg_weather_route (load_batch_id);")


def _record_version(conn, version_id:str):
    run_sql(conn, """
        INSERT INTO public.schema_version (version_id)
        VALUES (%(v)s)
        ON CONFLICT (version_id) DO NOTHING;
    """, {"v": version_id})

def _ensure_views(conn):
    """
    Ensure views exists otherwise creates them
    """
    run_sql(conn, """
      CREATE OR REPLACE VIEW public.v_ru_usage_daily AS
      SELECT
        date_trunc('day', started_at) AS day,
        sum(ru_estimate)              AS ru_estimated,
        count(*)                      AS runs
      FROM public.crag_runs_logs
      WHERE ru_estimate IS NOT NULL
      GROUP BY 1
      ORDER BY 1 DESC;
      """)
    
    run_sql(conn, """
    CREATE OR REPLACE VIEW public.v_ru_usage_monthly AS
    SELECT
      date_trunc('month', started_at) AS month,
      sum(ru_estimate)                AS ru_estimated,
      count(*)                        AS runs
    FROM public.crag_runs_logs
    WHERE ru_estimate IS NOT NULL
    GROUP BY 1
    ORDER BY 1 DESC;
    """)
  

    run_sql(conn, """ 
    CREATE OR REPLACE VIEW public.v_routes_with_crag AS
    SELECT 
      r.route_id,
      r.route_name,
      r.grade,
      r.safety_grade,
      crag_id,
      c.crag_name,
      c.county,
      c.latitude,
      c.longitude,
      c.rocktype,
      c.climbing_style
    FROM public.dimroutes AS r 
    JOIN public.dimcrags c USING (crag_id);
    """)

    run_sql(conn, "DROP VIEW IF EXISTS public.v_crag_hourly_weather")
    
    run_sql(conn, """
    CREATE OR REPLACE VIEW public.v_crag_hourly_weather AS
    SELECT 
      crag_id,
      c.crag_name,
      c.county,
      c.latitude,
      c.longitude,
      c.rocktype,
      c.climbing_style,
      f.date,
      f.temperature_c,
      f.relative_humidity_percentage,
      f.precipitation_mm,
      f.windspeed_ms,
      f.load_batch_id,
      f.load_ts,
      f.forecast_run_ts,
      f.horizon_hours,
      s.last_rained_ts,
      s.last_rain_severity,
       CASE 
        WHEN s.last_rained_ts IS NULL THEN NULL
        ELSE GREATEST(
           0,
           CAST(EXTRACT(EPOCH FROM (now() - s.last_rained_ts)) / 3600 AS INT)
         )
        END AS hours_since_rain
    FROM public.fact_crag_hourly_weather f
    JOIN public.dimcrags c USING (crag_id)
    LEFT JOIN public.crag_last_rain_state AS s USING (crag_id);
""")
    

def fetch_crag_ids_for_shard(total_shards: int,  shard_index: int) -> list[str]:
    """
    Derives shard indexed from crag_ids in public.dimcrags.

    When upserting crags will be split into shards and weather data
    with be upserted for each of the 16 shards concurrently.


    """
    sql = """
      SELECT crag_id::text 
      FROM public.dimcrags
      WHERE mod(abs(fnv64(crag_id::string)), %(total)s) = %(idx)s
"""

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, {"total": total_shards, "idx":shard_index})
        return [r["crag_id"] for r in cur.fetchall()]
    
def fetch_coords_for_crags(crag_ids: Iterable[str]) -> dict[str,tuple[float, float]]:
    """
    Fetching list of longitude and latitude coordinates from dimcrags
    """
    ids = list(crag_ids)
    if not ids:
        return {}
    sql = """
      SELECT crag_id::text AS crag_id, latitude, longitude
      FROM public.dimcrags
      WHERE crag_id = ANY(%(ids)s::uuid[]) AND latitude IS NOT NULL and longitude IS NOT NULL
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, {"ids":ids})
        rows = cur.fetchall()
        return {r["crag_id"]:(float(r["latitude"]), float(r["longitude"])) for r in rows}
    

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
        SELECT count(*) AS n FROM d;
    """, (batch_id,))
    return _one_int(cur.fetchone(), "n")
        
def load_to_staging(rows: Iterable[Mapping[str, Any]], load_batch_id: str, batch_size: int = 5000) -> int:
    """
    Batch insert into stg_weather_route with optional retry policy.
    Staging columns: date, precipitation_mm, temperature_c, relative_humidity_percentage,
                     windspeed_ms, crag_id, longitude, latitude, load_batch_id
    """
    if DISABLE_WRITES:
        return 0
    
    rows = list(rows)
    if not rows:
        return 0

    cols = [
        "date", "precipitation_mm", "temperature_c", "relative_humidity_percentage",
        "windspeed_ms", "crag_id", "longitude", "latitude", "load_batch_id"
    ]

    conflict_clause = (
        """
        ON CONFLICT (crag_id, date, load_batch_id) DO NOTHING
        """

    )

    inserted = 0
    row_placeholders = "(" + ",".join(["%s"] * len(cols)) + ")"

    with get_connection() as conn, conn.cursor() as cur:

        for i in range(0, len(rows), batch_size):
            chunk = rows[i:i + batch_size]
            vals_flat: list[Any] = []
            for r in chunk:
                vals_flat.extend([
                    r["date"],
                    r.get("precipitation_mm"),
                    r.get("temperature_c"),
                    r.get("relative_humidity_percentage"),
                    r.get("windspeed_ms"),
                    r["crag_id"],
                    r["longitude"],
                    r["latitude"],
                    load_batch_id,
                ])
            placeholders = ",".join([row_placeholders] * len(chunk))
            cur.execute(
                f"INSERT INTO public.stg_weather_route ({', '.join(cols)}) "
                f"VALUES {placeholders} {conflict_clause}",
                vals_flat,
            )
            if cur.rowcount is not None and cur.rowcount >= 0:
                inserted += cur.rowcount

        conn.commit()

    return inserted  


def delete_by_batch_loop(batch_id: str,
                         schema: str = "public", table: str = "stg_weather_route",
                         chunk_size: int = 5000, sleep_seconds: float = 0.0) -> int:
    chunk_sql = f"""
    WITH d AS (
      DELETE FROM {schema}.{table}
      WHERE load_batch_id = %s
      LIMIT {chunk_size}
      RETURNING 1
    )
    SELECT count(*) AS n FROM d;
    """
    total = 0
    while True:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(chunk_sql, (batch_id,))
            deleted = _one_int(cur.fetchone(), "n")
            conn.commit()
        total += deleted
        if deleted == 0:
            break
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
    return total

def count_staged_rows(batch_id: str, schema: str = "public", table: str = "stg_weather_route") -> int:
    sql = f"SELECT count(*) AS n FROM {schema}.{table} WHERE load_batch_id = %s;"
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (batch_id,))
        return _one_int(cur.fetchone(), "n")

def log_cleanup(batch_id: str, window_label: str, rows_deleted: int,
                ru_observed: Optional[float] = None, ru_per_row_obs: Optional[float] = None,
                schema: str = "public", logs_table: str = "crag_runs_logs") -> None:
    sql = f"""
    INSERT INTO {schema}.{logs_table}
    (batch_id, phase, window_label, rows_deleted, ru_observed, ru_per_row_obs, load_ts)
    VALUES (%s, 'cleanup', %s, %s, %s, %s, now());
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (batch_id, window_label, rows_deleted, ru_observed, ru_per_row_obs))
        conn.commit()

    
def upsert_from_staging(
    load_batch_id: str,
    hours: int,
    *,
    safety_min: int = WINDOW_SAFETY_MIN,
    chunk_size: int = 5000,       
    sleep_seconds: float = 0.02, 
    start_ts: datetime | None = None,
    end_ts: datetime | None = None
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
    default_end   = _hour_floor(now_utc - timedelta(minutes=max(0, safety_min)))
    default_start = default_end - timedelta(hours=max(1, hours) - 1)
    start = start_ts or default_start
    end   = end_ts   or default_end
    run_ts = _hour_floor(now_utc)  

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
        params = {"b": load_batch_id, "start_ts": start, "end_ts": end, "run_ts": run_ts}
        cur.execute(upsert_sql, params)
        upserted = cur.rowcount or 0

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


def log_run_start(batch_id:str, dp: int) -> str:
    """
    Logging runs in crag_runs_logs at start of upsert
    """

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("""
        INSERT INTO public.crag_runs_logs (job_name, status, notes)           
        VALUES ('weather_updater', 'running', %(n)s)
        RETURNING run_id::text
        """, {"n":f"batch={batch_id}, dp={dp}"})
        conn.commit()
        return cur.fetchone()["run_id"]

def log_run_finish(
    run_id: str,
    staged: int,
    unmatched: int,
    upserted: int,
    status: str,
    *,  # keyword-only (optional) metrics from here down
    rows_inserted: int | None = None,
    rows_updated: int | None = None,
    rows_deleted: int | None = None,
    ru_estimate: int | None = None,
    ru_per_k: float | None = None):
  
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("""
        UPDATE public.crag_runs_logs
          SET status = %(s)s,
                    notes = %(n)s,
                    finished_at = now(),
                    rows_inserted = COALESCE(%(ri)s, rows_inserted),
                    rows_updated  = COALESCE(%(ru)s, rows_updated),
                    rows_deleted  = COALESCE(%(rd)s, rows_deleted),
                    ru_estimate   = COALESCE(%(rue)s, ru_estimate),
                    ru_per_k      = COALESCE(%(ruk)s, ru_per_k)
        WHERE run_id = %(ids)s::uuid
        """, {
            "s": status,
            "n": f"staged={staged}, unmatched={unmatched}, upserted={upserted}",
            "ri": rows_inserted, "ru": rows_updated, "rd": rows_deleted,
            "rue": ru_estimate, "ruk": ru_per_k,
            "ids": run_id
        })
        conn.commit()
          


if __name__ == "__main__":
    ensure_schema()
    print("Schema ensured")
    