from __future__ import annotations
import os
from contextlib import contextmanager
import psycopg
from psycopg.rows import dict_row
from typing import Iterable, Mapping, Any
from datetime import timedelta

DATABASE_URL = os.environ.get("DATABASE_URL")

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
            'Psammite']

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
            'Via Ferrata']

UNKNOWNS = {"UNKNOWN", "UNK", "N/A", "NA", ""}

ROCK_TYPES_ALLOWED = [v for v in ROCK_TYPES if v.strip().upper() not in UNKNOWNS]
STYLES_ALLOWED      = [v for v in CLIMBING_STYLES if v.strip().upper() not in UNKNOWNS]

SCHEMA_VERSION_ID = "2025-08-28_optA_text_checks_v3"

@contextmanager
def get_connection():
    """
    Creates connection to Cockroach DB
    """

    # Raises RuntimeError if no DATABSE_URL set
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    # Establishes connection via psycopg 
    with psycopg.connect(DATABASE_URL, autocommit=True, 
                         row_factory=dict_row) as conn:
        yield conn

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
        # Executes functions found later in the script
        _ensure_primitives(conn)
        _ensure_tables(conn)
        _ensure_views(conn) 
        _ensure_indexes(conn)
        _record_version(conn, SCHEMA_VERSION_ID)

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
    
    # dimcrags 
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
    
    # Fact table at crag x hour grain
    run_sql(conn, """
    CREATE TABLE IF NOT EXISTS public.fact_crag_hourly_weather (
            crag_id UUID NOT NULL,
            date TIMESTAMPTZ NOT NULL,
            temperature_c REAL,
            relative_humidity_percentage REAL,
            precipitation_mm NUMERIC(6,2),
            windspeed_ms REAL,
            load_ts TIMESTAMPTZ DEFAULT now(),
            load_batch_id TEXT NOT NULL,
            forecast_run_ts TIMESTAMPTZ,
            horizon_hours INT,
            CONSTRAINT fact_crag_hourly_weather_pk PRIMARY KEY (crag_id, date),
            CONSTRAINT fact_crag_hourly_weather_crag_fk FOREIGN KEY (crag_id) REFERENCES public.dimcrags (crag_id),
            CONSTRAINT rh_0_100_chk CHECK (relative_humidity_percentage IS NULL OR (relative_humidity_percentage BETWEEN 0 AND 100)),
            CONSTRAINT precip_0_100_chk CHECK (precipitation_mm IS NULL OR precipitation_mm >= 0),
            CONSTRAINT wind_nonneg_chk CHECK (windspeed_ms IS NULL OR windspeed_ms >= 0)
      );
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
            longitude DOUBLE PRECISION,
            latitude DOUBLE PRECISION,
            load_batch_id TEXT NOT NULL,
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
    
    # Creating table that last time each crag experienced rain
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
    run_sql(conn, "CREATE INDEX IF NOT EXISTS dimroutes_crag_idx ON public.dimroutes (crag_id);")
    run_sql(conn, "CREATE INDEX IF NOT EXISTS fact_weather_date_idx ON public.fact_crag_hourly_weather (date);")
    run_sql(conn, "CREATE INDEX IF NOT EXISTS fact_weather_crag_date_idx ON public.fact_crag_hourly_weather (crag_id, date);")
    run_sql(conn, "CREATE INDEX IF NOT EXISTS stg_weather_batch_idx ON public.stg_weather_route (load_batch_id);")
    run_sql(conn, "CREATE INDEX IF NOT EXISTS stg_weather_crag_date_idx ON public.stg_weather_route (crag_id, date);")
    run_sql(conn, "CREATE INDEX IF NOT EXISTS fact_weather_batch_idx ON public.fact_crag_hourly_weather (load_batch_id);")
    run_sql(conn, "CREATE INDEX IF NOT EXISTS fact_weather_loadts_idx ON public.fact_crag_hourly_weather (load_ts);")

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
  
  # routes + crags

    run_sql(conn, """ 
    CREATE OR REPLACE VIEW public.v_routes_with_crag AS
    SELECT 
      r.route_id,
      r.route_name,
      r.grade,
      r.safety_grade,
      r.crag_id,
      c.crag_name,
      c.county,
      c.latitude,
      c.longitude,
      c.rocktype,
      c.climbing_style
    FROM public.dimroutes r 
    JOIN public.dimcrags c ON c.crag_id = r.crag_id;
    """)

  # Weather with crag attributes for API/frontend
    run_sql(conn, "DROP VIEW IF EXISTS public.v_crag_hourly_weather")
    
    run_sql(conn, """
    CREATE OR REPLACE VIEW public.v_crag_hourly_weather AS
    SELECT 
      f.crag_id,
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
            ELSE GREATEST(0, EXTRACT(EPOCH FROM ((now() AT TIME ZONE 'UTC') - s.last_rained_ts))/3600.0)::int
            END AS hours_since_rain
    FROM public.fact_crag_hourly_weather f
    JOIN public.dimcrags c ON c.crag_id = f.crag_id
    LEFT JOIN public.crag_last_rain_state s ON s.crag_id = f.crag_id;
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
      WHERE (fnv32(crag_id::text)::int % %(total)s) = %(idx)s
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
    
def load_to_staging(rows: Iterable[Mapping[str,Any]], load_batch_id: str, batch_size: int = 5000) -> int:
    """
    Batch insert into stg_weather_route
    """

    rows = list(rows)
    if not rows:
        return 0
    
    cols = ["date","precipitation_mm","temperature_c","relative_humidity_percentage",
            "windspeed_ms","crag_id","longitude","latitude","load_batch_id"]
    inserted = 0

    with get_connection() as conn, conn.cursor() as cur:
        for i in range (0, len(rows), batch_size):
            chunk = rows[i:i+batch_size]
            values = [
              (r["date"], r.get("precipitation_mm"), r.get("temperature_c"), r.get("relative_humidity_percentage"),
               r.get("windspeed_ms"), r["crag_id"], r["longitude"], r["latitude"], load_batch_id)
              for r in chunk
            ]

            cur.execute(
                f"""
                INSERT INTO public.stg_weather_route
                ({", ".join(cols)})
                VALUES {",".join(["(%s,%s,%s,%s,%s,%s,%s,%s,%s)"]*len(values))}
                ON CONFLICT (crag_id, date, load_batch_id) DO NOTHING
                """,
                [v for row in values for v in row]
            )
            inserted += cur.rowcount if cur.rowcount is not None else 0 
    return inserted

def delete_staging_batch(load_batch_id:str) -> int:
    """
    Deletes load batches
    """

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM public.stg_weather_route WHERE load_batch_id = %(b)s", 
                    {"b":load_batch_id}
        )
        deleted = cur.rowcount or 0
    return deleted 

def delete_old_staging(days: int = 7) -> int:
    """
    Deletes staging table after upsert
    """

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM public.stg_weather_route WHERE date < now() - (%(d)s || ' days')::interval", {"d":days})
        return cur.rowcount or 0
    


def upsert_fact_window(load_batch_id: str, hours: int = 12) -> tuple[int, int]:
    """
    Upserts fact table with hourly weather data
    Sliding window basis
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("SET application_name = 'cragcast_upsert_window'")
        cur.execute("SELECT date_trunc('hour', now() AT TIME ZONE 'UTC') AS w_start")
        window_start = cur.fetchone()["w_start"]
        cur.execute("SELECT %(ws)s + (%(h)s || ' hours')::interval AS w_end",
                    {"ws": window_start, "h": hours})
        window_end = cur.fetchone()["w_end"]

        cur.execute("""
          WITH batch_crags AS (
            SELECT DISTINCT crag_id
            FROM public.stg_weather_route
            WHERE load_batch_id = %(b)s
          ),
          del AS (
            DELETE FROM public.fact_crag_hourly_weather f
            USING batch_crags bc
            WHERE f.crag_id = bc.crag_id
              AND f.date >= %(ws)s::timestamptz AND f.date < %(we)s::timestamptz
            RETURNING 1
          )
          SELECT count(*) AS c FROM del
        """, {"b": load_batch_id, "ws": window_start, "we": window_end})
        deleted = cur.fetchone()["c"]

        cur.execute("""
          WITH batch_crags AS (
            SELECT DISTINCT crag_id
            FROM public.stg_weather_route
            WHERE load_batch_id = %(b)s
          )
          INSERT INTO public.fact_crag_hourly_weather
            (crag_id, date, temperature_c, relative_humidity_percentage,
             precipitation_mm, windspeed_ms, load_ts, load_batch_id,
             forecast_run_ts, horizon_hours)
          SELECT
            s.crag_id,
            s.date,
            s.temperature_c,
            s.relative_humidity_percentage,
            s.precipitation_mm,
            s.windspeed_ms,
            now(),
            s.load_batch_id,
            %(ws)s::timestamptz AS forecast_run_ts,
            (EXTRACT(EPOCH FROM (s.date - %(ws)s::timestamptz)) / 3600.0)::int AS horizon_hours
          FROM public.stg_weather_route s
          JOIN batch_crags bc USING (crag_id)
          WHERE s.load_batch_id = %(b)s
            AND s.date >= %(ws)s::timestamptz AND s.date < %(we)s::timestamptz
          ORDER BY s.crag_id, s.date
        """, {"b": load_batch_id, "ws": window_start, "we": window_end})
        inserted = cur.rowcount or 0

        cur.execute("SELECT now() AT TIME ZONE 'UTC' AS cap")
        cap_ts = cur.fetchone()['cap']


        cur.execute("""
          WITH batch_crags AS (
            SELECT DISTINCT crag_id
            FROM public.stg_weather_route
            WHERE load_batch_id = %(b)s
          ),
          newest_in_window AS (
            SELECT
              s.crag_id,
              max(CASE WHEN s.precipitation_mm > 0 THEN s.date END) AS newest_rain_ts
            FROM public.stg_weather_route s
            JOIN batch_crags bc USING (crag_id)
            WHERE s.load_batch_id = %(b)s
              AND s.date >= %(ws)s::timestamptz 
              AND s.date < %(we)s::timestamptz
              AND s.date <= %(cap)s::timestamptz
            GROUP BY s.crag_id
          ),
          severity_lookup AS (
            SELECT
              n.crag_id,
              n.newest_rain_ts,
              CASE
                WHEN n.newest_rain_ts IS NULL THEN NULL
                ELSE (
                  SELECT CASE
                           WHEN sw.precipitation_mm < 1.0 THEN 'light'
                           WHEN sw.precipitation_mm < 4.0 THEN 'medium'
                           ELSE 'heavy'
                         END
                  FROM public.stg_weather_route sw
                  WHERE sw.crag_id = n.crag_id
                    AND sw.date = n.newest_rain_ts
                  LIMIT 1
                )
              END AS newest_rain_severity
            FROM newest_in_window n
          ),
          merged AS (
            SELECT
              bc.crag_id,
              LEAST(                      
                COALESCE(sl.newest_rain_ts, prs.last_rained_ts),
                %(cap)s::timestamptz
              ) AS last_rained_ts,
              COALESCE(sl.newest_rain_severity, prs.last_rain_severity) AS last_rain_severity
            FROM batch_crags bc
            LEFT JOIN severity_lookup sl ON sl.crag_id = bc.crag_id
            LEFT JOIN public.crag_last_rain_state prs ON prs.crag_id = bc.crag_id
          )
          INSERT INTO public.crag_last_rain_state AS s
            (crag_id, last_rained_ts, last_rain_severity, updated_at)
          SELECT crag_id, last_rained_ts, last_rain_severity, now()
          FROM merged
          ON CONFLICT (crag_id) DO UPDATE
            SET last_rained_ts = EXCLUDED.last_rained_ts,
                last_rain_severity = EXCLUDED.last_rain_severity,
                updated_at = now();
        """, {"b": load_batch_id, "ws": window_start,
               "we": window_end, "cap":cap_ts})

        return inserted, deleted


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
        return cur.fetchone()["run_id"]

def log_run_finish(run_id: str, staged: int, unmatched: int, upserted: int,
                   status:str):
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("""
        UPDATE public.crag_runs_logs
          SET status = %(s)s,
                    notes = %(n)s,
                    finished_at = now()
        WHERE run_id = %(ids)s::uuid
        """, {"s":status, "n":f"staged={staged}, unmatched={unmatched}, upserted={upserted}", "ids": run_id})
    


if __name__ == "__main__":
    ensure_schema()
    print("Schema ensured")
    