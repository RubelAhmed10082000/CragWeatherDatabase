from __future__ import annotations
import os
from contextlib import contextmanager
import psycopg
from psycopg.rows import dict_row

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

SCHEMA_VERSION_ID = "2025-08-28_optA_text_checks_v1"

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
              CHECK (climbing_style IS NOT NULL OR climbing_style IN ({style_in}))
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
            precipitation_mm FLOAT,
            wind_speed_ms REAL,
            load_ts TIMESTAMPTZ DEFAULT now(),
            load_batch_id TEXT NOT NULL,
            forecast_run_ts TIMESTAMPTZ,
            horizon_hours INT,
            CONSTRAINT fact_crag_hourly_weather_pk PRIMARY KEY (crag_id, date),
            CONSTRAINT fact_crag_hourly_weather_crag_fk FOREIGN KEY (crag_id) REFERENCES public.dimcrags (crag_id),
            CONSTRAINT rh_0_100_chk CHECK (relative_humidity_percentage IS NULL OR (relative_humidity_percentage BETWEEN 0 AND 100)),
            CONSTRAINT precip_0_100_chk CHECK (precipitation_percentage IS NULL OR (precipitation_percentage BETWEEN 0 AND 100)),
            );
            """)
    
    # Staging for weather data
    run_sql(conn, """
    CREATE TABLE IF NOT EXISTS public.stg_weather_route(
            date TIMESTAMPTZ NOT NULL,
            precipitation_percentage REAL,
            temperature_c REAL,
            relative_humidity_percentage REAL,
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
      f.precipitation_percentage,
      f.wind_speed_ms,
      f.load_batch_id,
      f.load_ts,
      f.forecast_run_ts,
      f.horizon_hours
    FROM public.fact_hourly_weather f
    JOIN public.dimcrags c ON c.crag_id = f.crag_id;
""")
    

if __name__ == "__main__":
    ensure_schema()
    print("Schema ensured")
    