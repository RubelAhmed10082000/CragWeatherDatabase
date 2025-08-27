import os
import csv
import io
from typing import Iterable, Sequence, Mapping, Any
from psycopg import connect
from psycopg.rows import dict_row
import math

DATABASE_URL = os.environ["DATABASE_URL"]

def get_connection():
    """
    Connecting to PostgreSQL database
    """
    return connect(DATABASE_URL, row_factory=dict_row)

def fetch_crag_ids_for_shard(total_shards: int, shard_index: int,
                             refresh_rings: int | None = None,
                             ring_index: int | None = None) -> list[int]:
    q = """
        SELECT crag_id
        FROM public.dimcrags
        WHERE mod(crag_id, %s) = %s
    """
    params = [total_shards, shard_index]

    if refresh_rings is not None and ring_index is not None:
        q += " AND mod(crag_id, %s) = %s"
        params += [refresh_rings, ring_index]

    q += " ORDER BY crag_id"

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(q, params)
        return [r[0] for r in cur.fetchall()]
    
def fetch_coords_for_crags(crag_ids: Iterable[int]) -> dict[int, tuple[float,float]]:
    """
    Fetching coordinates for each crag. We will average out each route coord per crag
    """

    ids = list(crag_ids)
    if not ids:
        return {}
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("""
        SELECT crag_id, latitude AS lat, longitude AS lon
        FROM dimcrags
        WHERE crag_id = ANY(%(ids)s)
          AND latitude  IS NOT NULL
          AND longitude IS NOT NULL
        """, {"ids": ids})
        return {r["crag_id"]: (r["lat"], r["lon"]) for r in cur.fetchall()}
    
def ensure_weather_table():
    """
    Ensures weather table, if not, it creates an exact replica, ready for upsert.
    Ensures portability between Postgres instances
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS dimhourlyweatherinfo (
      crag_id                      BIGINT           NOT NULL,
      date                         TIMESTAMPTZ      NOT NULL,
      latitude                     DOUBLE PRECISION NOT NULL,
      longitude                    DOUBLE PRECISION NOT NULL,
      temperature_c                REAL,
      precipitation_percentage     REAL,
      relative_humidity_percentage REAL,
      load_batch_id                 text,
      load_ts                       timestamptz NOT NULL DEFAULT now(),
      CONSTRAINT pk_dimhourlyweatherinfo PRIMARY KEY(crag_id, date),
      CONSTRAINT fk_dimhourlyweatherinfo_crag
        FOREIGN KEY (crag_id) REFERENCES dimcrags(crag_id)
        ON UPDATE CASCADE ON DELETE NO ACTION
    );

    ALTER TABLE public.dimhourlyweatherinfo
          ADD COLUMN IF NOT EXISTS last_updated_ts TIMESTAMPTZ;
    
    ALTER TABLE public.dimhourlyweatherinfo
          ADD COLUMN IF NOT EXISTS load_ts TIMESTAMPTZ NOT NULL DEFAULT now();

    CREATE UNIQUE INDEX IF NOT EXISTS dimhourlyweatherinfo_crag_date_uidx
          ON public.dimhourlyweatherinfo (crag_id, date);

    CREATE INDEX IF NOT EXISTS dimhourlyweatherinfo_date_idx
      ON dimhourlyweatherinfo (date);

    CREATE INDEX IF NOT EXISTS dimhourlyweatherinfo_crag_idx
      ON dimhourlyweatherinfo (crag_id);

    CREATE INDEX IF NOT EXISTS dimhourlyweatherinfo_lat_lon_idx
      ON dimhourlyweatherinfo (latitude, longitude);
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(ddl)
        conn.commit()

def ensure_staging_exists():
    """Ensure staging table + helpful indexes exist."""
    ddl ="""
    CREATE TABLE IF NOT EXISTS public.stg_weather_route (
      date                          timestamptz,
      precipitation_percentage      real,
      temperature_c                 real,
      longitude                     double precision,
      latitude                      double precision,
      relative_humidity_percentage  real,
      load_batch_id                 text,
      load_ts                       timestamptz NOT NULL DEFAULT now()
    );

    CREATE INDEX IF NOT EXISTS stg_batch_idx
      ON public.stg_weather_route (load_batch_id);

    CREATE INDEX IF NOT EXISTS stg_loadts_idx
      ON public.stg_weather_route (load_ts);

    CREATE INDEX IF NOT EXISTS stg_latlon6_idx
      ON public.stg_weather_route (round(latitude::numeric,6), round(longitude::numeric,6));
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(ddl)
        conn.commit()


def _clean(v):
    if v is None:
        return ""
    if isinstance(v, float) and math.isnan(v):
        return ""
    return v

def load_to_staging(rows: Sequence[Mapping[str, Any] | Sequence[Any]], load_batch_id: str) -> int:
    if not rows:
        return 0

    def to_mapping(r) -> Mapping[str, Any]:
        if isinstance(r, Mapping):
            return {
                "date": r["date"],
                "precipitation_percentage": r.get("precipitation_percentage"),
                "temperature_c": r.get("temperature_c"),
                "longitude": r["longitude"],
                "latitude": r["latitude"],
                "relative_humidity_percentage": r.get("relative_humidity_percentage"),
                "load_batch_id": load_batch_id,
            }
        date, precip, temp, lon, lat, rh = r
        return {
            "date": date,
            "precipitation_percentage": precip,
            "temperature_c": temp,
            "longitude": lon,
            "latitude": lat,
            "relative_humidity_percentage": rh,
            "load_batch_id": load_batch_id,
        }

    normalized = [to_mapping(r) for r in rows]

    cols = [
        "date",
        "precipitation_percentage",
        "temperature_c",
        "longitude",
        "latitude",
        "relative_humidity_percentage",
        "load_batch_id",
    ]

    buf = io.StringIO()
    writer = csv.writer(buf, delimiter="\t", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    for r in normalized:
        writer.writerow([_clean(r.get(c)) for c in cols])
    buf.seek(0)

    with get_connection() as conn, conn.cursor() as cur:
        copy_sql = (
            "COPY public.stg_weather_route (" + ", ".join(cols) + ") "
            "FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '')"
        )
        with cur.copy(copy_sql) as cp:
            cp.write(buf.getvalue())
        conn.commit()

    return len(normalized)
    
def merge_batch(load_batch_id: str, dp: int) -> int:
    """
    Upsert from staging into dimhourlyweatherinfo, tagging lineage columns.
    """
    sql = """
    INSERT INTO public.dimhourlyweatherinfo AS d (
      crag_id, date, precipitation_percentage, temperature_c,
      longitude, latitude, relative_humidity_percentage,
      load_batch_id, load_ts, last_updated_ts
    )
    SELECT
      c.crag_id, s.date, s.precipitation_percentage, s.temperature_c,
      s.longitude, s.latitude, s.relative_humidity_percentage,
      s.load_batch_id,
      now() AS load_ts,
      now() AS last_updated_ts
    FROM public.stg_weather_route s
    JOIN public.dimcrags c
      ON round(c.latitude::numeric, %s)  = round(s.latitude::numeric, %s)
     AND round(c.longitude::numeric, %s) = round(s.longitude::numeric, %s)
    WHERE s.load_batch_id = %s
    ON CONFLICT (crag_id, date)
    DO UPDATE SET
      precipitation_percentage     = EXCLUDED.precipitation_percentage,
      temperature_c                = EXCLUDED.temperature_c,
      relative_humidity_percentage = EXCLUDED.relative_humidity_percentage,
      longitude                    = EXCLUDED.longitude,
      latitude                     = EXCLUDED.latitude,
      load_batch_id                = EXCLUDED.load_batch_id,  -- tag latest batch
      load_ts                      = d.load_ts,               -- keep first-seen
      last_updated_ts              = NOW();                   -- mark update
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (dp, dp, dp, dp, load_batch_id))
        affected = cur.rowcount
        conn.commit()
        return affected
    
def truncate_staging() -> None:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE public.stg_weather_route")
        conn.commit()

def count_unmatched_staging(dp: int = 6, load_batch_id: str | None = None) -> int:
    params = {}
    where_batch = ""
    if load_batch_id:
        where_batch = "WHERE s.load_batch_id = %(batch)s"
        params["batch"] = load_batch_id

    q = f"""
    WITH joined AS (
      SELECT s.*, c.crag_id
      FROM public.stg_weather_route s
      LEFT JOIN public.dimcrags c
        ON round(c.latitude::numeric,{dp})  = round(s.latitude::numeric,{dp})
       AND round(c.longitude::numeric,{dp}) = round(s.longitude::numeric,{dp})
      {where_batch}
    )
    SELECT COUNT(*) AS n FROM joined WHERE crag_id IS NULL;
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(q, params)
        return cur.fetchone()["n"]

def delete_staging_batch(load_batch_id: str) -> int:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM public.stg_weather_route WHERE load_batch_id = %(b)s",
            {"b": load_batch_id},
        )
        deleted = cur.rowcount
        conn.commit()
        return deleted
    
def log_run_start(load_batch_id: str, dp: int) -> int:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("""
          INSERT INTO public.weather_load_runs (load_batch_id, dp, status)
          VALUES (%s, %s, 'running') RETURNING id
        """, (load_batch_id, dp))
        run_id = cur.fetchone()["id"]
        conn.commit()
        return run_id

def log_run_finish(run_id: int, staged: int, unmatched: int, upserted: int, status: str = "success"):
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("""
          UPDATE public.weather_load_runs
             SET finished_at = now(),
                 staged_count = %s,
                 unmatched_count = %s,
                 upserted_count = %s,
                 status = %s
           WHERE id = %s
        """, (staged, unmatched, upserted, status, run_id))
        conn.commit()

def delete_old_staging(days: int = 7) -> int:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM public.stg_weather_route WHERE load_ts < now() - interval %s",
            (f"{int(days)} days",),
        )
        n = cur.rowcount
        conn.commit()
        return n

def ensure_brin_index_on_weather_date_concurrent(pages_per_range: int = 128) -> None:
    ddl = f"""
    CREATE INDEX CONCURRENTLY IF NOT EXISTS dimhourlyweatherinfo_date_brin
      ON public.dimhourlyweatherinfo
      USING BRIN (date)
      WITH (pages_per_range = {int(pages_per_range)});
    """
    with get_connection() as conn:
        conn.autocommit = True          
        with conn.cursor() as cur:
            cur.execute(ddl)

def delete_old_staging(days: int = 7) -> int:
    """
    Delete staging rows older than N days. Returns number of rows deleted.
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM public.stg_weather_route WHERE load_ts < now() - (%s || ' days')::interval",
            (str(int(days)),),
        )
        deleted = cur.rowcount
        conn.commit()
        return deleted
    
def upsert_dim_hourly(cur, *, dp: int, batch_id: str) -> int:
    sql = """
    INSERT INTO public.dimhourlyweatherinfo AS d (
      crag_id, date, precipitation_percentage, temperature_c,
      longitude, latitude, relative_humidity_percentage,
      load_batch_id, load_ts, last_updated_ts
    )
    SELECT
      c.crag_id, s.date, s.precipitation_percentage, s.temperature_c,
      s.longitude, s.latitude, s.relative_humidity_percentage,
      s.load_batch_id,
      now() AS load_ts,
      now() AS last_updated_ts
    FROM public.stg_weather_route s
    JOIN public.dimcrags c
      ON round(c.latitude::numeric, %s)  = round(s.latitude::numeric, %s)
     AND round(c.longitude::numeric, %s) = round(s.longitude::numeric, %s)
    WHERE s.load_batch_id = %s
    ON CONFLICT (crag_id, date)
    DO UPDATE SET
      precipitation_percentage     = EXCLUDED.precipitation_percentage,
      temperature_c                = EXCLUDED.temperature_c,
      relative_humidity_percentage = EXCLUDED.relative_humidity_percentage,
      longitude                    = EXCLUDED.longitude,
      latitude                     = EXCLUDED.latitude,
      load_batch_id                = EXCLUDED.load_batch_id,  -- tag the batch
      load_ts                      = d.load_ts,               -- keep first-seen
      last_updated_ts              = NOW();                   -- mark update
    """
    cur.execute(sql, (dp, dp, dp, dp, batch_id))
    return cur.rowcount