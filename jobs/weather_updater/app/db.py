import os
from typing import Iterable, Sequence, Mapping, Any
from psycopg import connect
from psycopg.rows import dict_row

DATABASE_URL = os.environ["DATABASE_URL"]

def get_conn():
    """
    Connecting to PostgreSQL database
    """
    return connect(DATABASE_URL, row_factory=dict_row)

def fetch_crag_ids_for_shard(total_shards: int, shard_index: int, limit:int|None=None) -> list[int]:
    """
    Assign work by crag_id % of total shards
    """

    q ="""
    SELECT crag_id
    FROM dimcrags
    WHERE crag_id IS NOT NULL
     AND (crag_id % %(total_shards)s) = %(shard_index)s
    ORDER BY crag_id
    """

    params = {"total_shards": total_shards, "shard_index": shard_index}
    if limit:
        q += " LIMIT %(limit)s"
        params["limit"] = limit

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(q, params)
        return [r["crag_id"] for r in cur.fetchall()]
    
def fetch_coords_for_crags(crag_ids: Iterable[int]) -> dict[int, tuple[float,float]]:
    """
    Fetching coordinates for each crag. We will average out each route coord per crag
    """

    ids = list(crag_ids)
    if not ids:
        return {}
    with get_conn() as conn, conn.cursor() as cur:
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

    CREATE INDEX IF NOT EXISTS dimhourlyweatherinfo_date_idx
      ON dimhourlyweatherinfo (date);

    CREATE INDEX IF NOT EXISTS dimhourlyweatherinfo_crag_idx
      ON dimhourlyweatherinfo (crag_id);

    CREATE INDEX IF NOT EXISTS dimhourlyweatherinfo_lat_lon_idx
      ON dimhourlyweatherinfo (latitude, longitude);
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(ddl)
        conn.commit()

def ensure_staging_exists():
    """Ensure staging table + helpful indexes exist."""
    ddl = """
    CREATE TABLE IF NOT EXISTS public.stg_weather_route (
      date                          timestamptz,
      precipitation_percentage      real,
      temperature_c                 real,
      longitude                     double precision,
      latitude                      double precision,
      relative_humidity_percentage  real
    );

    CREATE INDEX IF NOT EXISTS stg_latlon5_idx
      ON public.stg_weather_route (round(latitude::numeric,6), round(longitude::numeric,6));
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(ddl)
        conn.commit()


def load_to_staging(rows: Sequence[Mapping[str, Any] | Sequence[Any]], load_batch_id: str) -> int:
    """
    Insert parquet rows into public.stg_weather_route.
    """
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

    params = [to_mapping(r) for r in rows]

    insert_sql = """
        INSERT INTO public.stg_weather_route
          (date, precipitation_percentage, temperature_c, longitude, latitude, relative_humidity_percentage, load_batch_id)
        VALUES
          (%(date)s, %(precipitation_percentage)s, %(temperature_c)s, %(longitude)s, %(latitude)s, %(relative_humidity_percentage)s, %(load_batch_id)s)
    """

    with get_conn() as conn, conn.cursor() as cur:
        cur.executemany(insert_sql, params)
        conn.commit()
        return len(params)
    
def merge_staging_into_weather(dp: int = 5) -> int:
    merge_sql = f"""
    WITH src AS(
    SELECT DISTINCT ON (c.crag_id, s.date)
      c.crag_id,
      s.date,
      s.latitude,
      s.longitude,
      s.temperature_c,
      s.precipitation_percentage,
      s.relative_humidity_percentage
    FROM public.stg_weather_route s
    JOIN public.dimcrags c
    ON round(c.latitude::numeric,  {dp}) = round(s.latitude::numeric, {dp})
    AND round(c.longitude::numeric,  {dp}) = round(s.longitude::numeric, {dp})
    ORDER BY c.crag_id, s.date, s.load_ts DESC
    )
    INSERT INTO public.dimhourlyweatherinfo AS d (
          crag_id, date, latitude, longitude,
          temperature_c, precipitation_percentage, relative_humidity_percentage
        )
        SELECT
          crag_id, date, latitude, longitude,
          temperature_c, precipitation_percentage, relative_humidity_percentage
        FROM src
        ON CONFLICT (crag_id, date) DO UPDATE SET
          latitude                     = EXCLUDED.latitude,
          longitude                    = EXCLUDED.longitude,
          temperature_c                = EXCLUDED.temperature_c,
          precipitation_percentage     = EXCLUDED.precipitation_percentage,
          relative_humidity_percentage = EXCLUDED.relative_humidity_percentage
        RETURNING 1;
    """
    with get_conn() as conn, conn.cursor() as cur:
            cur.execute(merge_sql)
            affected = len(cur.fetchall())
            conn.commit()
            return affected

def truncate_staging() -> None:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE public.stg_weather_route")
        conn.commit()

def count_unmatched_staging(dp: int = 5) -> int:
    q = f"""
    SELECT count(*) AS n
    FROM public.stg_weather_route s
    LEFT JOIN public.dimcrags c
      ON round(c.latitude::numeric,  {dp}) = round(s.latitude::numeric,  {dp})
     AND round(c.longitude::numeric, {dp}) = round(s.longitude::numeric, {dp})
    WHERE c.crag_id IS NULL
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(q)
        return cur.fetchone()["n"]
