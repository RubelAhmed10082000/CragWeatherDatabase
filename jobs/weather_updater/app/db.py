import os
from typing import Iterable, Sequence, Mapping, Any
from psycopg import connect, sql
from psycopg.rows import dict_row

DATABASE_URL = os.environ["DATABASE_URL"]

def get_conn():
    """
    Connecting to PostgreSQL database
    """
    return connect(DATABASE_URL, row_factory=dict_row)

def fetch_crag_ids_for_shard(total_shards: int, shard_index: int, limit:int|None=None) -> list[int]:
    """
    Assing work by crag_id % of total shards
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
      CONSTRAINT pk_dimhourlyweatherinfo PRIMARY KEY (crag_id, date),
      CONSTRAINT fk_dimhourlyweatherinfo_crag
        FOREIGN KEY (crag_id) REFERENCES dimcrags(crag_id)
        ON UPDATE CASCADE ON DELETE CASCADE
    );

    CREATE UNIQUE INDEX IF NOT EXISTS dimhourlyweatherinfo_crag_date_uniq_idx
      ON dimhourlyweatherinfo (crag_id, date);

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




