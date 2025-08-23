import os
import pandas as pd
import fsspec
from psycopg import connect, sql
from modules.gcs_io import read_parquet

DATABASE_URL = os.getenv("DATABASE_URL")
TABLE_ROUTES  = "dimroutes"
TABLE_WEATHER = "dimhourlyweatherinfo"
TABLE_FACT    = "fact_hourlyrouteweather"

def _write_csv_gcs(df: pd.DataFrame, gs_uri: str) -> None:
    with fsspec.open(gs_uri, "w", newline="") as f:
        df.to_csv(f, index=False, header=False, na_rep="\\N")

def parquet_to_csv_crag(parquet_gs_uri: str, csv_gs_uri: str) -> list[str]:
    """
    Read crag_df parquet from GCS, normalize column names/order, coerce types,
    and write a headerless CSV to GCS. Returns the exact column list used (for COPY).
    """
    df = read_parquet(parquet_gs_uri).copy()

    # 1) Rename to match DB naming
    renames = {
        "type": "climbing_type",
        "difficulty_grade": "climbing_grade",
        "routes_count": "route_count",
    }
    df = df.rename(columns=renames)

    # 2) Ensure crag_name exists (if your upstream kept it as 'name')
    if "crag_name" not in df.columns and "name" in df.columns:
        df["crag_name"] = df["name"]

    # 3) Column order MUST match dimroutes
    cols = [
        "crag_name",
        "route_name",
        "climbing_type",
        "safety_grade",
        "climbing_grade",
        "sector_name",
        "rocktype",
        "longitude",
        "latitude",
        "route_count",
        "country",
        "county",
    ]

    # 4) Trim/clean strings
    for c in ("crag_name","route_name","climbing_type","safety_grade","climbing_grade",
              "sector_name","rocktype","country","county"):
        if c in df.columns:
            df[c] = df[c].astype("string").str.strip()

    # 5) Coerce numerics
    for c in ("longitude","latitude","route_count"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # 6) ENUM safety: coerce unknown climbing_type to NULL so COPY won't fail
    CLIMB_ENUM = {'Bouldering','Trad','Sport','Top Rope','Winter','DWS','Scrambling','Mixed',
                  'Boulder Circuit','Aid','Ice','Alpine','Via Ferrata'}
    ROCK_ENUM  = {'Gritstone','Limestone','Sandstone (hard)','Granite','Grit (quarried)',
                  'Sandstone (soft)','Rhyolite','UNKNOWN','Artificial','Culm','Slate',
                  'Greenstone','Volcanic tuff','Dolerite','Andesite','Gabbro','Killas slate',
                  'Mica schist','Shale','Pillow lava','Conglomerate','Chalk','Schist',
                  'Amphibiolite & S','Welded Tuff','Quartzite','Crumbly rubbish','Hornstone',
                  'Basalt','Diorites','Welsh igneous','Ice','Serpentine','Iron Rock',
                  'Ignimbrite','Microgranite','Psammite'}
    if "climbing_type" in df.columns:
        bad = set(df["climbing_type"].dropna().unique()) - CLIMB_ENUM
        if bad: print("Unknown climbing_type values:", bad)
    if "rocktype" in df.columns:
        bad = set(df["rocktype"].dropna().unique()) - ROCK_ENUM
        if bad: print("Unknown rocktype values:", bad)



    # 7) Validate required columns
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"crag_df missing columns for dimroutes: {missing}")

    # 8) Reorder and write CSV to GCS (no header; NULL -> \N)
    df = df.loc[:, cols]
    _write_csv_gcs(df, csv_gs_uri, header=False, na_rep="\\N")

    return cols

def parquet_to_csv_weather(parquet_gs_uri: str, csv_gs_uri: str) -> list[str]:
    df = read_parquet(parquet_gs_uri)

    cols = [
        "date","precipitation_percentage","temperature_c",
        "longitude","latitude","relative_humidity_percentage"
    ]

    # Normalize types
    if "date" in df.columns:
        dt = pd.to_datetime(df["date"], utc=True, errors="coerce")
        df["date"] = dt.dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    for c in ("precipitation_percentage","temperature_c","longitude","latitude","relative_humidity_percentage"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    for c in ("precipitation_percentage","relative_humidity_percentage"):
        if c in df.columns:
            df[c] = df[c].round().clip(0,100).astype("Int64")

    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"weather_df missing columns: {missing}")

    df = df.loc[:, cols]
    _write_csv_gcs(df, csv_gs_uri)
    return cols

def copy_csv_to_table_from_gcs(csv_gs_uri: str, table: str, columns: list[str]) -> int:
    cols = ", ".join([f'"{c}"' for c in columns])
    copy_sql = f"""
        COPY {table} ({cols})
        FROM STDIN WITH (FORMAT csv, DELIMITER ',', NULL '\\N', QUOTE '\"')
    """
    with connect(DATABASE_URL) as conn, conn.cursor() as cur, fsspec.open(csv_gs_uri, "rb") as f:
        with cur.copy(copy_sql) as cp:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                cp.write(chunk)
        conn.commit()
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table)))
        n = cur.fetchone()[0]
        return n
    
def load_routes_from_gcs(crag_parquet_gs: str, csv_gs_uri: str) -> int:
    """
    One-time (or occasional) loader for routes:
      - TRUNCATE dimroutes
      - COPY fresh snapshot
    Returns number of rows in dimroutes after load.
    """
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")

    cols = parquet_to_csv_crag(crag_parquet_gs, csv_gs_uri)

    with connect(DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute(f"TRUNCATE {TABLE_ROUTES};")
        conn.commit()

    n_routes = copy_csv_to_table_from_gcs(csv_gs_uri, TABLE_ROUTES, cols)
    print(f"✅ dimroutes reloaded: {n_routes}")
    return n_routes


def load_weather_snapshot_from_gcs(weather_parquet_gs: str, csv_gs_uri: str) -> dict:
    """
    Fully replace weather + fact each run. Routes remain untouched.
    """
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")

    weather_cols = parquet_to_csv_weather(weather_parquet_gs, csv_gs_uri)

    with connect(DATABASE_URL) as conn, conn.cursor() as cur:
        # Replace weather & fact
        cur.execute(f"TRUNCATE {TABLE_FACT};")
        cur.execute(f"TRUNCATE {TABLE_WEATHER};")
        conn.commit()

    # COPY fresh weather snapshot
    n_weather = copy_csv_to_table_from_gcs(csv_gs_uri, TABLE_WEATHER, weather_cols)

    # Rebuild fact by joining to static routes
    with connect(DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {TABLE_FACT} (
              route_id, weather_id, date,
              relative_humidity_percentage, temperature_c, precipitation_percentage
            )
            SELECT 
              r.route_id,
              w.weather_id,
              w.date,
              w.relative_humidity_percentage,
              w.temperature_c,
              w.precipitation_percentage
            FROM {TABLE_WEATHER} w
            JOIN {TABLE_ROUTES}  r
              ON ROUND(w.latitude::numeric,  4) = ROUND(r.latitude::numeric,  4)
             AND ROUND(w.longitude::numeric, 4) = ROUND(r.longitude::numeric, 4);
        """)
        n_fact = cur.rowcount
        conn.commit()

    return {"weather_rows": n_weather, "fact_rows": n_fact}
