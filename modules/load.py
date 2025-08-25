# modules/load.py
import os
import time
import pandas as pd
import fsspec
from psycopg import connect, sql
from modules.gcs_io import read_parquet, gcs_url

DATABASE_URL = os.getenv("DATABASE_URL")
TABLE_ROUTES  = "dimroutes"
TABLE_WEATHER = "dimhourlyweatherinfo"
TABLE_FACT    = "fact_hourlyrouteweather"
STG_WEATHER_ROUTE = "stg_weather_route"



def _write_csv_gcs(df: pd.DataFrame, gs_uri: str, *, header=False, na_rep="\\N") -> None:
    """Write CSV to GCS with Postgres-friendly NULL marker."""
    with fsspec.open(gs_uri, "w", newline="") as f:
        df.to_csv(f, index=False, header=header, na_rep=na_rep)

def _copy_csv_to_table_from_gcs(csv_gs_uri: str, table: str, columns: list[str]) -> int:
    """Stream a CSV from GCS into Postgres via COPY FROM STDIN."""
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
        return cur.fetchone()[0]


def parquet_to_csv_crag(parquet_gs_uri: str, csv_gs_uri: str) -> list[str]:
    """
    Read crag_df parquet from GCS, normalize column names/order, coerce types,
    and write a headerless CSV to GCS. Returns the exact column list used (for COPY).
    """
    df = read_parquet(parquet_gs_uri).copy()

    renames = {
        "type": "climbing_type",
        "difficulty_grade": "climbing_grade",
        "routes_count": "route_count",
    }
    df = df.rename(columns=renames)

    if "crag_name" not in df.columns and "name" in df.columns:
        df["crag_name"] = df["name"]

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

    for c in ("crag_name","route_name","climbing_type","safety_grade","climbing_grade",
              "sector_name","rocktype","country","county"):
        if c in df.columns:
            df[c] = df[c].astype("string").str.strip()

    for c in ("longitude","latitude","route_count"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    CLIMB_ENUM = {
        "Bouldering","Trad","Sport","Top Rope","Winter","DWS","Scrambling","Mixed",
        "Boulder Circuit","Aid","Ice","Alpine","Via Ferrata"
    }
    if "climbing_type" in df.columns:
        bad = set(df["climbing_type"].dropna().unique()) - CLIMB_ENUM
        if bad:
            print(" Unknown climbing_type values:", bad)
            df.loc[~df["climbing_type"].isin(CLIMB_ENUM), "climbing_type"] = pd.NA

    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"crag_df missing columns for dimroutes: {missing}")

    df = df.loc[:, cols]
    _write_csv_gcs(df, csv_gs_uri, header=False, na_rep="\\N")
    return cols

def parquet_to_csv_weather(parquet_gs_uri: str, csv_gs_uri: str) -> list[str]:
    """
    Read cleaned_weather_df parquet from GCS, normalize types/order, and write CSV (no header) to GCS.
    """
    df = read_parquet(parquet_gs_uri).copy()

    cols = [
        "date",
        "precipitation_percentage",
        "temperature_c",
        "longitude",
        "latitude",
        "relative_humidity_percentage",
    ]

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
    _write_csv_gcs(df, csv_gs_uri, header=False, na_rep="\\N")
    return cols


def load_from_gcs(
    crag_parquet_gs,weather_parquet_gs, csv_archive_prefix):
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")

    ts = time.strftime("%Y%m%d_%H%M%S")
    crag_csv_gs    = gcs_url(csv_archive_prefix, f"crag_load_{ts}.csv")
    weather_csv_gs = gcs_url(csv_archive_prefix, f"weather_load_{ts}.csv")

    crag_cols = parquet_to_csv_crag(crag_parquet_gs, crag_csv_gs)
    wx_cols   = parquet_to_csv_weather(weather_parquet_gs, weather_csv_gs)

    with connect(DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute(f"TRUNCATE {TABLE_FACT} RESTART IDENTITY CASCADE;")
        cur.execute(f"TRUNCATE {TABLE_WEATHER} RESTART IDENTITY CASCADE;")
        cur.execute(f"TRUNCATE {TABLE_ROUTES} RESTART IDENTITY CASCADE;")
        conn.commit()

    routes_rows  = _copy_csv_to_table_from_gcs(crag_csv_gs,    TABLE_ROUTES,  crag_cols)
    weather_rows = _copy_csv_to_table_from_gcs(weather_csv_gs, TABLE_WEATHER, wx_cols)

    with connect(DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {TABLE_FACT} (
              crag_name, route_id, weather_id, date,
              relative_humidity_percentage, temperature_c, precipitation_percentage
            )
            SELECT 
              r.route_id,
              r.crag_name,
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
        fact_rows = cur.rowcount
        conn.commit()

    print(f"dimroutes: {routes_rows} | dimhourlyweatherinfo: {weather_rows} | fact: {fact_rows}")
    return {
        "routes_rows": routes_rows,
        "weather_rows": weather_rows,
        "fact_rows": fact_rows,
        "crag_csv_gs": crag_csv_gs,
        "weather_csv_gs": weather_csv_gs,
    }


def load_weather_snapshot_from_gcs(
    weather_parquet_gs: str,
    csv_gs_uri: str,
) -> dict:
    """
    Hourly snapshot: replace weather + fact, leave routes as-is.
    """
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")

    w_cols = parquet_to_csv_weather(weather_parquet_gs, csv_gs_uri)

    with connect(DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute(f"TRUNCATE {TABLE_FACT}, {TABLE_WEATHER} RESTART IDENTITY;")
        conn.commit()

    n_stg = _copy_csv_to_table_from_gcs(csv_gs_uri, STG_WEATHER_ROUTE, w_cols)

    with connect(DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute("SELECT upsert_weather_from_staging();")
        n_weather = cur.fetchone()[0]
        conn.commit()


    with connect(DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {TABLE_FACT} (
              crag_id, crag_name, route_id, date,
              relative_humidity_percentage, temperature_c, precipitation_percentage
            )
            SELECT 
              w.crag_id,
              r.crag_name,
              r.route_id,
              w.date,
              w.relative_humidity_percentage,
              w.temperature_c,
              w.precipitation_percentage                                          
            FROM {TABLE_WEATHER} w
            JOIN {TABLE_ROUTES}  r
              ON ROUND(w.latitude::numeric,  6) = ROUND(r.latitude::numeric,  6)
             AND ROUND(w.longitude::numeric, 6) = ROUND(r.longitude::numeric, 6);
        """)
        n_fact = cur.rowcount
        conn.commit()

    print(f"staged: {n_stg} | weather upserted: {n_weather} | fact rebuilt: {n_fact}")
    return {"weather_rows": n_weather, "fact_rows": n_fact}