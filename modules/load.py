# loader_from_parquet.py
import os, pandas as pd
from psycopg import connect, sql

DATABASE_URL = os.getenv("DATABASE_URL")
TABLE_ROUTES  = "dimroutes"
TABLE_WEATHER = "dimhourlyweatherinfo"
TABLE_FACT    = "fact_hourlyrouteweather"

# --- shared helper ---
def _write_csv(df: pd.DataFrame, csv_path: str) -> None:
    # Postgres-friendly NULL marker
    df.to_csv(csv_path, index=False, header=False, na_rep="\\N")

def parquet_to_csv_crag(parquet_path: str, csv_path: str) -> list[str]:
    df = pd.read_parquet('data/processed/crag_df.parquet')

    renames = {
        "type": "climbing_type",
        "difficulty_grade": "climbing_grade",
        "routes_count": "route_count",
    }
    df = df.rename(columns=renames)

    cols = [
        "route_name","climbing_type","safety_grade","climbing_grade",
        "sector_name","rocktype","longitude","latitude","route_count",
        "country","county"
    ]
    

    for c in ("climbing_type","rocktype","route_name","sector_name","country","county",
              "safety_grade","climbing_grade"):
        if c in df.columns:
            df[c] = df[c].astype("string").str.strip()

    for c in ("longitude","latitude","route_count"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"crag_df missing columns after rename: {missing}")

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
        if bad: print(" Unknown climbing_type values:", bad)
    if "rocktype" in df.columns:
        bad = set(df["rocktype"].dropna().unique()) - ROCK_ENUM
        if bad: print(" Unknown rocktype values:", bad)

    df = df.loc[:, cols]
    _write_csv(df, csv_path)
    return cols

def parquet_to_csv_weather(parquet_path: str, csv_path: str) -> list[str]:
    df = pd.read_parquet('data/processed/cleaned_weather_df.parquet')

    cols = [
        "date","precipitation_percentage","temperature_c",
        "longitude","latitude","relative_humidity_percentage"
    ]

    if "date" in df.columns:
        dt = pd.to_datetime(df["date"], utc=True, errors="coerce")
        df["date"] = dt.dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    for c in ("precipitation_percentage","temperature_c","longitude","latitude","relative_humidity_percentage"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    if "precipitation_percentage" in df.columns:
        df["precipitation_percentage"] = (
            df["precipitation_percentage"].round().clip(0,100).astype("Int64")
        )

    if "relative_humidity_percentage" in df.columns:
        df["relative_humidity_percentage"] = (
            df["relative_humidity_percentage"].round().clip(0,100).astype("Int64")
        )
    
    
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"weather_df missing columns: {missing}")

    df = df.loc[:, cols]
    _write_csv(df, csv_path)
    return cols

def copy_csv_to_table(csv_path: str, table: str, columns: list[str]) -> int:
    from psycopg import sql, connect

    cols = ", ".join([f'"{c}"' for c in columns])
    copy_sql = f"""
        COPY {table} ({cols})
        FROM STDIN WITH (FORMAT csv, DELIMITER ',', NULL '\\N', QUOTE '\"')
    """

    # Open in *binary* mode on Windows to avoid newline/encoding surprises
    with connect(DATABASE_URL) as conn, conn.cursor() as cur, open(csv_path, "rb") as f:
        # psycopg3 pattern: open copy context, then write bytes into it
        with cur.copy(copy_sql) as copy:
            # If the file is big, stream in chunks to keep memory low
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                copy.write(chunk)

        conn.commit()

        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table)))
        n = cur.fetchone()[0]
        print(f"Rows now in {table}: {n}")
        return n


def load_from_parquet(crag_parquet: str, weather_parquet: str):
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not set")

    crag_csv    = "data/load/crag_load.csv"
    weather_csv = "data/load/weather_load.csv"

    crag_cols = parquet_to_csv_crag(crag_parquet, crag_csv)
    wx_cols   = parquet_to_csv_weather(weather_parquet, weather_csv)

    copy_csv_to_table(crag_csv, TABLE_ROUTES,  crag_cols)
    copy_csv_to_table(weather_csv, TABLE_WEATHER, wx_cols)

    with connect(DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute(f"""
            TRUNCATE {TABLE_FACT};
            INSERT INTO {TABLE_FACT} (
              route_id, weather_id, date, relative_humidity_percentage, temperature_c, precipitation_percentage
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
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(TABLE_FACT)))
        print("Rows now in", TABLE_FACT, ":", cur.fetchone()[0])
        conn.commit()
