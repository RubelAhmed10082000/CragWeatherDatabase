import os
import io
import pandas as pd
from psycopg import connect, sql

TABLE_ROUTES  = "dimroutes"
TABLE_WEATHER = "dimhourlyweatherinfo"
TABLE_FACT    = "fact_hourlyrouteweather"

DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set.")

def create_schema():
    with connect(DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute("SELECT current_database(), current_user, current_schema, current_setting('search_path')")
        print("DB identity:", cur.fetchone())

    ddl = f"""
    -- Create enums safely
    DO $$
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'climbing_type_enum') THEN
        CREATE TYPE climbing_type_enum AS ENUM (
          'Bouldering','Trad','Sport','Top Rope','Winter','DWS','Scrambling','Mixed',
          'Boulder Circuit','Aid','Ice','Alpine','Via Ferrata'
        );
      END IF;
    END$$;

    DO $$
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'rock_type_enum') THEN
        CREATE TYPE rock_type_enum AS ENUM (
          'Gritstone','Limestone','Sandstone (hard)','Granite','Grit (quarried)',
          'Sandstone (soft)','Rhyolite','UNKNOWN','Artificial','Culm','Slate',
          'Greenstone','Volcanic tuff','Dolerite','Andesite','Gabbro','Killas slate',
          'Mica schist','Shale','Pillow lava','Conglomerate','Chalk','Schist',
          'Amphibiolite & S','Welded Tuff','Quartzite','Crumbly rubbish','Hornstone',
          'Basalt','Diorites','Welsh igneous','Ice','Serpentine','Iron Rock',
          'Ignimbrite','Microgranite','Psammite'
        );
      END IF;
    END$$;

    -- Tables
    CREATE TABLE IF NOT EXISTS dimhourlyweatherinfo (
        weather_id SERIAL PRIMARY KEY,
        date TIMESTAMPTZ,
        precipitation_percentage INT,
        temperature_c FLOAT,
        longitude FLOAT,
        latitude FLOAT,
        relative_humidity_percentage INT
    );

    CREATE TABLE IF NOT EXISTS dimroutes (
        route_id SERIAL PRIMARY KEY,
        route_name VARCHAR,
        climbing_type climbing_type_enum,
        safety_grade VARCHAR,
        climbing_grade VARCHAR,
        sector_name VARCHAR,
        rocktype rock_type_enum,
        longitude FLOAT,
        latitude FLOAT,
        route_count INT,
        country VARCHAR,
        county VARCHAR
    );

    CREATE TABLE IF NOT EXISTS fact_hourlyrouteweather (
        route_id INT REFERENCES dimroutes(route_id),
        weather_id INT REFERENCES dimhourlyweatherinfo(weather_id),
        date TIMESTAMPTZ,
        relative_humidity_percentage INT,
        temperature_c FLOAT,
        precipitation_percentage INT
    );
    """
    with connect(DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute(ddl)
        conn.commit()

def copy_dataframe(df, table, columns):
    """Loads dataframe into PostgreSQL using COPY"""   
    if df.empty:
        print("DataFrame is empty. No data to load.")
        return
    
    if columns:
        missing = [c for c in columns if c not in df.columns]
        if missing:
            raise ValueError(f"Columns {missing} not found in DataFrame. Available columns: {df.columns.tolist()}")
        df = df.loc[:, columns]

    print(f"About to COPY into {table}: df.shape={df.shape}")
    df = df.where(pd.notnull(df), None)

    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)

    with connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cols = sql.SQL(",").join(map(sql.Identifier, df.columns))
            stmt = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv)").format(
                sql.Identifier(table), cols
            )
            cur.copy(stmt, buf)
            cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table)))

            count = cur.fetchone()[0]
            print(f"Rows now in {table}: {count}")
            conn.commit()

def populate_fact_table():
    q = f"""
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
    JOIN {TABLE_ROUTES} r
      ON ROUND(w.latitude::numeric,  4) = ROUND(r.latitude::numeric,  4)
     AND ROUND(w.longitude::numeric, 4) = ROUND(r.longitude::numeric, 4);
    """
    with connect(DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute(q)
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(TABLE_FACT)))
        print("✅ Rows now in", TABLE_FACT, ":", cur.fetchone()[0])
        conn.commit()


def load(crag_df, cleaned_weather_df):
    """
    Loads data into PostgreSQL database.
    
    Args:
        crag_df (pd.DataFrame): DataFrame containing crag data.
        cleaned_weather_df (pd.DataFrame): DataFrame containing cleaned weather data.
    """
    
    create_schema()

    crag_df = crag_df.rename(
        columns={
            "type": "climbing_type",
            "difficulty_grade": "climbing_grade",
            "routes_count": "route_count",
        }
    )

    crag_cols = ["route_name","climbing_type","safety_grade","climbing_grade",
                 "sector_name","rocktype","longitude","latitude","route_count",
                 "country","county"]
    wx_cols   = ["date","precipitation_percentage","temperature_c",
                 "longitude","latitude","relative_humidity_percentage"]

   
    copy_dataframe(crag_df, TABLE_ROUTES, crag_cols)
    copy_dataframe(cleaned_weather_df, TABLE_WEATHER, wx_cols)

    populate_fact_table()

    print("Data loaded into PostgreSQL database successfully.")
    
