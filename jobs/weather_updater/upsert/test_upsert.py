import os
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from jobs.weather_updater.app.db import (
    ensure_schema,
    get_connection,
    load_to_staging,
    upsert_fact_window,
)

def insert_test_crag() -> str:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO public.dimcrags (crag_name, county, latitude, longitude, rocktype, climbing_style)
            VALUES ('Test Crag', 'Nowhere', 51.5007, -0.1246, 'Limestone', 'Trad')
            RETURNING crag_id::text
        """)
        return cur.fetchone()["crag_id"]

def fetch_fact_rows(crag_id: str):
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT crag_id::text, date, temperature_c, relative_humidity_percentage,
                   precipitation_mm, windspeed_ms, load_batch_id, forecast_run_ts, horizon_hours
            FROM public.fact_crag_hourly_weather
            WHERE crag_id = %(c)s
            ORDER BY date
        """, {"c": crag_id})
        return cur.fetchall()

def main():
    assert os.getenv("DATABASE_URL"), "Set DATABASE_URL"
    ensure_schema()  

    crag_id = insert_test_crag()
    batch_id = f"manualtest_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"
    batch_id2 = batch_id + "_v2"

    base = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    ts0 = base
    ts1 = base + timedelta(hours=1)

    rows = [
        {
            "date": ts0,
            "precipitation_mm": 0.2,
            "temperature_c": 12.3,
            "relative_humidity_percentage": 70.0,
            "windspeed_ms": 3.0,
            "crag_id": crag_id,
            "longitude": -0.1246,
            "latitude": 51.5007,
        },
        {
            "date": ts1,
            "precipitation_mm": 0.0,
            "temperature_c": 13.1,
            "relative_humidity_percentage": 68.0,
            "windspeed_ms": 3.5,
            "crag_id": crag_id,
            "longitude": -0.1246,
            "latitude": 51.5007,
        },
    ]

    staged = load_to_staging(rows, load_batch_id=batch_id)  
    print(f"staged: {staged}")

    inserted, deleted = upsert_fact_window(load_batch_id=batch_id, hours=int(os.getenv("WINDOW_HOURS", "2")))
    print({"first_upsert": {"inserted_or_updated": inserted, "deleted_in_window": deleted}})

    print("after first upsert:", fetch_fact_rows(crag_id))

    rows_update = [
    {**rows[0], "temperature_c": 15.8, "relative_humidity_percentage": 62.0,
     "precipitation_mm": 1.6, "windspeed_ms": 4.2},  # changed ts0
    rows[1],  # include ts1 too so it doesn't get deleted
]

    staged2 = load_to_staging(rows_update, load_batch_id=batch_id2)
    print(f"staged (update batch): {staged2}")

    upserted2, deleted2 = upsert_fact_window(load_batch_id=batch_id2, hours=2)
    print({"second_upsert": {"inserted_or_updated": upserted2, "deleted_in_window": deleted2}})

    print("after second upsert:", fetch_fact_rows(crag_id))

if __name__ == "__main__":
    main()
