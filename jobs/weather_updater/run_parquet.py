import sys
import pandas as pd

from jobs.weather_updater.app.db import (
    ensure_weather_table,
    ensure_staging_exists,
    load_to_staging,
    merge_staging_into_weather,
    truncate_staging,
    count_unmatched_staging,
)


def main():
    if len(sys.argv) < 2:
        print("Usage: python -m jobs.weather_updater.run_parquet path/to/weather.parquet [dp]")
        sys.exit(1)

    parquet_path = sys.argv[1]
    dp = int(sys.argv[2]) if len(sys.argv) > 2 else 5

    ensure_weather_table()
    ensure_staging_exists()

    # Load parquet into DataFrame
    df = pd.read_parquet(parquet_path)
    if df["date"].dtype == "object":
        df["date"] = pd.to_datetime(df["date"], utc=True)

    rows = df.to_dict(orient="records")

    staged = load_to_staging(rows)
    print(f"Loaded {staged} rows into staging.")

    unmatched = count_unmatched_staging(dp=dp)
    print(f"Unmatched rows: {unmatched}")

    upserted = merge_staging_into_weather(dp=dp)
    print(f"Upserted {upserted} rows into dimhourlyweatherinfo.")

    truncate_staging()
    print("🧹 Staging table truncated.")


if __name__ == "__main__":
    main()
