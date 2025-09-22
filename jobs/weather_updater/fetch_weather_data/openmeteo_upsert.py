from typing import List, Tuple
import pandas as pd
import requests_cache
from retry_requests import retry
import openmeteo_requests
from datetime import timedelta, datetime

def fetch_weather_for_crags_staging(
    crags: List[Tuple[str, float, float]],
    load_batch_id: str,
    max_points: int | None = None,
    target_hour_utc: datetime | None = None
) -> pd.DataFrame:
    """
    Return in-memory dataframe ready for weather stage insert.
    Columns: date, precipitation_mm, temperature_c, relative_humidity_percentage,
             windspeed_ms, crag_id, longitude, latitude, load_batch_id
    """
    if max_points:
        crags = crags[:max_points]
    if not crags:
        return pd.DataFrame(columns=[
            "date","precipitation_mm","temperature_c","relative_humidity_percentage",
            "windspeed_ms","crag_id","longitude","latitude","load_batch_id"
        ])

    cache_session = requests_cache.CachedSession("/tmp/.cache", expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    client = openmeteo_requests.Client(session=retry_session)

    url = "https://api.open-meteo.com/v1/forecast"
    frames: list[pd.DataFrame] = []

    for crag_id, lat, lon in crags:
        if target_hour_utc is not None:
            day = target_hour_utc.date()
            params = {
                "latitude": float(lat),
                "longitude": float(lon),
                "timezone": "UTC",
                "hourly": ["temperature_2m", "relative_humidity_2m", "precipitation", "windspeed_10m"],
                "windspeed_unit": "ms",
                "precipitation_unit": "mm",
                "start_date": str(day),
                "end_date": str(day),
            }
        else:
            params = {
                "latitude": float(lat),
                "longitude": float(lon),
                "timezone": "UTC",
                "hourly": ["temperature_2m", "relative_humidity_2m", "precipitation", "windspeed_10m"],
                "windspeed_unit": "ms",
                "precipitation_unit": "mm",
                "past_days": 1,
                "forecast_days": 2,
            }

        try:
            resp = client.weather_api(url, params=params)[0]
            hourly = resp.Hourly()

            idx = pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left",
            )

            df = pd.DataFrame({
                "date": idx,
                "temperature_c": hourly.Variables(0).ValuesAsNumpy(),
                "relative_humidity_percentage": hourly.Variables(1).ValuesAsNumpy(),
                "precipitation_mm": hourly.Variables(2).ValuesAsNumpy(),
                "windspeed_ms": hourly.Variables(3).ValuesAsNumpy(),
            })

            if target_hour_utc is not None:
                hour = target_hour_utc.replace(minute=0, second=0, microsecond=0)
                df = df[(df["date"] >= hour) & (df["date"] < hour + timedelta(hours=1))]


            # attach ids/coords/batch
            df["crag_id"] = crag_id
            df["latitude"] = float(lat)
            df["longitude"] = float(lon)
            df["load_batch_id"] = load_batch_id

            # sanitize ranges / types
            df["relative_humidity_percentage"] = (
                pd.to_numeric(df["relative_humidity_percentage"], errors="coerce").clip(0, 100)
            )
            df["precipitation_mm"] = (
                pd.to_numeric(df["precipitation_mm"], errors="coerce").clip(lower=0).round(2)
            )
            df["windspeed_ms"] = pd.to_numeric(df["windspeed_ms"], errors="coerce").clip(lower=0)
            df["temperature_c"] = pd.to_numeric(df["temperature_c"], errors="coerce")

            frames.append(df[[
                "date","precipitation_mm","temperature_c","relative_humidity_percentage",
                "windspeed_ms","crag_id","longitude","latitude","load_batch_id"  
            ]])

        except Exception as e:
            print(f"Fetch failed for ({crag_id}, {lat}, {lon}): {e}")

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=[
        "date","precipitation_mm","temperature_c","relative_humidity_percentage",
        "windspeed_ms","crag_id","longitude","latitude","load_batch_id"
    ])
