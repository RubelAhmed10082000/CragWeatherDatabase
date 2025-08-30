from typing import List, Tuple
import pandas as pd
import requests_cache
from retry_requests import retry
import openmeteo_requests

def fetch_weather_for_crags_staging(
    crags: List[Tuple[str, float, float]],   
    load_batch_id: str,
    max_points: int | None = None
) -> pd.DataFrame:
    """
    return in-memory dataframe ready for weather stage insert
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
    now_hour = pd.Timestamp.utcnow().floor("H")
    frames: list[pd.DataFrame] = []

    for crag_id, lat, lon in crags:
        params = {
            "latitude": float(lat),
            "longitude": float(lon),
            "timezone": "UTC",
            "hourly": ["temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m"],
            "windspeed_unit": "ms",
            "precipitation_unit": "mm",
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

            df = df[df["date"] >= now_hour].copy()

            df["crag_id"] = crag_id
            df["latitude"] = float(lat)
            df["longitude"] = float(lon)
            df["load_batch_id"] = load_batch_id

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
                "wind_speed_ms","crag_id","longitude","latitude","load_batch_id"
            ]])

        except Exception as e:
            print(f"Fetch failed for ({crag_id}, {lat}, {lon}): {e}")

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=[
        "date","precipitation_mm","temperature_c","relative_humidity_percentage",
        "windspeed_ms","crag_id","longitude","latitude","load_batch_id"
    ])