from typing import Tuple, List
import pandas as pd
import requests_cache
from retry_requests import retry
import openmeteo_requests


def fetch_weather_data_inmem(coords: List[Tuple[float, float]], max_points: int | None = None) -> pd.DataFrame:
    """
    In-memory equivalent of fetch_weather_data
    """
    if max_points:
        coords = coords[:max_points]

    cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    client = openmeteo_requests.Client(session=retry_session)

    out = []
    for lat, lon in coords:
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": float(lat),
            "longitude": float(lon),
            "hourly": ["temperature_2m", "relative_humidity_2m", "precipitation_probability"],
            "wind_speed_unit": "mph",
        }
        try:
            resp = client.weather_api(url, params=params)[0]
            hourly = resp.Hourly()

            # Build hourly frame (exactly like your file-based version). :contentReference[oaicite:2]{index=2}
            df = pd.DataFrame({
                "date": pd.date_range(
                    start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                    end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                    freq=pd.Timedelta(seconds=hourly.Interval()),
                    inclusive="left",
                ),
                "temperature_2m":        hourly.Variables(0).ValuesAsNumpy(),
                "relative_humidity_2m":  hourly.Variables(1).ValuesAsNumpy(),
                "precipitation":         hourly.Variables(2).ValuesAsNumpy(),  # probability % :contentReference[oaicite:3]{index=3}
            })
            df["latitude"] = float(lat)
            df["longitude"] = float(lon)
            df["relative_humidity_2m"] = df["relative_humidity_2m"].round().clip(0, 100).astype("Int64")
            out.append(df)
        except Exception as e:
            print(f"Fetch failed for ({lat}, {lon}): {e}")

    if not out:
        return pd.DataFrame(columns=["date","temperature_2m","relative_humidity_2m","precipitation","latitude","longitude"])
    return pd.concat(out, ignore_index=True)