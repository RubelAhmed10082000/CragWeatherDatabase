from typing import Tuple, List
import pandas as pd
import requests_cache
from retry_requests import retry
import openmeteo_requests


def fetch_weather_data_inmem(coords: List[Tuple[float, float]], max_points: int | None = None) -> pd.DataFrame:
    """
    Fetches weather data in memory. Both current and hourly weather forecast.
    Hourly weather forecast only begins after current weather forecast
    """

    cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    lat, lon = 52.52, 13.41

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "timezone": "UTC",
        "hourly": ["temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m"],
        "current": ["temperature_2m", "relative_humidity_2m", "wind_speed_10m", "precipitation"],
        "windspeed_unit": "ms",
        "precipitation_unit": "mm",
    }

    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

    current = response.Current()
    now_hour = pd.to_datetime(current.Time(), unit="s", utc=True).floor("H")
    current_row = pd.DataFrame({
        "date": [now_hour],
        "temperature_c": [current.Variables(0).Value()],          
        "relative_humidity_percentage": [current.Variables(1).Value()],  
        "wind_speed_ms": [current.Variables(2).Value()],            
        "precipitation_mm": [current.Variables(3).Value()],        
    })

   
    hourly = response.Hourly()
    hourly_dates = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left",
    )

    df_hourly_raw = pd.DataFrame({
        "date": hourly_dates,
        "temperature_c": hourly.Variables(0).ValuesAsNumpy(),
        "relative_humidity_percentage": hourly.Variables(1).ValuesAsNumpy(),
        "precipitation_mm": hourly.Variables(2).ValuesAsNumpy(),   
        "wind_speed_ms": hourly.Variables(3).ValuesAsNumpy(),      
    })

    df_hourly = df_hourly_raw.loc[df_hourly_raw["date"] > now_hour].copy()

    df = pd.concat([current_row, df_hourly], ignore_index=True)

    df["latitude"] = float(lat)
    df["longitude"] = float(lon)
    df["relative_humidity_percentage"] = pd.to_numeric(df["relative_humidity_percentage"], errors="coerce").clip(0, 100)
    df["precipitation_mm"] = pd.to_numeric(df["precipitation_mm"], errors="coerce").clip(lower=0).round(2)
    df["wind_speed_ms"] = pd.to_numeric(df["wind_speed_ms"], errors="coerce").clip(lower=0)

    return df 