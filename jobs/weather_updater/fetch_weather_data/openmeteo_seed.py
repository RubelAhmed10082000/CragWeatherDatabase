from datetime import datetime, timezone, timedelta
from typing import Iterable, Dict, Tuple, List, Any
import httpx

# Setting column variables
HOURLY_VARS = "temperature_2m,relative_humidity_2m,precipitation,windspeed_10m"

def build_url(lat: float, lon:float, start:datetime, hours:int) -> str:
    """
    Building request that well call weather data
    """
    return (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        f"&hourly={HOURLY_VARS}"
        "&timezone=UTC"
        "&timeformat=unixtime"
        "&windspeed_unit=ms"
        "&forecast_days=7"
    )

def rows_from_response(crag_id: str, lat: float, lon:float,
                        t0: datetime, hours:int, data:dict) -> Iterable[Dict[str, Any]]:
    """
    Builds a dict row object from raw OpenMeteo JSON response
    Which contains weather hourly 7-day data for each crag
    Floored to the nearest hour

    """
    h = data.get("hourly") or {}
    times = h.get("time") or []
    temp = h.get("temperature_2m") or []
    rh   = h.get("relative_humidity_2m") or []
    pr   = h.get("precipitation") or []
    ws   = h.get("windspeed_10m") or []
    if not times:
        return []
    
    t0_ts = int(t0.timestamp())
    t_end_ts = int((t0 + timedelta(hours=hours)).timestamp())

    out = []

    for i, t in enumerate(times):
        if not isinstance(t, (int,float)):
            continue
        if t < t0_ts or t >= t_end_ts:
            continue
        out.append({
            "date": datetime.fromtimestamp(t, tz=timezone.utc),
            "precipitation_mm": float(pr[i]) if i < len(pr) and pr[i] is not None else None,
            "temperature_c": float(temp[i]) if i < len(temp) and temp[i] is not None else None,
            "relative_humidity_percentage": float(rh[i]) if i < len(rh) and rh[i] is not None else None,
            "windspeed_ms": float(ws[i]) if i < len(ws) and ws[i] is not None else None,
            "crag_id": crag_id,
            "longitude": float(lon),
            "latitude": float(lat),
        })
    return out

