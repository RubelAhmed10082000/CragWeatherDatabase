from __future__ import annotations

from dataclasses import dataclass

ICON = {
    "clear": ("clear-day.png", "Sunny"),
    "clear_night": ("clear-night.png", "Clear Night"),
    "partly_cloudy": ("partly-cloudy-day.png", "Partly Cloudy"),
    "cloudy": ("cloudy.png", "Cloudy"),
    "fog": ("fog.png", "Fog"),
    "wind": ("wind.png", "Windy"),
    "rain": ("rain.png", "Rain"),
    "snow": ("snow.png", "Snow"),
}


def _num(x):
    try:
        if x is None:
            return None
        return float(str(x).replace("%", "").strip())
    except Exception:
        return None


def _coalesce(*vals):
    for v in vals:
        if v is not None:
            return v
    return None


@dataclass
class Wx:
    temperature_c: float | None
    precipitation_mm: float | None
    relative_humidity_percentage: float | None
    windspeed_ms: float | None
    hour_24: int | None = None

    @classmethod
    def from_raw(cls, raw: dict, hour_24: int | None = None) -> Wx:
        t = _num(_coalesce(raw.get("temperature_c"), raw.get("temperature"), raw.get("temp")))
        p = _num(
            _coalesce(raw.get("precipitation_mm"), raw.get("precipitation"), raw.get("precip"))
        )
        h = _num(
            _coalesce(raw.get("relative_humidity_percentage"), raw.get("humidity"), raw.get("rh"))
        )
        w = _num(_coalesce(raw.get("windspeed_ms"), raw.get("wind")))
        return cls(t, p, h, w, hour_24)


def clamp_non_temp(w: Wx) -> Wx:
    """Clamp non-temp metrics to non-negative and humidity 0..100."""
    p = None if w.precipitation_mm is None else max(0.0, w.precipitation_mm)
    r = (
        None
        if w.relative_humidity_percentage is None
        else min(100.0, max(0.0, w.relative_humidity_percentage))
    )
    s = None if w.windspeed_ms is None else max(0.0, w.windspeed_ms)
    return Wx(w.temperature_c, p, r, s, w.hour_24)


SNOW_THRESHOLD_C = 0.0


def test_snow_guardrail():
    assert SNOW_THRESHOLD_C == 0.0


def pick_icon_and_summary(w: Wx) -> tuple[str, str]:
    tC = w.temperature_c
    pr = 0.0 if w.precipitation_mm is None else w.precipitation_mm
    rh = 0.0 if w.relative_humidity_percentage is None else w.relative_humidity_percentage
    ms = 0.0 if w.windspeed_ms is None else w.windspeed_ms

    if tC is not None and tC <= SNOW_THRESHOLD_C and pr > 0:
        return ICON["snow"]

    if pr > 0:
        label = "Heavy Rain" if pr >= 4 else "Rain" if pr >= 1 else "Light Rain"
        return (ICON["rain"][0], label)

    if rh >= 98 and ms <= 2:
        return ICON["fog"]
    if ms >= 10:
        return ICON["wind"]
    hour = w.hour_24
    if hour is not None and (hour >= 20 or hour < 6):
        return ICON["clear_night"]
    return ICON["clear"]


def format_card(w: Wx) -> dict:
    """
    Formats values for display in your card:
      temp: rounds to nearest int (can be negative)
      wind: km/h, integer
      rain: mm with 1 decimal
      hum:  integer %
    Non-temp are clamped to non-negative before formatting.
    """
    w = clamp_non_temp(w)
    icon_file, summary = pick_icon_and_summary(w)

    t_str = "N/A" if w.temperature_c is None else f"{int(round(w.temperature_c))}°C"
    kmh = None if w.windspeed_ms is None else round(w.windspeed_ms * 3.6)
    wind_str = "N/A" if kmh is None else f"{kmh} km/h"
    rmm = None if w.precipitation_mm is None else round(w.precipitation_mm, 1)
    rain_str = "N/A" if rmm is None else f"{rmm:.1f} mm"
    hum = (
        None
        if w.relative_humidity_percentage is None
        else int(round(w.relative_humidity_percentage))
    )
    hum_str = "N/A" if hum is None else f"{hum}%"

    return {
        "icon_file": icon_file,
        "summary": summary,
        "temp": t_str,
        "wind": wind_str,
        "rain": rain_str,
        "hum": hum_str,
    }
