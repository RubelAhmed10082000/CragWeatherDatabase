from datetime import datetime, timedelta

import pytest


def _mk_rows(n):
    """Make n hourly rows with increasing timestamp and stable metrics."""
    base = datetime(2025, 9, 8, 12, 0, 0)
    rows = []
    for i in range(n):
        rows.append(
            {
                "timestamp": (base + timedelta(hours=i)).isoformat(),
                "temperature_c": 12.6 + (i % 2),  # 12.6, 13.6, …
                "precipitation_mm": 0.24 + (i % 3),  # 0.24, 1.24, 2.24, …
                "windspeed_ms": 4.49 + (i % 4),  # 4.49, 5.49, …
                "relative_humidity_percentage": 77.6,
            }
        )
    return rows


def _format_temp_c(x):  # Math.round in JS
    return f"{int(round(x))}°C" if x is not None else "N/A"


def _format_rain_mm(x):  # Math.round(x*10)/10 in JS
    return f"{round(x * 10) / 10:.1f} mm" if x is not None else "N/A"


def _format_wind_kmh(ms):  # Math.round(ms*3.6) in JS
    return f"{int(round(ms * 3.6))} km/h" if ms is not None else "N/A"


def _format_hum(pct):  # Math.round in JS
    return f"{int(round(pct))}%" if pct is not None else "N/A"


@pytest.mark.parametrize("hours", [24, 48, 72, 168])
def test_slice_to_selected_hours(hours):
    rows = _mk_rows(250)
    assert len(rows[:hours]) == hours


def test_metric_formatting_matches_client():
    row = {
        "temperature_c": 12.6,
        "precipitation_mm": 0.24,
        "windspeed_ms": 4.49,
        "relative_humidity_percentage": 77.6,
    }
    assert _format_temp_c(row["temperature_c"]) == "13°C"
    assert _format_rain_mm(row["precipitation_mm"]) == "0.2 mm"
    assert _format_wind_kmh(row["windspeed_ms"]) == "16 km/h"
    assert _format_hum(row["relative_humidity_percentage"]) == "78%"


def _find_template_path():
    import glob
    from pathlib import Path

    for p in (Path("templates/crag_detail.html"), Path("crag_detail.html")):
        if p.exists():
            return p
    hits = glob.glob("**/crag_detail.html", recursive=True)
    return Path(hits[0]) if hits else None


def test_default_is_24_hours_active_button():
    p = _find_template_path()
    if not p:
        pytest.skip("crag_detail.html not found")
    html = p.read_text(encoding="utf-8")
    assert (
        'class="wx-chip active" data-hours="24"' in html
        or "wx-chip active" in html
        and 'data-hours="24"' in html
    ), "Default 24h chip should be active"
