import pytest

from tests.web.index.wx import ICON, Wx, pick_icon_and_summary


def _wx(hour=None, **raw):
    """Small helper to build Wx with an optional hour_24 override."""
    return Wx.from_raw(raw, hour_24=hour)


@pytest.mark.parametrize(
    "raw, expected",
    [
        ({"temperature_c": 0.0, "precipitation_mm": 0.1}, ICON["snow"]),
        ({"temperature_c": -3.0, "precipitation_mm": 2.0}, ICON["snow"]),
        ({"temperature_c": 6.0, "precipitation_mm": 0.2}, ("rain.png", "Light Rain")),
        ({"temperature_c": 9.0, "precipitation_mm": 1.0}, ("rain.png", "Rain")),
        ({"temperature_c": 9.0, "precipitation_mm": 4.0}, ("rain.png", "Heavy Rain")),
        ({"relative_humidity_percentage": 99, "windspeed_ms": 1.5}, ICON["fog"]),
        ({"relative_humidity_percentage": 98, "windspeed_ms": 2.0}, ICON["fog"]),
        ({"windspeed_ms": 10.0}, ICON["wind"]),
        ({"windspeed_ms": 12.3}, ICON["wind"]),
        ({"precipitation_mm": 0.0}, ICON["clear"]),
        ({"precipitation_mm": 0.0}, ICON["clear"]),
    ],
)
def test_basic_labels(raw, expected):
    w = _wx(**raw)
    file_, label = pick_icon_and_summary(w)
    if isinstance(expected, tuple):
        assert (file_, label) == expected
    else:
        assert (file_, label) == expected


@pytest.mark.parametrize(
    "hour, expected",
    [
        (22, ICON["clear_night"]),
        (0, ICON["clear_night"]),
        (5, ICON["clear_night"]),
        (6, ICON["clear"]),
        (14, ICON["clear"]),
        (20, ICON["clear_night"]),
    ],
)
def test_day_vs_night_clear(hour, expected):
    w = _wx(hour, precipitation_mm=0.0, relative_humidity_percentage=60, windspeed_ms=3)
    assert pick_icon_and_summary(w) == expected


def test_precedence_rain_over_fog_and_wind():
    w = _wx(
        2, temperature_c=3, precipitation_mm=0.5, relative_humidity_percentage=100, windspeed_ms=12
    )
    file_, label = pick_icon_and_summary(w)
    assert file_ == ICON["rain"][0]
    assert label == "Light Rain"


def test_precedence_snow_over_wind():
    w = _wx(10, temperature_c=-1, precipitation_mm=2.0, windspeed_ms=15)
    assert pick_icon_and_summary(w) == ICON["snow"]


@pytest.mark.parametrize(
    "mm, expected",
    [
        (0.0001, "Light Rain"),
        (0.9999, "Light Rain"),
        (1.0, "Rain"),
        (3.99, "Rain"),
        (4.0, "Heavy Rain"),
        (8.7, "Heavy Rain"),
    ],
)
def test_rain_tier_boundaries(mm, expected):
    file_, label = pick_icon_and_summary(_wx(12, temperature_c=8, precipitation_mm=mm))
    assert file_ == ICON["rain"][0]
    assert label == expected
