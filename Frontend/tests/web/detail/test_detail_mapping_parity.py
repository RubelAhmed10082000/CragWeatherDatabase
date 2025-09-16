import pytest

from tests.web.index.wx import ICON, Wx, pick_icon_and_summary


def _wx(hour=None, **raw):
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
        ({"windspeed_ms": 12.3}, ICON["wind"]),
        ({"precipitation_mm": 0.0}, ICON["clear"]),
    ],
)
def test_detail_icon_labels_match_index_mapping(raw, expected):
    assert pick_icon_and_summary(_wx(**raw)) == expected


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
def test_detail_day_night_rules_match_index(hour, expected):
    w = _wx(hour, precipitation_mm=0.0, relative_humidity_percentage=60, windspeed_ms=3)
    assert pick_icon_and_summary(w) == expected
