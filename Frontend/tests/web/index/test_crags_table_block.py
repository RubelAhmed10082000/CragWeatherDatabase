import pathlib
import re
import sys

import pytest
from bs4 import BeautifulSoup
from jinja2 import Environment

repo_root = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(repo_root))

from src.arquivio.web.app import fmt_unknown, timeago


def test_fmt_unknown():
    assert fmt_unknown(None) == "Unknown"
    assert fmt_unknown("<NA>") == "Unknown"
    assert fmt_unknown("NA") == "Unknown"
    assert fmt_unknown("N/A") == "Unknown"
    assert fmt_unknown("None") == "Unknown"
    assert fmt_unknown("Yorkshire") == "Yorkshire"
    assert fmt_unknown(0) == "0"


BLOCK = r"""
<div class="table-container">
    <table>
        <thead>
            <tr>
                <th>
                    <div class="sort-header {% if sort_by == 'name' %}active {% if sort_order == 'desc' %}desc{% endif %}{% endif %}" 
                         data-sort="name">
                        Name
                    </div>
                </th>
                <th>
                    <div class="sort-header {% if sort_by == 'country' %}active {% if sort_order == 'desc' %}desc{% endif %}{% endif %}" 
                         data-sort="country">
                        Location
                    </div>
                </th>
                <th>
                    <div class="sort-header {% if sort_by == 'rocktype' %}active {% if sort_order == 'desc' %}desc{% endif %}{% endif %}" 
                         data-sort="rocktype">
                        Rock Type
                    </div>
                </th>
                <th>
                    <div class="sort-header {% if sort_by == 'climbing_style' %}active {% if sort_order == 'desc' %}desc{% endif %}{% endif %}"
                    data-sort="climbing_style">
                    Climbing Style
                    </div>
                </th>
                <th>
                    <div class="sort-header {% if sort_by == 'routes' %}active {% if sort_order == 'desc' %}desc{% endif %}{% endif %}" 
                         data-sort="routes">
                        Routes
                    </div>
                </th>
                <th>Weather</th>
                <th>Last Rained</th>
            </tr>
        </thead>
        <tbody>
            {% for crag in crags %}
            <tr data-lat="{{ crag.latitude }}" data-lon="{{ crag.longitude }}">
                <td>
                    <a href="/crag/{{ crag.id }}" class="crag-link">
                        <strong>{{ crag.name | fmt }}</strong>
                        <div class="view-details">View details →</div>
                    </a>
                </td>
                <td>
                    {{ crag.county | fmt}}<br>
                    <span class="text-secondary">{{ crag.latitude }}, {{ crag.longitude }}</span>
                </td>
                {% set _raw = crag.rocktype %}
                {% set _rt = (_raw ~ '') | trim %}
                <td>{{ 'Unknown' if (not _rt or _rt|lower in ['<na>','na','none','null','nan']) else _rt }}</td>
                <td>{{ crag.climbing_style | fmt }}</td>
                <td>
                    <span class="route-count">{{ crag.routes_count }} routes</span>
                </td>
                <td class="weather-data">
                    <div class="weather-cell">
                        {% if crag.weather and crag.weather.temperature is not none %}
                            <span class="badge badge-temp">{{ crag.weather.temperature }}°C</span>
                            <span class="badge badge-humidity">{{ crag.weather.humidity }}%</span>
                            <span class="badge badge-precip">{{ crag.weather.precipitation }}mm</span>
                        {% else %}
                            <span class="weather-loading">Loading weather data...</span>
                        {% endif %}
                    </div>
                </td>
                <td class="last-rained">
                {% if crag.last_rained_ts %}
                    {{ crag.last_rained_ts | timeago }}{% if crag.last_rain_severity %} ({{ crag.last_rain_severity|title }}){% endif %}
                {% else %}—{% endif %}
                </td>
            </tr>
            {% endfor %}
        </tbody>
    </table>
</div>
"""


def render(crags, sort_by="name", sort_order="asc"):
    env = Environment(autoescape=False, trim_blocks=True, lstrip_blocks=True)
    env.filters["fmt"] = fmt_unknown
    env.filters["timeago"] = timeago
    tmpl = env.from_string(BLOCK)
    html = tmpl.render(crags=crags, sort_by=sort_by, sort_order=sort_order)
    return BeautifulSoup(html, "html.parser")


def test_headers_and_order():
    soup = render([])
    headers = [th.get_text(strip=True) for th in soup.select("thead th")]
    assert headers == [
        "Name",
        "Location",
        "Rock Type",
        "Climbing Style",
        "Routes",
        "Weather",
        "Last Rained",
    ]


def test_row_alignment_and_unknown_fallbacks():
    crags = [
        {
            "id": "c1",
            "name": "Grit Edge",
            "latitude": 53.1,
            "longitude": -1.7,
            "county": None,
            "rocktype": "  <NA>  ",
            "climbing_style": None,
            "routes_count": 42,
            "weather": {"temperature": 12, "humidity": 80, "precipitation": 0.2},
            "last_rained_ts": "2025-09-07T08:00:00Z",
            "last_rain_severity": "light",
        }
    ]
    soup = render(crags)
    row = soup.select_one("tbody tr")
    cells = [td.get_text(" ", strip=True) for td in row.find_all("td")]
    assert len(cells) == 7
    assert "Grit Edge" in cells[0]
    assert "Unknown" in cells[1]
    assert cells[2] == "Unknown"
    assert cells[3] == "Unknown"
    assert "42" in cells[4] and "routes" in cells[4]
    assert "°C" in cells[5] and "%" in cells[5] and "mm" in cells[5]
    text = cells[6]
    assert re.search(r"\b\d+\s+(minute|hour|day|week|month|year)s?\s+ago\b", text)
    assert "(Light)" in text


@pytest.mark.parametrize("rocktype_val", ["<NA>", "na", "None", "null", "nan", ""])
def test_rocktype_unknown_variants(rocktype_val):
    crags = [
        {
            "id": "c2",
            "name": "Test",
            "latitude": 0,
            "longitude": 0,
            "county": "Derbyshire",
            "rocktype": rocktype_val,
            "climbing_style": "Sport",
            "routes_count": 1,
            "weather": None,
            "last_rained_ts": None,
            "last_rain_severity": None,
        }
    ]
    soup = render(crags)
    row = soup.select_one("tbody tr")
    cells = [td.get_text(" ", strip=True) for td in row.find_all("td")]
    assert cells[2] == "Unknown"
    assert "Loading weather data..." in cells[5]
    assert cells[6] == "—"


def test_sort_header_classes_toggle():
    soup = render([], sort_by="climbing_style", sort_order="desc")
    hdr = soup.select_one('thead [data-sort="climbing_style"]')
    classes = set(hdr["class"])
    assert "sort-header" in classes
    assert "active" in classes
    assert "desc" in classes

    name_hdr = soup.select_one('thead [data-sort="name"]')
    assert "active" not in set(name_hdr["class"])
