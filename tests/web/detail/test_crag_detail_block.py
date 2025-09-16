import math

from bs4 import BeautifulSoup
from jinja2 import Environment, Template


def fmt_unknown(value):
    if value is None:
        return "Unknown"
    if isinstance(value, float) and math.isnan(value):
        return "Unknown"
    s = str(value).strip()
    return "Unknown" if s.lower() in {"", "<na>", "na", "n/a", "none", "null", "nan"} else s


BLOCK = r"""
</style>
</head>
<body>
    <div class="container">
        <a href="/" class="back-link">← Back to Crags List</a>

        <div class="crag-header">
            <h1>{{ crag.name }}</h1>
            <p>{{ crag.country }}</p>
        </div>

        <div class="crag-grid">
            <div class="main-content">
                <div class="card">
                    <h2 class="card-title">Location & Information</h2>
                    <div class="info-grid">
                        <div class="info-item">
                            <div class="info-item-label">Rock Type</div>
                            <div class="info-item-value">{{ crag.rocktype | fmt }}</div>
                        </div>
                        <div class="info-item">
                            <div class="info-item-label">County</div>
                            <div class="info-item-value">{{ crag.county | fmt }}</div>
                        </div>
                        <div class="info-item">
                            <div class="info-item-label">Coordinates</div>
                            <div class="info-item-value">{{ crag.latitude }}, {{ crag.longitude }}</div>
                        </div>
                    </div>

                    <h3 class="card-title">Location Map</h3>
                    <div id="map" class="map-container"></div>

                    {% if crag.description %}
                    <h3 class="card-title">About this Crag</h3>
                    <p>{{ crag.description }}</p>
                    {% endif %}
                </div>

                {% if crag.routes %}
                <div class="card" style="margin-top: 2rem;">
                    <h2 class="card-title">Routes ({{ crag.routes|length }})</h2>
                    <div class="routes-controls">
                        <div class="routes-per-page">
                            <label for="routesPerPage">Routes per page:</label>
                            <select id="routesPerPage">
                                <option value="10">10</option>
                                <option value="25">25</option>
                                <option value="50">50</option>
                                <option value="100">100</option>
                            </select>
                        </div>
                        <div class="routes-search">
                            <input type="text" id="routeSearch" placeholder="Search routes...">
                        </div>
                    </div>
                    <ul class="routes-list" id="routesList">
                        {% for route in crag.routes %}
                        <li class="route-item">
                        <div class="route-name">
                            {{ route.get('name') or route.get('route_name') or 'Unnamed route' }}

                            <span class="grade">
                            {{ (route.get('difficulty') or route.get('grade') or route.get('difficulty')) | fmt }}
                            </span>

                            {% if route.get('safety') %}
                            <span class="grade">{{ route.get('safety') | upper }}</span>
                            {% endif %}
                        </div>

                        <div class="route-info">
                            {% if route.get('type') %}
                            <span class="route-type">{{ route.get('type') }}</span>
                            {% endif %}
                        </div>
                        </li>
                        {% endfor %}
                    </ul>
                    <div class="routes-pagination">
                        <button id="prevPage" class="pagination-btn" disabled>&larr; Previous</button>
                        <span id="pageInfo">Page <span id="currentPage">1</span> of <span id="totalPages">1</span></span>
                        <button id="nextPage" class="pagination-btn" disabled>Next &rarr;</button>
                    </div>
                </div>
                {% else %}
                <div class="card" style="margin-top: 2rem;">
                    <h2 class="card-title">Routes</h2>
                    <p class="no-routes">No routes available for this crag.</p>
                </div>
                {% endif %}
            </div>

            <div class="sidebar">
                <div class="card">
                    <h2 class="card-title">Current Weather</h2>
                    {% if weather %}
                    <div>
                        <span class="weather-badge weather-temp">{{ weather.temperature }}°C</span>
                        <span class="weather-badge weather-humidity">{{ weather.humidity }}% Humidity</span>
                        <span class="weather-badge weather-precip">{{ weather.precipitation }}mm Precipitation</span>
                    </div>
                    {% else %}
                    <p>Weather information unavailable</p>
                    {% endif %}
                </div>

                {% if crag.access %}
                <div class="card" style="margin-top: 2rem;">
                    <h2 class="card-title">Access Information</h2>
                    <p>{{ crag.access }}</p>
                </div>
                {% endif %}
            </div>
        </div>
    </div>
"""


def _render(crag, weather=None):
    env = Environment(autoescape=False, trim_blocks=True, lstrip_blocks=True)
    env.filters["fmt"] = fmt_unknown
    tmpl: Template = env.from_string(BLOCK)
    html = tmpl.render(crag=crag, weather=weather)
    return BeautifulSoup(html, "html.parser")


def test_header_and_info_uses_fmt_and_coords():
    crag = {
        "name": "Grit Edge",
        "country": "UK",
        "rocktype": None,
        "county": "Derbyshire",
        "latitude": 53.1,
        "longitude": -1.7,
        "routes": [],
    }

    soup = _render(crag)
    assert soup.select_one(".crag-header h1").get_text(strip=True) == "Grit Edge"
    assert soup.select_one(".crag-header p").get_text(strip=True) == "UK"
    values = [el.get_text(strip=True) for el in soup.select(".info-item-value")]
    assert values[0] == "Unknown"
    assert "Derbyshire" in values[1]
    assert "53.1" in values[2] and "-1.7" in values[2]
    assert soup.select_one("#map") is not None


def test_routes_difficulty_always_safety_only_when_present():
    crag = {
        "name": "Millstone",
        "country": "UK",
        "rocktype": "Gritstone",
        "county": "Derbyshire",
        "latitude": 53.3,
        "longitude": -1.62,
        "routes": [
            {"name": "Embankment 3", "difficulty": "E1", "safety": "PG", "type": "Trad"},
            {"name": "Boulder Arete", "difficulty": "6B", "safety": None, "type": "Bouldering"},
            {"name": "Mystery Slab", "difficulty": None, "safety": None, "type": "Sport"},
        ],
    }
    soup = _render(crag)
    items = soup.select("ul#routesList li.route-item")
    assert len(items) == 3

    g1 = items[0].select(".grade")
    assert [s.get_text(strip=True) for s in g1] == ["E1", "PG"]
    assert items[0].select_one(".route-type").get_text(strip=True) == "Trad"

    g2 = items[1].select(".grade")
    assert len(g2) == 1
    assert g2[0].get_text(strip=True) == "6B"
    assert items[1].select_one(".route-type").get_text(strip=True) == "Bouldering"

    g3 = items[2].select(".grade")
    assert len(g3) == 1
    assert g3[0].get_text(strip=True) == "Unknown"


def test_no_routes_branch():
    crag = {
        "name": "Stanage",
        "country": "UK",
        "rocktype": "Gritstone",
        "county": "Derbyshire",
        "latitude": 53.4,
        "longitude": -1.63,
        "routes": [],
    }
    soup = _render(crag)

    assert soup.select_one("#routesList") is None
    assert soup.select_one(".routes-controls") is None
    assert soup.find(string=lambda t: t and "No routes available for this crag." in t)


def test_weather_present_vs_missing_and_pagination_defaults():
    crag = {
        "name": "Grit Edge",
        "country": "UK",
        "rocktype": "Gritstone",
        "county": "Derbyshire",
        "latitude": 53.1,
        "longitude": -1.7,
        "routes": [{"name": "Test", "difficulty": "VS", "safety": None, "type": "Trad"}],
    }
    soup = _render(crag, weather={"temperature": 12, "humidity": 80, "precipitation": 0.2})
    text = " ".join(x.get_text(" ", strip=True) for x in soup.select(".weather-badge"))
    assert "°C" in text and "Humidity" in text and "Precipitation" in text

    assert soup.select_one("#prevPage").has_attr("disabled")
    assert soup.select_one("#nextPage").has_attr("disabled")

    opts = [o.get_text(strip=True) for o in soup.select("#routesPerPage option")]
    assert opts == ["10", "25", "50", "100"]


def test_weather_missing_shows_message():
    crag = {
        "name": "Grit Edge",
        "country": "UK",
        "rocktype": "Gritstone",
        "county": "Derbyshire",
        "latitude": 53.1,
        "longitude": -1.7,
        "routes": [],
    }
    soup = _render(crag, weather=None)
    assert soup.find(string=lambda t: t and "Weather information unavailable" in t)
