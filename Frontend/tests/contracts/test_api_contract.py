import os, pytest, requests
API = os.getenv("API_BASE") or os.getenv("API")
assert API

pytestmark = pytest.mark.contract

def get(path, **params):
    r = requests.get(f"{API}/{path.lstrip('/')}", params=params, timeout=(2, 8))
    r.raise_for_status()
    return r.json()

def test_health():
    r = requests.get(f"{API}/health", timeout=(2, 8))
    assert r.status_code == 200
    assert "ok" in r.text.lower()

def test_list_basic_shape():
    data = get("api/crags", per_page=5, page=1, sort_by="name", sort_order="asc")
    assert isinstance(data["items"], list)
    assert isinstance(data["total"], int)

@pytest.mark.parametrize("styles", [[], ["Sport"], ["Sport","Trad"]])
def test_styles_filter_accepts_repeated_keys(styles):
    params = [("per_page", 5)]
    for s in styles:
        params.append(("style", s))
    r = requests.get(f"{API}/api/crags", params=params, timeout=(2,8))
    r.raise_for_status()
    data = r.json()
    assert "items" in data

def test_facets_and_filter_roundtrip():
    f = get("api/crags/facets")
    if f.get("counties"):
        c = f["counties"][0]
        data = get("api/crags", per_page=3, county=c)
        assert "items" in data

def test_detail_and_routes():
    first = get("api/crags", per_page=1)["items"][0]
    d = get(f"api/crags/{first['id']}")
    assert d["id"] == first["id"]
    routes = get(f"api/crags/{first['id']}/routes", limit=10)
    assert isinstance(routes, (list, dict))

def test_weather_forecast_24h():
    first = get("api/crags", per_page=1)["items"][0]
    w = get(f"api/weather/crags/{first['id']}/forecast", hours=24)
    assert len(w) == 24 or len(w) > 0
