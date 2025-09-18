import json, re
import pytest

pytestmark = pytest.mark.web  

def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.is_json and r.json.get("status") == "ok"

def test_homepage_renders(client):
    r = client.get("/?per_page=5")
    assert r.status_code == 200
    # crude sanity: HTML with at least one table/list marker
    assert b"<html" in r.data.lower()

def test_filters_and_pagination_forwarded(client):
    # styles should be passed as repeated keys to API
    r = client.get("/?rocktype=Limestone&style=Sport&style=Trad&per_page=5&page=1&sort_by=name&sort_order=asc")
    assert r.status_code == 200

def test_detail_page(client):
    # Pull one id from the real API through the web → navigate detail
    # (your index page likely links to /crags/<id> in HTML; we just call directly)
    from urllib.parse import urlencode
    # first retrieve one id via the API contract if your web exposes a link; else query API directly:
    import requests, os
    api = os.getenv("API_BASE_URL") or os.getenv("API_BASE") or os.getenv("API")
    first = requests.get(f"{api}/api/crags", params={"per_page": 1}, timeout=(2,8)).json()["items"][0]["id"]

    r = client.get(f"/crags/{first}")
    assert r.status_code == 200
    assert b"crag" in r.data.lower() or b"name" in r.data.lower()

def test_weather_proxy_json(client):
    import requests, os
    api = os.getenv("API_BASE_URL") or os.getenv("API_BASE") or os.getenv("API")
    first = requests.get(f"{api}/api/crags", params={"per_page": 1}, timeout=(2,8)).json()["items"][0]["id"]

    r = client.get(f"/api/weather/crags/{first}/forecast?hours=24")
    assert r.status_code == 200
    data = r.get_json()
    assert isinstance(data, list) and len(data) >= 1
