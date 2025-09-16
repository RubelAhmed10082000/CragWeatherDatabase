import os
from typing import Any

import pandas as pd
import pytest
from starlette.testclient import TestClient

from arquivio.api import main as api_main

os.environ.setdefault("SANITY_MODE", "1")


@pytest.fixture
def client(app):
    return TestClient(app)


def test_root_and_health(client):
    r = client.get("/api")
    assert r.status_code == 200 and r.json().get("status") == "ok"
    r = client.get("/health")
    assert r.status_code == 200 and r.json() == {"status": "ok"}


def test_list_crags_sort_paging(monkeypatch, client):
    from types import SimpleNamespace

    import arquivio.api.main as api_main

    rows = [
        {
            "id": "c1",
            "name": "Beta",
            "county": "A",
            "rocktype": "Limestone",
            "climbing_style": "Sport",
        },
        {
            "id": "c2",
            "name": "Alpha",
            "county": "B",
            "rocktype": "Gritstone",
            "climbing_style": "Trad",
        },
        {
            "id": "c3",
            "name": "Delta",
            "county": "C",
            "rocktype": "Granite",
            "climbing_style": "Bouldering",
        },
    ]

    def fake_search_crags(query=None, filters=None):
        return pd.DataFrame(rows)

    route = next(r for r in client.app.routes if getattr(r, "path", None) == "/api/crags")
    endpoint = route.endpoint
    fake_db = SimpleNamespace(search_crags=fake_search_crags)
    monkeypatch.setitem(endpoint.__globals__, "db", fake_db)
    monkeypatch.setattr(api_main, "db", fake_db, raising=True)

    r = client.get("/api/crags?page=1&per_page=2&sort_by=name&sort_order=asc")
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 3 and len(data["items"]) == 2
    assert [x["name"] for x in data["items"]] == ["Alpha", "Beta"]

    r2 = client.get("/api/crags?page=2&per_page=2&sort_by=name&sort_order=asc")
    assert r2.status_code == 200
    assert [x["name"] for x in r2.json()["items"]] == ["Delta"]

    r3 = client.get("/api/crags?page=0")
    assert r3.status_code == 422


def test_list_crags_empty(monkeypatch, client):
    from types import SimpleNamespace

    import arquivio.api.main as api_main

    def fake_search_crags(query=None, filters=None):
        return pd.DataFrame(columns=["id", "name"])

    route = next(r for r in client.app.routes if getattr(r, "path", None) == "/api/crags")
    endpoint = route.endpoint
    fake_db = SimpleNamespace(search_crags=fake_search_crags)

    monkeypatch.setitem(endpoint.__globals__, "db", fake_db)
    monkeypatch.setattr(api_main, "db", fake_db, raising=True)

    r = client.get("/api/crags")
    assert r.status_code == 200
    assert r.json() == {"items": [], "total": 0, "page": 1, "per_page": 25}


def test_list_crags_filters_merging(monkeypatch, client):
    from types import SimpleNamespace

    import arquivio.api.main as api_main

    captured = {"query": None, "filters": None}

    def fake_search_crags(query=None, filters=None):
        captured["query"] = query
        captured["filters"] = filters or {}
        return pd.DataFrame([{"id": "x", "name": "X"}])

    route = next(r for r in client.app.routes if getattr(r, "path", None) == "/api/crags")
    endpoint = route.endpoint
    fake_db = SimpleNamespace(search_crags=fake_search_crags)
    monkeypatch.setitem(endpoint.__globals__, "db", fake_db)
    monkeypatch.setattr(api_main, "db", fake_db, raising=True)

    r = client.get(
        "/api/crags?"
        "q=stanage&style=Sport&style=Trad&climbing_style=Bouldering&"
        "county=Derbyshire&rocktype=Gritstone"
    )
    assert r.status_code == 200

    assert captured["query"] == "stanage"
    assert set(captured["filters"]["climbing_style"]) == {"Sport", "Trad", "Bouldering"}
    assert captured["filters"]["county"] == ["Derbyshire"]
    assert captured["filters"]["rocktype"] == ["Gritstone"]


def test_crag_detail_success_and_routes(monkeypatch, client):
    from types import SimpleNamespace

    crag = {
        "id": "abc",
        "name": "Test Crag",
        "routes": [{"name": "A"}, {"name": "B"}, {"name": "C"}],
    }

    fake_db = SimpleNamespace(
        get_crag_with_routes=lambda crag_id: crag if crag_id == "abc" else None
    )

    def bind_db_to_paths(paths):
        for path in paths:
            route = next(r for r in client.app.routes if getattr(r, "path", None) == path)
            monkeypatch.setitem(route.endpoint.__globals__, "db", fake_db)

    bind_db_to_paths(("/api/crags/{crag_id}", "/api/crags/{crag_id}/routes"))

    monkeypatch.setattr(api_main, "db", fake_db, raising=True)

    r = client.get("/api/crags/abc")
    assert r.status_code == 200
    assert r.json()["name"] == "Test Crag"

    r2 = client.get("/api/crags/abc/routes?limit=2&offset=1")
    assert r2.status_code == 200
    assert r2.json() == [{"name": "B"}, {"name": "C"}]

    r3 = client.get("/api/crags/does-not-exist")
    assert r3.status_code == 404


def test_forecast_by_crag_ok_and_404(monkeypatch, client):
    from types import SimpleNamespace

    import arquivio.api.main as api_main

    route = next(
        r
        for r in client.app.routes
        if getattr(r, "path", None) == "/api/weather/crags/{crag_id}/forecast"
    )
    endpoint = route.endpoint

    def use_df(df):
        """Force the route to use a fake db that returns `df`."""
        fake = SimpleNamespace(get_forecast=lambda crag_id, hours=168: df)
        monkeypatch.setitem(endpoint.__globals__, "db", fake)
        monkeypatch.setattr(api_main, "db", fake, raising=True)

    use_df(pd.DataFrame([{"timestamp": "2025-09-15T00:00:00Z", "temp": 12.3}]))
    r = client.get("/api/weather/crags/abc/forecast?hours=24")
    assert r.status_code == 200
    assert isinstance(r.json(), list) and r.json()[0]["timestamp"]

    use_df(pd.DataFrame())
    r2 = client.get("/api/weather/crags/abc/forecast?hours=25")
    assert r2.status_code == 404

    assert client.get("/api/weather/crags/abc/forecast?hours=0").status_code == 422
    assert client.get("/api/weather/crags/abc/forecast?hours=999").status_code == 422


class _DummyResult:
    def __init__(self, row: dict[str, Any] | None):
        self._row = row

    def mappings(self):
        return self

    def first(self):
        return self._row

    def scalar(self):
        return None


class _DummyConn:
    def __init__(self, row: dict[str, Any] | None):
        self._row = row

    def execute(self, *_args, **_kwargs):
        return _DummyResult(self._row)


def test_weather_by_coords_ok_and_404(monkeypatch, client):
    from types import SimpleNamespace

    route = next(
        r for r in client.app.routes if getattr(r, "path", None) == "/api/weather/{lat}/{lon}"
    )
    endpoint = route.endpoint

    class _DummyResult:
        def __init__(self, row):
            self._row = row

        def mappings(self):
            return self

        def first(self):
            return self._row

    class _DummyConn:
        def __init__(self, row):
            self._row = row

        def execute(self, *_a, **_k):
            return _DummyResult(self._row)

    class _ConnCtx:
        def __init__(self, row):
            self._row = row

        def __enter__(self):
            return _DummyConn(self._row)

        def __exit__(self, exc_type, exc, tb):
            return False

    class _DummyEngine:
        def __init__(self, row):
            self._row = row

        def connect(self):
            return _ConnCtx(self._row)

    def use_fake(row_from_lookup, next_point_dict):
        fake_db = SimpleNamespace(
            T_CRAGS="crags",
            engine=_DummyEngine(row_from_lookup),
            get_next_forecast_point=lambda crag_id, hours=168: next_point_dict,
        )
        monkeypatch.setitem(endpoint.__globals__, "db", fake_db)
        monkeypatch.setattr(api_main, "db", fake_db, raising=True)

    use_fake(
        {"crag_id": "abc"},
        {
            "temp": 11.1,
            "humidity": 80.0,
            "precip": 0.2,
            "wind": 4.5,
            "timestamp": "2025-09-15T10:00:00Z",
        },
    )
    r = client.get("/api/weather/53.1/-1.7")
    assert r.status_code == 200
    body = r.json()
    assert set(body.keys()) == {"temperature", "humidity", "precipitation", "wind", "timestamp"}

    use_fake(None, None)
    r2 = client.get("/api/weather/0/0")
    assert r2.status_code == 404

    use_fake({"crag_id": "abc"}, None)
    r3 = client.get("/api/weather/53.1")
    assert r3.status_code == 404


def test_cors_allows_default_origin(client):
    r = client.get("/api", headers={"Origin": "http://localhost:5000"})
    assert r.headers.get("access-control-allow-origin") == "http://localhost:5000"
