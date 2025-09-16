import importlib
from fastapi.testclient import TestClient

def test_health_and_crags_listing():
    mod = importlib.import_module("arquivio.api.main")
    client = TestClient(mod.app)

    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()

    r = client.get("/api/crags?per_page=1")
    assert r.status_code == 200
    data = r.json()
    assert "items" in data

def test_forecast_ok_or_404():
    mod = importlib.import_module("arquivio.api.main")
    client = TestClient(mod.app)

    crag = "045b438f-7029-4eb6-af1f-fa75eee6d4db"
    r = client.get(f"/api/weather/crags/{crag}/forecast?hours=24")
    assert r.status_code in (200, 404)
    if r.status_code == 200:
        body = r.json()
        assert isinstance(body, list)
        if body:  
            sample = body[0]
            assert {"timestamp","temp","humidity","precip","wind"} <= set(sample.keys())
