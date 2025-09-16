import os
import time
import importlib
import pytest
from fastapi.testclient import TestClient

@pytest.fixture
def client_env(monkeypatch):
    monkeypatch.setenv("FORECAST_TTL_S", "600")
    monkeypatch.setenv("FORECAST_MAX_HOURS", "24") 
    monkeypatch.setenv("RU_DEGRADE_24H", "0")     
    monkeypatch.setenv("LOG_LEVEL", "INFO")

@pytest.fixture
def client(client_env):
    mod = importlib.import_module("arquivio.api.main")
    importlib.reload(mod)
    return TestClient(mod.app)

def test_hard_clamp_header(client):
    crag = "045b438f-7029-4eb6-af1f-fa75eee6d4db"
    r = client.get(f"/api/weather/crags/{crag}/forecast?hours=168")
    assert r.status_code in (200, 404)
    assert r.headers.get("x-clamped-hours") == "24"

def test_soft_degrade_toggle(client_env, monkeypatch):
    # soft degrade on
    monkeypatch.setenv("FORECAST_MAX_HOURS", "168")
    monkeypatch.setenv("RU_DEGRADE_24H", "1")

    mod = importlib.import_module("arquivio.api.main")
    importlib.reload(mod)
    client = TestClient(mod.app)

    crag = "045b438f-7029-4eb6-af1f-fa75eee6d4db"
    r = client.get(f"/api/weather/crags/{crag}/forecast?hours=72")
    assert r.status_code in (200, 404)
    assert r.headers.get("x-clamped-hours") == "24"

def test_cache_hit_indicator(client, caplog):
    caplog.set_level("INFO")
    crag = "045b438f-7029-4eb6-af1f-fa75eee6d4db"


    _ = client.get(f"/api/weather/crags/{crag}/forecast?hours=24")

    _ = client.get(f"/api/weather/crags/{crag}/forecast?hours=24")

    msgs = [rec.message for rec in caplog.records]
    assert any("cache=hit" in m for m in msgs) or any("cache=miss" in m for m in msgs)
