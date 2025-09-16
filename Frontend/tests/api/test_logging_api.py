import os
import re
import pytest
from fastapi.testclient import TestClient
import importlib

@pytest.fixture
def app(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    monkeypatch.setenv("FORECAST_MAX_HOURS", "168")
    monkeypatch.setenv("FORECAST_TTL_S", "600")
    mod = importlib.import_module("arquivio.api.main")
    importlib.reload(mod)
    return mod.app

def test_logging_lines(caplog, app, monkeypatch):
    caplog.set_level("INFO")
    client = TestClient(app)

    r = client.get("/health")
    assert r.status_code == 200
    assert any("method=GET" in rec.message and "path=/health" in rec.message for rec in caplog.records)

    crag = "045b438f-7029-4eb6-af1f-fa75eee6d4db"
    caplog.clear()
    r = client.get(f"/api/weather/crags/{crag}/forecast?hours=24")
    assert r.status_code in (200, 404)

    msgs = [rec.message for rec in caplog.records]
    assert any("forecast" in m and "hours=24" in m for m in msgs)
