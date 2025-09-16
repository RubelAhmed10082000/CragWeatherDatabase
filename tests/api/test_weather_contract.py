from starlette.testclient import TestClient


def test_forecast_by_crag(app):
    client = TestClient(app)
    r = client.get("/api/weather/crags/abc/forecast?hours=24")
    assert r.status_code == 200
    body = r.json()
    assert isinstance(body, list) and body and "timestamp" in body[0]
