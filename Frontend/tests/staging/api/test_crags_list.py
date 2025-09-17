import pandas as pd

def test_crags_list_uses_core_and_handles_last_rain(monkeypatch, client):
    df = pd.DataFrame([{
        "crag_id": "045b438f-7029-4eb6-af1f-fa75eee6d4db",
        "name": "Grit Edge",
        "region": "Peak",
        "last_rained_ts": None,
        "last_rain_severity": None,
    }])

    # Patch the *instance* methods used by the core
    from arquivio.api.services.cockroach import db

    def fake_search_crags(query=None, filters=None, sort=None, page=1, per_page=25):
        return df

    def fake_count_crags(query=None, filters=None):
        return len(df)

    monkeypatch.setattr(db, "search_crags", fake_search_crags, raising=False)
    monkeypatch.setattr(db, "count_crags", fake_count_crags, raising=False)

    r = client.get("/api/crags?per_page=5")
    assert r.status_code == 200
    js = r.json()
    items = js.get("items") or js.get("data")
    assert items and isinstance(items, list)
    assert js.get("total", 1) >= 1
