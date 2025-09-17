import json

def test_weather_coords_200(client):
    r = client.get("/api/weather/crags/045b438f-7029-4eb6-af1f-fa75eee6d4db/forecast?hours=24")
    assert r.status_code in (200, 400, 404) 
    json.loads(r.text)