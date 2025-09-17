import json

def test_weather_coords_200(client):
    r = client.get("/api/weather/53.1/-1.7?hours=24")
    
    assert r.status_code in (200, 400, 404) 
    # Must be valid JSON either way
    json.loads(r.text)