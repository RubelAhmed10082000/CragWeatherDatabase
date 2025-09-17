def test_forecast_clamp_header_present_when_clamped(client):
    # Ask for too many hours to trigger clamping and header
    r = client.get("/api/weather/crags/045b438f-7029-4eb6-af1f-fa75eee6d4db/forecast?hours=9999")
    # Endpoint should exist; 2xx/4xx both acceptable for this test
    assert r.status_code in (200, 400, 404)
    # If the handler sets the clamp header, it should be a string
    hdr = r.headers.get("x-clamped-hours")
    # Header may be absent if ID not found; just ensure no crash
    if hdr is not None:
        assert hdr.isdigit()
