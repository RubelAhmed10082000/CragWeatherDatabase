def test_web_root_ok(client):
    r = client.get("/web/")
    assert r.status_code == 200
    js = r.json()
    assert js.get("status") == "ok"
    assert js.get("service") == "flask-frontend"