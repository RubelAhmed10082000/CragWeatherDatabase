from starlette.testclient import TestClient


def test_facets(app):
    client = TestClient(app)
    r = client.get("/api/crags/facets")
    assert r.status_code == 200
    data = r.json()
    assert "county" in data and data["county"]


def test_list_crags_defaults(app):
    client = TestClient(app)
    r = client.get("/api/crags?page=1&per_page=25&sort_by=name&sort_order=asc")
    assert r.status_code == 200
    data = r.json()
    assert data["total"] >= 1
    assert data["items"]


def test_crag_detail_404(app):
    client = TestClient(app)
    r = client.get("/api/crags/does-not-exist")
    assert r.status_code == 404


def test_crag_routes_slice(app):
    client = TestClient(app)
    r = client.get("/api/crags/abc/routes?limit=1&offset=0")
    assert r.status_code == 200
    assert isinstance(r.json(), list)
