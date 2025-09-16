def test_imports():
    import src.arquivio.api.main as api
    import src.arquivio.web.app as web

    assert hasattr(api, "app")
    assert hasattr(web, "app")


def test_docs_and_health(app):
    from starlette.testclient import TestClient

    client = TestClient(app)
    assert client.get("/health").status_code == 200
    assert client.get("/docs").status_code == 200
    assert client.get("/openapi.json").status_code == 200
