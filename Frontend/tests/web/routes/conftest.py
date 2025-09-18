import os, pytest
from arquivio.web.app import create_app

@pytest.fixture(scope="session")
def app():
    api_base = os.getenv("API_BASE_URL") or os.getenv("API_BASE") or os.getenv("API")
    assert api_base, "Set API_BASE_URL (or API_BASE/API) to your staged API URL"
    app = create_app()
    app.config.update(
        TESTING=True,
        API_BASE_URL=api_base,
        HTTP_CONNECT_TIMEOUT=2,
        HTTP_READ_TIMEOUT=8,
    )
    return app

@pytest.fixture()
def client(app):
    return app.test_client()
