# tests/conftest.py
import os
import pytest
from fastapi.testclient import TestClient

os.environ["ENV"] = "test"
os.environ["SANITY_MODE"] = "1"     
os.environ.pop("CRAG_DB_URL", None) 
os.environ.pop("DATABASE_URL", None)

from arquivio.api.main import app

@pytest.fixture(scope="session")
def client():
    with TestClient(app) as c:
        yield c
