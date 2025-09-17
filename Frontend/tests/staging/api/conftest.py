import os
import pytest
from sqlalchemy import create_engine
from fastapi.testclient import TestClient

@pytest.fixture(scope="session")
def client():
    os.environ["ENV"] = "test"
    os.environ["SANITY_MODE"] = "1"
    os.environ.pop("CRAG_DB_URL", None)
    os.environ.pop("DATABASE_URL", None)

    from arquivio.api.services.cockroach import db
    dummy_engine = create_engine("sqlite:///:memory:")
    db._engine = dummy_engine 

    try:
        import pandas as pd
        def fake_get_forecast(crag_id: str, hours: int):
            return pd.DataFrame([
                {"ts": "2025-09-17T00:00:00Z", "temp_c": 12.3, "precip_mm": 0.0},
                {"ts": "2025-09-17T01:00:00Z", "temp_c": 12.1, "precip_mm": 0.0},
            ])
        db.get_forecast = fake_get_forecast
    except Exception:
        pass

    from arquivio.api.main import app
    with TestClient(app) as c:
        yield c
