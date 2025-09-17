import os
from urllib.parse import urlparse

IN_CLOUD_RUN = bool(os.getenv("K_SERVICE"))
ENV = os.getenv("ENV", "development")

if not IN_CLOUD_RUN and ENV == "development":
    try:
        from dotenv import load_dotenv  
        load_dotenv()
    except Exception:
        pass

def get_database_url() -> str:
    dsn = (
        os.getenv("DATABASE_URL") if ENV == "production"
        else os.getenv("DEV_DATABASE_URL") or os.getenv("DATABASE_URL", "")
    )
    if not dsn:
        raise RuntimeError("No database URL configured")

    if ENV == "production":
        u = urlparse(dsn)
        if u.hostname in {"localhost", "127.0.0.1"} or "sslmode=disable" in dsn:
            raise RuntimeError(f"Refusing to start with invalid prod DSN: {dsn!r}")
    return dsn