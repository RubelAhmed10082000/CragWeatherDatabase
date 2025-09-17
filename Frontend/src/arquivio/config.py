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
    if ENV == "production":
        dsn = os.getenv("DATABASE_URL", "")
    else:
        dsn = os.getenv("DEV_DATABASE_URL", "")
    if not dsn:
        raise RuntimeError("No database URL configured")
    return dsn