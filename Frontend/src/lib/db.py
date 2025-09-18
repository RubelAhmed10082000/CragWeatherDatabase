from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import make_url
from .config import get_database_url

def make_engine():
    raw = get_database_url()
    url = make_url(raw)

    if url.drivername.startswith(("postgresql", "postgresql+psycopg2")):
        url = url.set(drivername="cockroachdb+psycopg2")

    return create_engine(url, pool_pre_ping=True, future=True)

ENGINE = make_engine()
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, autocommit=False, future=True)

def get_session():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
