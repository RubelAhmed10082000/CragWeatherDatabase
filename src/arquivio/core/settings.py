import os

from pydantic import BaseModel


class Settings(BaseModel):
    DATABASE_URL: str | None
    CORS_ORIGINS: list[str]
    DEFAULT_ITEMS_PER_PAGE: int = 2
    FORECAST_HOURS: int = 168
    READONLY: bool = True


def _parse_origins(value: str | None) -> list[str]:
    return [o.strip() for o in (value or "http://localhost:5000").split(",") if o.strip()]


settings = Settings(
    DATABASE_URL=os.environ["DATABASE_URL"],
    CORS_ORIGINS=_parse_origins(os.environ.get("CORS_ORIGINS")),
)
