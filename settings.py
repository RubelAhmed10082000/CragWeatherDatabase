from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class Settings(BaseSettings):
    database_url: str = Field(..., alias="DATABASE_URL")
    gcs_bucket: str   = Field(..., alias="GCS_BUCKET")
    wx_max_points: int = Field(50, alias="WX_MAX_POINTS")

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )

settings = Settings()