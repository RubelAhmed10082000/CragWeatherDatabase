from datetime import datetime

from pydantic import BaseModel, Field


class WeatherPoint(BaseModel):
    timestamp: datetime
    temp: float | None = Field(None, description="°C")
    humidity: float | None = Field(None, description="%")
    precip: float | None = Field(None, description="mm")
    wind: float | None = Field(None, description="m/s")

    model_config = {"from_attributes": True}


class WeatherSnapshot(BaseModel):
    crag_id: int
    point: WeatherPoint | None = None
    units: dict[str, str] = {"temp": "°C", "humidity": "%", "precip": "mm", "wind": "m/s"}
    last_updated: datetime | None = None


class WeatherForecast(BaseModel):
    crag_id: int
    points: list[WeatherPoint]
    units: dict[str, str] = {"temp": "°C", "humidity": "%", "precip": "mm", "wind": "m/s"}
    start: datetime | None = None
    end: datetime | None = None
    last_updated: datetime | None = None
