"""
Crag and route data models and schemas.
"""

from datetime import datetime

from pydantic import BaseModel, Field


class Route(BaseModel):
    """Climbing route model."""

    crag_id: str
    name: str
    grade: str | None = None
    stars: int = Field(default=0, ge=0, le=5)
    safety_grade: str | None = None
    type: str | None = None
    difficulty: str | None = None
    description: str | None = None
    created_at: datetime


class Crag(BaseModel):
    """Climbing crag model."""

    route_id: str
    id: str
    name: str
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    routes_count: int = 0
    county: str | None = None
    rocktype: str | None = None
    climbing_style: str | None = None
    routes: list[Route] | None = None
