"""FastAPI API for CragCast.

Uses JSON endpoint for crag search and weather. Mounts the Flask
frontend via WSGI. Cockroach DB CragWeatherDatabase represented as db

 Args:
        app: The FastAPI app.

    Yields:
        Control back to FastAPI to run the application
"""

import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import text
from starlette.middleware.wsgi import WSGIMiddleware
from arquivio.web.app import app as flask_app
from .services.cockroach import db
import os
from time import monotonic
from datetime import datetime, timezone, timedelta
from sqlalchemy import text


@asynccontextmanager
async def lifespan(app: FastAPI):
    if os.getenv("SANITY_MODE") != "1":
        _ = db.engine
    try:
        yield
    finally:
        db.close()


app = FastAPI(title="CragCast API", version="0.1.0", lifespan=lifespan)

origins = [o.strip() for o in os.getenv("CORS_ORIGINS", "").split(",") if o.strip()]
if origins:  
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_methods=["*"],
        allow_headers=["*"],
        allow_credentials=True,
    )
    
_FORECAST_CACHE: dict[tuple[str, int, str], dict[str, object]] = {}

def _ttl_get(key, ttl_s: int, loader):
    now = monotonic()
    hit = _FORECAST_CACHE.get(key)
    if hit and (now - hit["t"] < ttl_s):
        return hit["v"]
    val = loader()
    _FORECAST_CACHE[key] = {"t": now, "v": val}
    return val

@app.get("/debug/db")
def db_debug():
    """basic db debug

        Returns:
            JSON file with the fb version, current database names, and
            row counts for known tables. On error, responds with `500` and an `error` field.
        """

    try:
        with db.engine.connect() as c:
            ver = c.execute(text("select version()")).scalar()
            curdb = c.execute(text("select current_database()")).scalar()
            schema = c.execute(text("select current_schema()")).scalar()

            counts = {}
            for t in [db.T_CRAGS, db.T_ROUTES, db.T_FACT]:
                try:
                    n = c.execute(text(f"select count(*) from {t}")).scalar()
                    counts[t] = int(n)
                except Exception as e:
                    counts[t] = f"ERROR: {e}"

        return JSONResponse({"version": ver, "database": curdb, "schema": schema, "counts": counts})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api")
def root():
    return {"service": "CragCast API", "status": "ok"}


@app.get("/health")
async def health_check() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/crags/facets")
def crag_facets():
    return db.get_filter_options()


@app.get("/api/crags")
def list_crags(
    q: str | None = None,
    county: list[str] | None = Query(None),
    rocktype: list[str] | None = Query(None),
    style: list[str] | None = Query(None, alias="style"),
    climbing_style: list[str] | None = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=1, le=100),
    sort_by: str = "name",
    sort_order: str = "asc",
):  
    """Search for, and paginate, crags.

    Combines text search with filters, applies sorting, and returns a
    simple paginated page.

    Args:
        q: Optional free-text query against crag name.
        county: One or more county names to include.
        rocktype: One or more rock types to include.
        style: One or more styles from the `style` parameter (alias for `climbing_style`).
        climbing_style: One or more climbing styles to include.
        page: 1-based page index (validated to be ≥ 1).
        per_page: Page size (validated to 1–100).
        sort_by: Column name to sort by (e.g., `"name"`).
        sort_order: `"asc"` or `"desc"`.

    Returns:
        Dict with:
            - `items`: List of crag records for the requested slice.
            - `total`: Total number of rows after filters.
            - `page`: Current page number.
            - `per_page`: Page size.

    Notes:
        If no results match, returns an empty `items` list with `total=0`.
    """
    # Merge the two param names the UI might send; treat empty as “no filter”.
    styles = (style or []) + (climbing_style or []) or None

    # Only pass filters with values or None
    filters = {
        "county": county or None,
        "rocktype": rocktype or None,
        "climbing_style": styles or None,
    }

    df = db.search_crags(query=q, filters=filters)
    if df is None or df.empty:
        # Consistent shape on empty result; front-end pagination stays happy.
        return {"items": [], "total": 0, "page": page, "per_page": per_page}

    if sort_by in df.columns:
        # Ascending unless explicitly “desc”; unknown sort_by is ignored on purpose.
        df = df.sort_values(by=sort_by, ascending=(sort_order.lower() != "desc"))

    total = int(df.shape[0])
    start, end = (page - 1) * per_page, (page - 1) * per_page + per_page
    items = df.iloc[start:end].to_dict(orient="records")
    return {"items": items, "total": total, "page": page, "per_page": per_page}


@app.get("/api/crags/{crag_id}")
def get_crags(crag_id: str):
    """Fetch data for a single crag. e.g. routes_count, climbing style and county location

    Args:
        crag_id: The crag identifier.

    Returns:
        The full crag record as a dict.

    Raises:
        HTTPException: 404 if the crag is not found.
    """
    crag = db.get_crag_with_routes(crag_id)
    if not crag:
        raise HTTPException(status_code=404, detail="Crag not found")
    return crag


@app.get("/api/crags/{crag_id}/routes")
def get_crag_routes(
    crag_id: str, limit: int = Query(200, ge=1, le=500), offset: int = Query(0, ge=0)
):
    """Route data for an individual crag.

    Args:
        crag_id: The crag identifier.
        limit: Maximum number of routes to return (1–500).
        offset: Number of routes to skip before returning results.

    Returns:
        A list of route dicts for the crag, sliced by `offset:offset+limit`.

    Raises:
        HTTPException: 404 if the crag is not found.
    """
    crag = db.get_crag_with_routes(crag_id)
    if not crag:
        raise HTTPException(status_code=404, detail="Crag not found")
    routes = crag.get("routes", [])
    return routes[offset : offset + limit]


@app.get("/api/weather/{lat}/{lon}")
def get_weather_for_coord(lat: float, lon: float):
    """Return the next forecast point for the crag nearest to the given coordinates.

    First tries to find an exact match. If none, searches within a
    0.5° distance and selects the nearest crag by squared distance.

    Args:
        lat: Latitude in decimal degrees.
        lon: Longitude in decimal degrees.

    Returns:
        Dict containing `temperature`, `humidity`, `precipitation`, `wind`, and `timestamp`.

    Raises:
        HTTPException: 404 if no nearby crag is found or no forecast data is available.
    """
    with db.engine.connect() as c:
        row = (
            c.execute(
                text(f"""
                SELECT crag_id::STRING AS crag_id
                FROM {db.T_CRAGS}
                WHERE latitude = :lat AND longitude = :lon
                LIMIT 1
            """),
                {"lat": lat, "lon": lon},
            )
            .mappings()
            .first()
        )

        # Fallback: search a small box then sort by squared distance
        if not row:
            row = (
                c.execute(
                    text(f"""
                    SELECT crag_id::STRING AS crag_id
                    FROM {db.T_CRAGS}
                    WHERE latitude  BETWEEN :lat - 0.5 AND :lat + 0.5
                      AND longitude BETWEEN :lon - 0.5 AND :lon + 0.5
                    ORDER BY ((latitude - :lat)*(latitude - :lat)
                           +  (longitude - :lon)*(longitude - :lon)) ASC
                    LIMIT 1
                """),
                    {"lat": lat, "lon": lon},
                )
                .mappings()
                .first()
            )

    # No nearby crag; return 404
    if not row:
        raise HTTPException(status_code=404, detail="No matching crag")

    crag_id = row["crag_id"]
    rec = db.get_next_forecast_point(crag_id, hours=168)
    if not rec:
        
        raise HTTPException(status_code=404, detail="No forecast available")

    return {
        "temperature": rec.get("temp"),
        "humidity": rec.get("humidity"),
        "precipitation": rec.get("precip"),
        "wind": rec.get("wind"),
        "timestamp": rec.get("timestamp"),
    }


@app.get("/api/weather/crags/{crag_id}/forecast")
def get_weather_history(crag_id: str, hours: int = Query(168, ge=1, le=168)):
    """Return the hourly forecast horizon for a crag.

    Args:
        crag_id: The crag identifier.
        hours: Number of future hours to return (1–168).

    Returns:
        A list of hourly forecast records (dicts), suitable for charting.

    Raises:
        HTTPException: 404 if no forecast data is available.
    """

    cap = int(os.getenv("FORECAST_MAX_HOURS", "168"))
    hours = min(hours, cap)

    ttl_s = int(os.getenv("FORECAST_TTL_S", "600"))          
    buster = os.getenv("FORECAST_CACHE_BUSTER", "")         
    key = (str(crag_id), int(hours), buster)

    def load():
        # Calls DB layer; returns a pandas DataFrame or empty DataFrame.
        df = db.get_forecast(str(crag_id), hours=hours)
        if df is None or df.empty:
            # No data is a 404, not an empty success payload.
            raise HTTPException(status_code=404, detail="No forecast available")
        return df.to_dict(orient="records")
    
    return _ttl_get(key, ttl_s, load)




app.mount("/", WSGIMiddleware(flask_app))
