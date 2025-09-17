"""FastAPI API for CragCast.

Uses JSON endpoint for crag search and weather. Mounts the Flask
frontend via WSGI. Cockroach DB CragWeatherDatabase represented as db

 Args:
        app: The FastAPI app.

    Yields:
        Control back to FastAPI to run the application
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.middleware.wsgi import WSGIMiddleware
from arquivio.web.app import app as flask_app
from .services.cockroach import db
import os
from time import monotonic
from sqlalchemy import text
from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel
import logging, uuid
from fastapi import Query, Response
from sqlalchemy.exc import SQLAlchemyError, DataError
import time
from fastapi import Request
from fastapi.responses import Response
from arquivio.core.crags import list_crags_core


@asynccontextmanager
async def lifespan(app: FastAPI):
    if os.getenv("SANITY_MODE") != "1":
        _ = db.engine
    try:
        yield
    finally:
        db.close()

app = FastAPI(title="CragCast API", version="0.1.0", lifespan=lifespan)
@app.get("/health")
async def health_check() -> dict[str, str]:
    return {"status": "ok"}

app.mount("/", WSGIMiddleware(flask_app))


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.engine.Engine").setLevel(logging.WARNING)
                                                       
log = logging.getLogger("api")
DEBUG = os.getenv("DEBUG", "0") == "1"

class ErrorOut(BaseModel):
    ok: bool = False
    code: int
    message: str
    detail: str | None = None
    request_id: str

log = logging.getLogger("api")

@app.middleware("http")
async def add_request_id(request: Request, call_next):
    rid = request.headers.get("x-request-id") or uuid.uuid4().hex[:12]
    request.state.request_id = rid
    response = await call_next(request)
    response.headers["x-request-id"] = rid
    return response

@app.middleware("http")
async def timing(request: Request, call_next):
    t0 = time.perf_counter()
    resp: Response = await call_next(request)
    ms = round((time.perf_counter() - t0) * 1000, 1)

    # if you added request IDs earlier:
    rid = getattr(request.state, "request_id", "-")

    log.info(
        "method=%s path=%s status=%s ms=%.1f rid=%s",
        request.method, request.url.path, resp.status_code, ms, rid
    )
    return resp



@app.exception_handler(HTTPException)
async def http_exc_handler(request: Request, exc: HTTPException):
    rid = getattr(request.state, "request_id", "-")
    detail = exc.detail if DEBUG else None
    if exc.status_code >= 500:
        log.exception("HTTP %s error: %s  rid=%s", exc.status_code, exc.detail, rid)
    else:
        log.warning("HTTP %s: %s  rid=%s", exc.status_code, exc.detail, rid)
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorOut(code=exc.status_code, message=str(exc.detail), detail=detail, request_id=rid).model_dump(),
    )

@app.exception_handler(RequestValidationError)
async def validation_handler(request: Request, exc: RequestValidationError):
    rid = getattr(request.state, "request_id", "-")
    msg = "Invalid request"
    detail = exc.errors() if DEBUG else None
    log.warning("422 validation: %s rid=%s", exc, rid)
    return JSONResponse(
        status_code=422,
        content=ErrorOut(code=422, message=msg, detail=str(exc) if DEBUG else None, request_id=rid).model_dump(),
    )

@app.exception_handler(Exception)
async def unhandled_handler(request: Request, exc: Exception):
    rid = getattr(request.state, "request_id", "-")
    log.exception("Unhandled error rid=%s", rid)
    return JSONResponse(
        status_code=500,
        content=ErrorOut(code=500, message="Internal server error", detail=str(exc) if DEBUG else None, request_id=rid).model_dump(),
    )

def _parse_origins(value: str | None) -> list[str]:
    return [o.strip().rstrip('/') for o in (value or "").split(",") if o.strip()]

origins = _parse_origins(os.getenv(
    "CORS_ORIGINS",
    "http://localhost:5000,http://127.0.0.1:5000"  
))

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
    styles = (style or []) + (climbing_style or []) or None
    return list_crags_core(
        q=q,
        county=county,
        rocktype=rocktype,
        styles=styles,
        page=page,
        per_page=per_page,
        sort_by=sort_by,
        sort_order=sort_order,
        db=db,
    )


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
def get_forecast(crag_id: str, hours: int = Query(168, ge=1, le=168), response: Response = None):
    """Return the hourly forecast horizon for a crag.

    Args:
        crag_id: The crag identifier.
        hours: Number of future hours to return (1–168).

    Returns:
        A list of hourly forecast records (dicts), suitable for charting.

    Raises:
        HTTPException: 404 if no forecast data is available.
    """
    orig = hours
    
    cap = int(os.getenv("FORECAST_MAX_HOURS", "168"))
    hours = min(hours, cap)
    if os.getenv("RU_DEGRADE_24H", "0") == "1":
        hours = min(hours, 24)
    if response is not None and hours != orig:
        response.headers["x-clamped-hours"] = str(hours)

    ttl_s = int(os.getenv("FORECAST_TTL_S", "600"))          
    buster = os.getenv("FORECAST_CACHE_BUSTER", "")         
    key = (str(crag_id), int(hours), buster)

    def load():
        # Calls DB layer; returns a pandas DataFrame or empty DataFrame.
        try:
            df = db.get_forecast(crag_id, hours=hours)
        except DataError:
            raise HTTPException(status_code=404, detail="No forecast for this crag")
        except SQLAlchemyError as e:
            raise HTTPException(status_code=503, detail=f"Database error: {e.__class__.__name__}")
        except SQLAlchemyError as e:
            raise HTTPException(status_code=503, detail=f"Database error: {e.__class__.__name__}")
        if df is None or getattr(df, "empty", False):
            # No data is a 404, not an empty success payload.
            raise HTTPException(status_code=404, detail="No forecast for this crag")
        return df.to_dict(orient="records")
    
    val = _ttl_get(key, ttl_s, load)
    log.info("forecast crag=%s hours=%s rows=%s cache=%s", crag_id, hours, len(val), "hit" if key in _FORECAST_CACHE else "miss")
    return val




