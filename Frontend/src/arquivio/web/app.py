"""Flask frontend app for CragCast.

Bridges the HTML UI to the FastAPI backend and exposes a couple of small HTTP
proxies. Also registers Jinja filters used by the templates.

Env:
    API_BASE_URL: Base URL of the FastAPI service (default http://127.0.0.1:8000).
    DEFAULT_ITEMS_PER_PAGE: Default page size for index views (string int).
    PER_PAGE_MAX: Max page size allowed (string int; defaults to DEFAULT_ITEMS_PER_PAGE).
"""

import math
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests
from flask import Flask, Response, current_app, jsonify, request

from arquivio.web.routes import web

BASE = Path(__file__).resolve().parent

# Core Flask app + basic config. Keep defaults conservative for local dev.
app = Flask(__name__, template_folder="templates", static_folder="static")

PORT = os.getenv("PORT", "8080")
INTERNAL_DEFAULT = f"http://127.0.0.1:{PORT}"

app.config["API_BASE_URL"] = os.getenv(
    "API_BASE_URL",           
    os.getenv("INTERNAL_BASE",  
             INTERNAL_DEFAULT)  
)

app.config["DEFAULT_ITEMS_PER_PAGE"] = int(os.getenv("DEFAULT_ITEMS_PER_PAGE", "25"))
app.config["PER_PAGE_MAX"] = int(
    os.getenv("PER_PAGE_MAX", str(app.config["DEFAULT_ITEMS_PER_PAGE"]))
)

# Register the site routes (index/detail pages).
app.register_blueprint(web)


@app.route("/api-ping")
def api_ping():
    """Hit FastAPI /health and return its JSON.

    Returns:
        200 JSON on success; raises if the upstream health check fails.
    """
    r = requests.get(f"{current_app.config['API_BASE_URL']}/health", timeout=5)
    r.raise_for_status()
    return jsonify(r.json())


@app.route("/api/weather/<lat>/<lon>")
def weather_proxy(lat, lon):
    """Thin proxy to the FastAPI coordinate weather endpoint.

    Args:
        lat: Latitude path segment.
        lon: Longitude path segment. 

    Returns:
        A Flask Response mirroring upstream status and JSON body.
    """
    r = requests.get(f"{current_app.config['API_BASE_URL']}/api/weather/{lat}/{lon}", timeout=8)
    return Response(r.content, status=r.status_code, content_type="application/json")


def timeago(value):
    """Human-friendly relative time (e.g., '3 hours ago').

    Accepts datetime or ISO-8601 string; assumes UTC if naive.

    Args:
        value: A datetime or ISO string (with or without 'Z').

    Returns:
        Relative time string like '2 days ago', or the original string on parse failure.
    """
    if not value:
        return None
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return value
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)

    now = datetime.now(UTC)
    seconds = int((now - value).total_seconds())
    if seconds < 60:
        return "just now"

    # Larger to smaller units; first match wins.
    units = [
        (31536000, "year"),
        (2592000, "month"),
        (604800, "week"),
        (86400, "day"),
        (3600, "hour"),
        (60, "minute"),
    ]
    for unit_seconds, name in units:
        if seconds >= unit_seconds:
            n = seconds // unit_seconds
            return f"{n} {name}{'' if n == 1 else 's'} ago"
    return "just now"


# Jinja filters used in templates.
app.jinja_env.filters["timeago"] = timeago


def fmt_unknown(Value: Any) -> str:
    """Normalize 'unknown-like' values to 'Unknown' for cleaner UI.

    Treats None, NaN, and common NA spellings as unknown.

    Args:
        Value: Any value coming from the data layer.

    Returns:
        'Unknown' if the value is effectively missing; otherwise a trimmed string.
    """
    if Value is None:
        return "Unknown"

    if isinstance(Value, float) and math.isnan(Value):
        return "Unknown"

    s = str(Value).strip()
    bad = {"N/A", "NA", "NaN", "Na", "Null", "", "<NA>", "None"}
    return "Unknown" if s in bad else s


app.jinja_env.filters["fmt"] = fmt_unknown


@app.route("/api/weather/crags/<crag_id>/forecast")
def weather_forecast_proxy(crag_id):
    """Proxy to the FastAPI forecast endpoint, preserving query params.

    Passes through ?hours=… (defaults to 168) and returns upstream status/body.

    Args:
        crag_id: Crag identifier string.

    Returns:
        A Flask Response mirroring upstream status and JSON body.
    """
    base = current_app.config["API_BASE_URL"]
    # Pass through query string, default to 168h if not specified.
    params = dict(request.args)
    params.setdefault("hours", "168")
    r = requests.get(f"{base}/api/weather/crags/{crag_id}/forecast", params=params, timeout=12)
    return Response(r.content, status=r.status_code, content_type="application/json")
