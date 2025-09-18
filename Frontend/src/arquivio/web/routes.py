# src/arquivio/web/routes.py
from __future__ import annotations
import math
from flask import Blueprint, current_app, render_template, request, abort, jsonify, flash
from lib.http import get_json 

web = Blueprint("web", __name__, template_folder="templates", static_folder="static")

def _int_arg(name: str, default: int, lo: int, hi: int) -> int:
    try:
        v = int(request.args.get(name, default))
    except (TypeError, ValueError):
        v = default
    return max(lo, min(hi, v))

def _str_arg(name: str) -> str | None:
    v = request.args.get(name)
    return v if v else None

def _list_arg(name: str) -> list[str] | None:
    vals = [v for v in request.args.getlist(name) if v]
    return vals or None

@web.get("/")
def crags_page():
    page = _int_arg("page", 1, 1, 1_000_000)
    per_page = _int_arg("per_page", 25, 1, 100)
    sort_by = request.args.get("sort_by") or "name"
    sort_order = request.args.get("sort_order") or "asc"

    q = _str_arg("q")
    county = _str_arg("county")
    rocktype = _str_arg("rocktype")
    styles = _list_arg("style") 

    try:
        facets = get_json("api/crags/facets")
        if not isinstance(facets, dict):
            facets = {}
    except Exception:
        current_app.logger.exception("facets fetch failed")
        facets = {}

    params = {
        "page": page,
        "per_page": per_page,
        "sort_by": sort_by,
        "sort_order": sort_order,
    }
    if q is not None:
        params["q"] = q
    if county is not None:
        params["county"] = county
    if rocktype is not None:
        params["rocktype"] = rocktype
    if styles is not None:
        params["style"] = styles

    try:
        data = get_json("api/crags", params=params)
    except Exception:
        current_app.logger.exception("crags fetch failed")
        flash("Service is slow right now. Try again shortly.", "warning")
        data = {"items": [], "total": 0}

    items = data.get("items", [])
    total = int(data.get("total", 0))
    total_pages = max(1, math.ceil(total / per_page))
    current_page = min(page, total_pages)

    if page > total_pages and total_pages > 0:
        try:
            data = get_json("api/crags", params={**params, "page": total_pages})
            items = data.get("items", [])
            current_page = total_pages
        except Exception:
            current_app.logger.exception("refetch last page failed")

    return render_template(
        "crags.html",
        crags=items,
        total=total,
        page=current_page,
        per_page=per_page,
        total_pages=total_pages,
        sort_by=sort_by,
        sort_order=sort_order,
        facets=facets,
        selected={"q": q, "county": county, "rocktype": rocktype, "style": styles or []},
    )

@web.get("/crags/<crag_id>")
def crag_detail(crag_id: str):
    try:
        crag = get_json(f"api/crags/{crag_id}")
    except Exception:
        current_app.logger.exception("crag fetch failed")
        abort(404)

    limit = _int_arg("limit", 200, 1, 500)
    offset = _int_arg("offset", 0, 0, 10_000)
    try:
        routes = get_json(f"api/crags/{crag_id}/routes", params={"limit": limit, "offset": offset})
        if isinstance(routes, dict) and "items" in routes:
            routes = routes["items"]
    except Exception:
        current_app.logger.exception("routes fetch failed")
        routes = []

    return render_template("crag_detail.html", crag=crag, routes=routes)

@web.get("/api/weather/crags/<crag_id>/forecast")
def weather_proxy(crag_id: str):
    """Temporary proxy to avoid CORS until you switch to single-origin LB."""
    hours = _int_arg("hours", 24, 1, 168)
    try:
        data = get_json(f"api/weather/crags/{crag_id}/forecast", params={"hours": hours})
        return jsonify(data)
    except Exception:
        current_app.logger.exception("weather fetch failed")
        return jsonify({"error": "unavailable"}), 502
