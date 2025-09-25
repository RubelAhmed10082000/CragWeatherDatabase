from __future__ import annotations
import math
from flask import Blueprint, current_app, render_template, request, abort, jsonify, flash
from lib.http import get_json, api_url
import os
from requests import RequestException, HTTPError
import requests 

web = Blueprint("web", __name__, template_folder="templates", static_folder="static")

def get_json(path: str, params: dict | None = None) -> dict:
    """GET JSON via module-level `requests` so pytest can monkeypatch it.
    Normalize *any* failure into RequestException for the caller.
    """
    url = api_url(path)
    current_app.logger.warning("WEB GET %s params=%r", url, params)
    connect_t = float(os.getenv("HTTP_CONNECT_TIMEOUT", current_app.config.get("HTTP_CONNECT_TIMEOUT", 2)))
    read_t    = float(os.getenv("HTTP_READ_TIMEOUT",    current_app.config.get("HTTP_READ_TIMEOUT",    8)))
    try:
        resp = requests.get(url, params=params, timeout=(connect_t, read_t))
        if hasattr(resp, "ok") and not resp.ok:
            raise HTTPError(f"HTTP {getattr(resp, 'status_code', '???')}", response=resp)
        if hasattr(resp, "raise_for_status"):
            resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise RequestException(str(e))

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
    counties_sel = _list_arg("county")
    rocktypes_sel = _list_arg("rocktype")
    styles_sel = _list_arg("style")

    try:
        facets = get_json("api/crags/facets")
        if not isinstance(facets, dict):
            facets = {}
    except RequestException:
        current_app.logger.exception("facets fetch failed")
        flash("Facets are temporarily unavailable.", "warning")
        facets = {}

    counties = facets.get("counties", []) or []
    rock_types = facets.get("rock_types", []) or []
    climbing_styles = facets.get("climbing_styles", []) or []

    params = {
        "page": page,
        "per_page": per_page,
        "sort_by": sort_by,
        "sort_order": sort_order,
    }
    if q: params["q"] = q
    if counties_sel: params["county"] = counties_sel     
    if rocktypes_sel: params["rocktype"] = rocktypes_sel  
    if styles_sel: params["style"] = styles_sel           

    try:
        current_app.logger.warning("WEB→API /api/crags %r", params)
        data = get_json("api/crags", params=params)
    except RequestException:
        current_app.logger.exception("crags fetch failed")
        flash("Our data service is slow right now. Showing an empty list.", "warning")
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
        except RequestException:
            current_app.logger.exception("refetch last page failed")
            flash("Couldn’t load the requested page of results.", "warning")

    return render_template(
        "crags.html",
        crags=items,
        total=total,
        per_page=per_page,
        total_pages=total_pages,
        current_page=current_page,
        sort_by=sort_by,
        sort_order=sort_order,
        search_query=q or "",
        counties=counties,
        rock_types=rock_types,
        climbing_styles=climbing_styles,
        sel={
            "q": q or "",
            "county": counties_sel or [],
            "rocktype": rocktypes_sel or [],
            "style": styles_sel or [],
        },
        api_base_url=current_app.config.get("API_BASE_URL", ""),
    )
@web.get("/crag/<crag_id>")
def crag_detail_alias(crag_id: str):
    return crag_detail(crag_id)

@web.get("/crags/<crag_id>")
def crag_detail(crag_id: str):
    try:
        crag = get_json(f"api/crags/{crag_id}")
    except RequestException:
        current_app.logger.exception("crag fetch failed")
        flash("That crag isn’t available right now.", "error")
        abort(404)

    limit = _int_arg("limit", 200, 1, 500)
    offset = _int_arg("offset", 0, 0, 10_000)
    try:
        routes = get_json(f"api/crags/{crag_id}/routes", params={"limit": limit, "offset": offset})
        if isinstance(routes, dict) and "items" in routes:
            routes = routes["items"]
    except RequestException:
        current_app.logger.exception("routes fetch failed")
        flash("Routes failed to load; showing crag info only.", "warning")
        routes = []
    return render_template("crag_detail.html", crag=crag, routes=routes, api_base_url=current_app.config.get("API_BASE_URL", ""))

@web.get("/api/weather/crags/<crag_id>/forecast")
def weather_proxy(crag_id: str):
    hours = _int_arg("hours", 24, 1, 168)
    try:
        data = get_json(f"api/weather/crags/{crag_id}/forecast", params={"hours": hours})
        return jsonify(data)
    except RequestException:
        current_app.logger.exception("weather fetch failed")
        return jsonify([]), 200
    
@web.get("/api/weather/<lat>/<lon>")
def weather_proxy_latlon(lat: str, lon: str):
    hours = _int_arg("hours", 24, 1, 168)
    try:
        data = get_json(f"api/weather/{lat}/{lon}", params={"hours": hours})
        return jsonify(data)
    except RequestException as e:
        current_app.logger.exception("weather latlon proxy failed")
        return jsonify({"error": "weather_unavailable"}), 502