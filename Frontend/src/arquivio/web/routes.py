"""Flask web routes for CragCast.

Renders the index and detail pages, and provides a small proxy for the API's
weather endpoint (used by front-end JS). The actual data comes from the FastAPI
backend configured via `API_BASE_URL` in Flask app config.
"""
from arquivio.core.crags import list_crags_core, get_crag_facets_core
import math
from pathlib import Path
from arquivio.api.services.cockroach import db
import requests
from flask import Blueprint, abort, current_app, jsonify, render_template, request

HERE = Path(__file__).resolve().parent
web = Blueprint("web", __name__, template_folder=str(HERE / "templates"))


def _to_int(val, default: int) -> int:
    """Parse `val` to int, returning `default` on TypeError/ValueError."""
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


def _clamp(n: int, lo: int, hi: int) -> int:
    """Clamp integer `n` to the inclusive range [lo, hi]."""
    return max(lo, min(n, hi))


def _get_list(name: str) -> list[str]:
    """Return a cleaned list of query-arg values for `name` (drop blanks/None)."""
    vals = request.args.getlist(name)
    return [v for v in vals if v is not None and str(v).strip() != ""]


@web.route("/")
def crags_page():
    """Index page: search, filter, sort, and paginate crags.

    Reads query args, calls the API (`/api/crags` and `/api/crags/facets`), and
    renders `crags.html`. If filters change and the user isn't coming "via" the
    pager, resets to page 1 to avoid empty slices.
    """
    DEFAULT_PP = int(current_app.config.get("DEFAULT_ITEMS_PER_PAGE", 25))
    PER_PAGE_MAX = int(current_app.config.get("PER_PAGE_MAX", DEFAULT_PP))

    q = request.args.get("q") or request.args.get("search") or None

    page = _clamp(_to_int(request.args.get("page", "1"), 1), 1, 1_000_000)

    raw_pp = request.args.get("per_page")
    if raw_pp is None:
        raw_pp = request.args.get("page_size", str(DEFAULT_PP))
    per_page = _clamp(_to_int(raw_pp, DEFAULT_PP), 1, PER_PAGE_MAX)

    sort_by = request.args.get("sort_by", "name")
    sort_order = (request.args.get("sort_order", "asc") or "asc").lower()
    if sort_order not in {"asc", "desc"}:
        sort_order = "asc"

    sel = {
        "style": (_get_list("style") or _get_list("climbing_style")),
        "rocktype": _get_list("rocktype"),
        "county": _get_list("county"),
    }

    try:
        facets = get_crag_facets_core(db)
        if isinstance(facets, list):
            facets = {"countries": [], "rock_types": [], "counties": [], "climbing_styles": []}
    except Exception:
        facets = {"countries": [], "rock_types": [], "counties": [], "climbing_styles": []}

    via = request.args.get("via", "")
    filters_present = bool(q) or bool(sel["style"]) or bool(sel["rocktype"]) or bool(sel["county"])
    if filters_present and page > 1 and via != "pager":
        page = 1

    data = list_crags_core(
        q=q,
        county=sel["county"],
        rocktype=sel["rocktype"],
        styles=sel["style"],
        page=page,
        per_page=per_page,
        sort_by=sort_by,
        sort_order=sort_order,
        db=db,
    )

    items = data.get("items", []) if isinstance(data, dict) else (data or [])
    total = int(data.get("total", len(items))) if isinstance(data, dict) else len(items)
    total_pages = max(1, math.ceil(total / per_page))

    # Overflow clamp & refetch once
    if page > total_pages:
        page = total_pages
        data2 = list_crags_core(
            q=q, county=sel["county"], rocktype=sel["rocktype"], styles=sel["style"],
            page=page, per_page=per_page, sort_by=sort_by, sort_order=sort_order, db=db,
        )
        items = data2.get("items", []) if isinstance(data2, dict) else (data2 or [])

    # Render
    return render_template(
        "crags.html",
        crags=items,
        total_crags=total,
        current_page=page,
        total_pages=total_pages,
        per_page=per_page,
        page=page,
        sort_by=sort_by,
        sort_order=sort_order,
        search_query=q or "",
        countries=facets.get("countries", []),
        rock_types=facets.get("rock_types", []),
        counties=facets.get("counties", []),
        climbing_styles=facets.get("climbing_styles", []),
        sel=sel,
        selected_country="",
        selected_rocktype=(sel["rocktype"][0] if sel["rocktype"] else ""),
        selected_climbing_style=(sel["style"][0] if sel["style"] else ""),
        selected_county=(sel["county"][0] if sel["county"] else ""),
    )


@web.route("/crag/<crag_id>")
@web.route("/crags/<crag_id>")
def crag_detail(crag_id: str):
    """Detail page for a single crag.

    Retrieves the crag by ID from the API and attempts to fetch current/next
    weather by the crag's coordinates. Renders `crag_detail.html`.
    """
    api = current_app.config["API_BASE_URL"]
    r = requests.get(f"{api}/api/crags/{crag_id}", timeout=10)
    if not r.ok:
        abort(404, "Crag not found")
    crag = r.json()

    w = requests.get(f"{api}/api/weather/{crag['latitude']}/{crag['longitude']}", timeout=8)
    weather = w.json() if w.ok else {}

    return render_template("crag_detail.html", crag=crag, weather=weather)


@web.route("/api/weather/<path:lat>/<path:lon>")
def weather_proxy(lat: str, lon: str):
    """Normalizes the upstream weather response.

    Accepts latitude/longitude as path segments and returns normalized JSON keys used by the
    front-end table. Numbers are coerced to floats where possible.

    Returns:
        JSON with keys:
            - temperature_c
            - relative_humidity_percentage
            - precipitation_mm
            - windspeed_ms
            - timestamp
        Or an error JSON with appropriate HTTP status.
    """
    # Validate and coerce path segments early.
    try:
        lat_f = float(lat)
        lon_f = float(lon)
    except ValueError:
        return jsonify({"error": "bad coords"}), 400

    api = current_app.config["API_BASE_URL"]
    url = f"{api}/api/weather/{lat_f}/{lon_f}"
    r = requests.get(url, timeout=8)
    current_app.logger.info("weather_proxy %s -> %s %s", (lat, lon), url, r.status_code)
    if not r.ok:
        return jsonify({"error": "upstream", "status": r.status_code}), r.status_code

    src = r.json()

    def _num(v):
        # Convert strings like "80%" or " 11.3 " to floats; return None on failure.
        try:
            return float(str(v).replace("%", "").strip())
        except Exception:
            return None

    return jsonify(
        {
            "temperature_c": _num(src.get("temperature")),
            "relative_humidity_percentage": _num(src.get("humidity")),
            "precipitation_mm": _num(src.get("precipitation")),
            "windspeed_ms": _num(src.get("wind")),
            "timestamp": src.get("timestamp"),
        }
    )
