"""Flask web routes for CragCast.

Renders the index and detail pages, and provides a small proxy for the API's
weather endpoint (used by front-end JS). The actual data comes from the FastAPI
backend configured via `API_BASE_URL` in Flask app config.
"""

import math
from pathlib import Path

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
    api = current_app.config["API_BASE_URL"]

    # Config-driven pagination defaults (kept small by default for UX).
    DEFAULT_PP = int(current_app.config.get("DEFAULT_ITEMS_PER_PAGE", 2))
    PER_PAGE_MAX = int(current_app.config.get("PER_PAGE_MAX", DEFAULT_PP))

    # Parse inputs safely.
    q = request.args.get("q") or request.args.get("search") or None

    page = _to_int(request.args.get("page", "1"), 1)
    page = _clamp(page, 1, 1_000_000)

    raw_pp = request.args.get("per_page")
    if raw_pp is None:
        raw_pp = request.args.get("page_size", str(DEFAULT_PP))
    per_page = _to_int(raw_pp, DEFAULT_PP)
    per_page = _clamp(per_page, 1, PER_PAGE_MAX)

    sort_by = request.args.get("sort_by", "name")
    sort_order = (request.args.get("sort_order", "asc") or "asc").lower()
    if sort_order not in {"asc", "desc"}:
        sort_order = "asc"

    # Preserve `sel` structure used by the template.
    sel = {
        "style": (_get_list("style") or _get_list("climbing_style")),
        "rocktype": _get_list("rocktype"),
        "county": _get_list("county"),
    }

    # Fetch facet lists; fail soft with empty lists to keep the page usable.
    try:
        facets_resp = requests.get(f"{api}/api/crags/facets", timeout=10)
        facets_resp.raise_for_status()
        facets = facets_resp.json()
        if isinstance(facets, list):  # defensive: older API shapes
            facets = {"countries": [], "rock_types": [], "counties": [], "climbing_styles": []}
    except Exception:
        facets = {"countries": [], "rock_types": [], "counties": [], "climbing_styles": []}

    # If filters/search are present and the user didn't click a pager control,
    # jump back to page 1 to avoid asking for an out-of-range slice.
    via = request.args.get("via", "")
    filters_present = bool(q) or bool(sel["style"]) or bool(sel["rocktype"]) or bool(sel["county"])
    if filters_present and page > 1 and via != "pager":
        page = 1

    # Build upstream params. Lists become repeated keys in `requests`.
    params = {
        "page": page,
        "per_page": per_page,
        "page_size": per_page,  # alias for any legacy readers
        "sort_by": sort_by,
        "sort_order": sort_order,
    }
    if q:
        params["q"] = q
    if sel["style"]:
        # Send both for compatibility with API variants.
        params["style"] = sel["style"]
        params["climbing_style"] = sel["style"]
    if sel["rocktype"]:
        params["rocktype"] = sel["rocktype"]
    if sel["county"]:
        params["county"] = sel["county"]

    # First fetch.
    r = requests.get(f"{api}/api/crags", params=params, timeout=10)
    if not r.ok:
        abort(502, f"API /api/crags failed: {r.status_code}")

    data = r.json()
    if isinstance(data, list):
        # Raw/mock shape: payload IS the items.
        items = data
        total = len(items)
    else:
        items = data.get("items", [])
        total = data.get("total") or data.get("count") or data.get("total_count")
        if total is None:
            total = len(items)
        total = int(total)

    total_pages = max(1, math.ceil(total / per_page))

    # Overflow clamp & refetch once (e.g., filters reduced total).
    if page > total_pages:
        page = total_pages
        params["page"] = page
        r2 = requests.get(f"{api}/api/crags", params=params, timeout=10)
        if r2.ok:
            data2 = r2.json()
            items = data2 if isinstance(data2, list) else data2.get("items", [])

    # Debug log to help trace pagination behavior when troubleshooting.
    print(
        f"[crags_page] page={page} per_page={per_page} total={total} "
        f"total_pages={total_pages} url={r.url} returned={len(items)}"
    )

    # Render the index template with current state and facet lists.
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
        # Pre-selects (single-value helpers for the template UI).
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
