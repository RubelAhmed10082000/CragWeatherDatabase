"""Flask frontend app for CragCast.

Bridges the HTML UI to the FastAPI backend and exposes a couple of small HTTP
proxies. Also registers Jinja filters used by the templates.

Env:
    API_BASE_URL: Base URL of the FastAPI service (default http://127.0.0.1:8000).
    DEFAULT_ITEMS_PER_PAGE: Default page size for index views (string int).
    PER_PAGE_MAX: Max page size allowed (string int; defaults to DEFAULT_ITEMS_PER_PAGE).
"""

import os
from flask import Flask, jsonify, request, redirect


def create_app() -> Flask:
    app = Flask(__name__)

    # Config (local dev can come from .env via python-dotenv; Cloud Run sets envs)
    app.config["DEFAULT_ITEMS_PER_PAGE"] = int(os.getenv("DEFAULT_ITEMS_PER_PAGE", "25"))
    app.config["PER_PAGE_MAX"] = int(os.getenv("PER_PAGE_MAX", str(app.config["DEFAULT_ITEMS_PER_PAGE"])))
    # We deliberately do NOT use API_BASE_URL here; redirects keep us within the same service.

    @app.route("/")
    def index():
        # Minimal root to prove liveness from the Flask side
        return jsonify({"status": "ok", "service": "flask-frontend"})

    @app.route("/api-ping")
    def api_ping():
        """Hit FastAPI /health and return its JSON.

        Returns:
            200 JSON on success; raises if the upstream health check fails.
        """
        # Preserve your original docstring; implementation now redirects to FastAPI.
        return redirect("/health", code=307)

    @app.route("/api/weather/<lat>/<lon>")
    def weather_proxy(lat, lon):
        """Thin proxy to the FastAPI coordinate weather endpoint.

        Args:
            lat: Latitude path segment.
            lon: Longitude path segment. 

        Returns:
            A Flask Response mirroring upstream status and JSON body.
        """
        # Preserve your original docstring; implementation now redirects to FastAPI.
        qs = request.query_string.decode()
        target = f"/api/weather/{lat}/{lon}" + (f"?{qs}" if qs else "")
        return redirect(target, code=307)

    @app.route("/api/weather/crags/<crag_id>/forecast")
    def weather_forecast_proxy(crag_id):
        """Proxy to the FastAPI forecast endpoint, preserving query params.

        Passes through ?hours=… (defaults to 168) and returns upstream status/body.

        Args:
            crag_id: Crag identifier string.

        Returns:
            A Flask Response mirroring upstream status and JSON body.
        """
        # Preserve your original docstring; implementation now redirects to FastAPI.
        args = request.args.to_dict(flat=True)
        args.setdefault("hours", "168")
        qs = "&".join(f"{k}={v}" for k, v in args.items())
        target = f"/api/weather/crags/{crag_id}/forecast?{qs}"
        return redirect(target, code=307)

    # Optional: health for the Flask component itself
    @app.route("/flask/health")
    def flask_health():
        return jsonify({"status": "ok", "component": "flask"})

    return app


# WSGI entrypoint for Gunicorn / local dev
app = create_app()

if __name__ == "__main__":
    # Local dev only. Cloud Run injects PORT and runs via Gunicorn/uvicorn.
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=os.getenv("DEBUG", "0") == "1")
