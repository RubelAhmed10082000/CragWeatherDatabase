from __future__ import annotations
from flask import current_app
import os, requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_session: requests.Session | None = None

def _session_get() -> requests.Session:
    """One shared Session with sane retries for idempotent GETs."""
    global _session
    if _session is not None:
        return _session

    s = requests.Session()
    s.headers.update({"Accept": "application/json"})

    # Tunables (env or Flask config; all optional)
    total = int(os.getenv("HTTP_RETRY_TOTAL", current_app.config.get("HTTP_RETRY_TOTAL", 3)))
    backoff = float(os.getenv("HTTP_RETRY_BACKOFF", current_app.config.get("HTTP_RETRY_BACKOFF", 0.5)))
    status_forcelist = (502, 503, 504)

    retry = Retry(
        total=total,
        connect=total,
        read=total,
        backoff_factor=backoff,              
        status_forcelist=status_forcelist,   
        allowed_methods=frozenset(["GET", "HEAD"]),  
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry, pool_maxsize=10)
    s.mount("https://", adapter)
    s.mount("http://", adapter)

    _session = s
    return _session

def api_url(path: str) -> str:
    base = (current_app.config.get("API_BASE_URL") or "").rstrip("/")
    return f"{base}/{path.lstrip('/')}"

def get_json(path: str, params: dict | None = None) -> dict:
    """GET + JSON with strict timeouts."""
    url = api_url(path)

    connect_t = float(os.getenv("HTTP_CONNECT_TIMEOUT", current_app.config.get("HTTP_CONNECT_TIMEOUT", 2)))
    read_t = float(os.getenv("HTTP_READ_TIMEOUT", current_app.config.get("HTTP_READ_TIMEOUT", 8)))

    r = _session_get().get(url, params=params, timeout=(connect_t, read_t))
    r.raise_for_status()
    return r.json()