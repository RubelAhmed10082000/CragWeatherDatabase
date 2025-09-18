from __future__ import annotations
from flask import current_app
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_session: requests.Session | None = None

def _session_get() -> requests.Session:
    global _session
    if _session is None:
        s = requests.Session()
        s.headers.update({"Accept": "application/json"})
        retry = Retry(
            total=3,
            connect=3,
            read=3,
            backoff_factor=0.5,
            status_forcelist=(502, 503, 504),
            allowed_methods=frozenset(["GET", "HEAD"]),
            raise_on_status=False,
        )
        s.mount("https://", HTTPAdapter(max_retries=retry))
        s.mount("http://", HTTPAdapter(max_retries=retry))
        _session = s
    return _session

def api_url(path: str) -> str:
    base = current_app.config.get("API_BASE_URL", "").rstrip("/")
    return f"{base}/{path.lstrip('/')}"

def get_json(path: str, params: dict | None = None) -> dict:
    url = api_url(path)
    r = _session_get().get(url, params=params, timeout=(2, 8))
    r.raise_for_status()
    return r.json()