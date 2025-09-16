import math
import os
import re
import sys
import types
from pathlib import Path

import pandas as pd
import pytest
from starlette.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

os.environ.setdefault("SANITY_MODE", "1")


@pytest.fixture(scope="session")
def app():
    from src.arquivio.api.main import app as asgi_app

    return asgi_app


@pytest.fixture
def client(app):
    """
    Shim Starlette's TestClient to look like Flask's:
    - expose .data (bytes) for HTML parsing tests
    - keep .status_code/.json()/.text/.headers
    """
    base = TestClient(app)

    class ShimResponse:
        def __init__(self, r):
            self._r = r

        @property
        def status_code(self):
            return self._r.status_code

        @property
        def data(self):
            return self._r.content

        def json(self):
            return self._r.json()

        @property
        def text(self):
            return self._r.text

        @property
        def headers(self):
            return self._r.headers

    class ShimClient:
        def get(self, *args, **kwargs):
            return ShimResponse(base.get(*args, **kwargs))

        def post(self, *args, **kwargs):
            return ShimResponse(base.post(*args, **kwargs))

        def put(self, *args, **kwargs):
            return ShimResponse(base.put(*args, **kwargs))

        def delete(self, *args, **kwargs):
            return ShimResponse(base.delete(*args, **kwargs))

    return ShimClient()


@pytest.fixture(autouse=True)
def stub_db(monkeypatch):
    import src.arquivio.api.main as main

    fake = types.SimpleNamespace()

    fake.get_filter_options = lambda: {
        "county": ["Derbyshire", "North Yorkshire"],
        "rocktype": ["Limestone", "Gritstone"],
        "climbing_style": ["Sport", "Trad"],
    }

    def _df():
        return pd.DataFrame(
            [
                {
                    "id": "c1",
                    "name": "Aardvark Wall",
                    "county": "Derbyshire",
                    "rocktype": "Limestone",
                    "climbing_style": "Sport",
                    "routes_count": 12,
                    "latitude": 53.35,
                    "longitude": -1.80,
                },
                {
                    "id": "c2",
                    "name": "Beta Buttress",
                    "county": "Derbyshire",
                    "rocktype": "Limestone",
                    "climbing_style": "Sport",
                    "routes_count": 8,
                    "latitude": 53.36,
                    "longitude": -1.81,
                },
                {
                    "id": "c3",
                    "name": "Crux Quarry",
                    "county": "Derbyshire",
                    "rocktype": "Limestone",
                    "climbing_style": "Sport",
                    "routes_count": 15,
                    "latitude": 53.37,
                    "longitude": -1.82,
                },
                {
                    "id": "c4",
                    "name": "Delta Edge",
                    "county": "Derbyshire",
                    "rocktype": "Limestone",
                    "climbing_style": "Sport",
                    "routes_count": 5,
                    "latitude": 53.38,
                    "longitude": -1.83,
                },
                {
                    "id": "c5",
                    "name": "Echo Slab",
                    "county": "Derbyshire",
                    "rocktype": "Limestone",
                    "climbing_style": "Sport",
                    "routes_count": 21,
                    "latitude": 53.39,
                    "longitude": -1.84,
                },
            ]
        )

    fake.search_crags = lambda query=None, filters=None: _df()

    fake.get_crag_with_routes = lambda crag_id: (
        {"crag_id": crag_id, "name": "Stanage", "routes": [{"name": "Flying Buttress"}]}
        if crag_id in {"c1", "c2", "c3", "c4", "c5", "abc"}
        else None
    )
    fake.get_forecast = lambda crag_id, hours=168: pd.DataFrame(
        [{"timestamp": "2024-01-01T00:00:00Z", "temp": 10, "wind": 5, "precip": 0}]
    )
    fake.get_next_forecast_point = lambda crag_id, hours=168: {
        "timestamp": "2024-01-01T00:00:00Z",
        "temp": 10,
        "wind": 5,
        "precip": 0,
        "humidity": 70,
    }

    fake.engine = property(lambda self: (_ for _ in ()).throw(RuntimeError("should not touch DB")))
    monkeypatch.setattr(main, "db", fake)
    yield


try:
    from bs4 import BeautifulSoup
except Exception:
    BeautifulSoup = None


@pytest.fixture
def soup():
    if BeautifulSoup is None:
        pytest.skip("bs4 not installed; run: pip install beautifulsoup4")

    def _make(html: str):
        return BeautifulSoup(html, "html.parser")

    return _make


@pytest.fixture
def html_tools():
    class Tools:
        def q(self, s, selector):  # first match
            return s.select_one(selector)

        def qq(self, s, selector):  # all matches
            return s.select(selector)

        def text(self, el):
            return "" if el is None else el.get_text(strip=True)

        def rows(self, s):
            for sel in [
                "table#crags-table tbody tr",
                "table[data-testid='crags'] tbody tr",
                "table.crags tbody tr",
                "#crags-table tbody tr",
                "tbody tr.crag-row",
            ]:
                els = s.select(sel)
                if els:
                    return [e for e in els if e.find_all("td")]
            t = s.select_one("table")
            if t:
                body_rows = t.select("tbody tr")
                if body_rows:
                    return [e for e in body_rows if e.find_all("td")]
                all_rows = t.select("tr")
                if len(all_rows) > 1:
                    return [e for e in all_rows[1:] if e.find_all("td")]
            return s.select("[data-row='crag'], .crag-row, li.crag, .table-row")

        def page(self, s):
            # ---- current page ----
            curr = None
            for sel in [
                ".pagination li.active",
                "nav[aria-label='pagination'] .active",
                "#pager .active",
                ".pager .active",
                "[data-current-page]",
            ]:
                el = s.select_one(sel)
                if el:
                    val = el.get("data-current-page") or el.get_text(strip=True)
                    try:
                        curr = int(val)
                        break
                    except Exception:
                        pass
            if curr is None:
                inp = s.select_one("input[name=page][value]")
                if inp:
                    try:
                        curr = int(inp["value"])
                    except Exception:
                        pass

            # ---- total pages (prefer metadata) ----
            total_pages = None

            # 1) explicit data attr
            el = s.select_one("[data-total-pages]")
            if el and el.get("data-total-pages"):
                try:
                    total_pages = int(el["data-total-pages"])
                except Exception:
                    pass

            # 2) hidden inputs total/per_page -> ceil
            if total_pages is None:

                def _int_from_input(name):
                    inp = s.select_one(f"input[name='{name}'][value]")
                    if inp:
                        try:
                            return int(inp["value"])
                        except Exception:
                            pass
                    return None

                total = _int_from_input("total") or _int_from_input("total_items")
                per = _int_from_input("per_page") or _int_from_input("page_size")
                if total and per:
                    total_pages = max(1, math.ceil(total / per))

            # 3) inline JSON-like text: "total": N, "per_page": M
            if total_pages is None:
                m = re.search(
                    r'"total"\s*:\s*(\d+).*?"per_?page"\s*:\s*(\d+)',
                    s.get_text(" ", strip=True),
                    re.I | re.S,
                )
                if m:
                    total_pages = max(1, math.ceil(int(m.group(1)) / int(m.group(2))))

            # 4) "Page X of Y"
            if total_pages is None:
                m = re.search(r"Page\s+(\d+)\s+of\s+(\d+)", s.get_text(" ", strip=True), re.I)
                if m:
                    curr = curr or int(m.group(1))
                    total_pages = int(m.group(2))

            # 5) fallback: max numeric pagination link
            if total_pages is None:
                nums = []
                for a in s.select(
                    ".pagination a, .pagination li, nav[aria-label='pagination'] a, #pager a"
                ):
                    t = "".join(ch for ch in a.get_text() if ch.isdigit())
                    if t:
                        try:
                            nums.append(int(t))
                        except Exception:
                            pass
                total_pages = max(nums) if nums else 1

            return (curr or 1, total_pages)

    return Tools()


from urllib.parse import urlencode, urlsplit

import src.arquivio.web.routes as web_routes


@pytest.fixture(autouse=True)
def loopback_requests(monkeypatch, app):
    """
    Make requests from Flask routes (requests.get(...)) hit the in-process ASGI app
    instead of 127.0.0.1:8000. This avoids needing a real server during tests.
    """
    client = TestClient(app)

    def to_path(url: str, params=None) -> str:
        u = urlsplit(url)
        q = u.query
        if params:
            q = (q + "&" if q else "") + urlencode(params, doseq=True)
        return u.path + (("?" + q) if q else "")

    class _Req:
        def get(self, url, params=None, **kwargs):
            path = to_path(url, params)
            return client.get(path, headers=kwargs.get("headers"))

        def post(self, url, data=None, json=None, **kwargs):
            path = to_path(url, None)
            return client.post(path, data=data, json=json, headers=kwargs.get("headers"))

    monkeypatch.setattr(web_routes, "requests", _Req())
    yield
