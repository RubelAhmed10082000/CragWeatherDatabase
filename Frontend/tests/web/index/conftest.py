import re
import types
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import pytest
from bs4 import BeautifulSoup

from src.arquivio.web.app import app as flask_app


@pytest.fixture(scope="session")
def app():
    flask_app.config.update(
        TESTING=True,
        API_BASE_URL="http://mock.api",
    )
    return flask_app


@pytest.fixture()
def client(app):
    return app.test_client()


def _dataset():
    """
    Deterministic crags with varied filters.
    """
    rows = [
        (1, "Stanage Popular", "Derbyshire", "Grit", "Trad"),
        (2, "Stanage Plantation", "Derbyshire", "Grit", "Bouldering"),
        (3, "Froggatt Edge", "Derbyshire", "Grit", "Trad"),
        (4, "Raven Tor", "Derbyshire", "Limestone", "Sport"),
        (5, "Malham Cove", "North Yorkshire", "Limestone", "Sport"),
        (6, "Almscliff", "West Yorkshire", "Grit", "Bouldering"),
        (7, "Curbar Edge", "Derbyshire", "Grit", "Trad"),
        (8, "Kilnsey Crag", "North Yorkshire", "Limestone", "Sport"),
    ]
    out = []
    for i, (id_, name, county, rock, style) in enumerate(rows, 1):
        out.append(
            {
                "id": id_,
                "name": name,
                "county": county,
                "rocktype": rock,
                "climbing_style": style,
                "routes_count": 100 + i,
                "latitude": 53.3 + i * 0.01,
                "longitude": -1.7 - i * 0.01,
                "weather": None,
                "last_rained_ts": None,
            }
        )
    return out


def _facets_from(ds):
    return {
        "countries": ["UK"],
        "rock_types": sorted({r["rocktype"] for r in ds}),
        "counties": sorted({r["county"] for r in ds}),
        "climbing_styles": sorted({r["climbing_style"] for r in ds}),
    }


@pytest.fixture(autouse=True)
def mock_requests(monkeypatch):
    """
    Intercepts requests.get for /api/crags and /api/crags/facets.
    Applies filters and pagination in-memory so the page renders deterministically.
    """
    ds = _dataset()
    facets = _facets_from(ds)

    class _Resp:
        def __init__(self, ok=True, data=None, status=200, url="http://mock"):
            self.ok = ok
            self._data = data or {}
            self.status_code = status
            self.url = url

        def json(self):
            return self._data

        def raise_for_status(self):
            if not self.ok:
                raise RuntimeError(f"HTTP {self.status_code}")

    def _get(url, params=None, timeout=10, **kwargs):
        parsed = urlparse(url)
        path = parsed.path
        params = params or {}

        if path.endswith("/api/crags/facets"):
            return _Resp(True, facets, 200, url)

        if path.endswith("/api/crags"):

            def as_list(v):
                if v is None:
                    return []
                return v if isinstance(v, list) else [v]

            q = (params.get("q") or "").lower()
            styles = set(as_list(params.get("style") or params.get("climbing_style")))
            rocks = set(as_list(params.get("rocktype")))
            counties = set(as_list(params.get("county")))

            # Filter
            filtered = []
            for r in ds:
                if q and q not in r["name"].lower():
                    continue
                if styles and r["climbing_style"] not in styles:
                    continue
                if rocks and r["rocktype"] not in rocks:
                    continue
                if counties and r["county"] not in counties:
                    continue
                filtered.append(r)

            sort_by = params.get("sort_by", "name")
            sort_order = params.get("sort_order", "asc")
            reverse = sort_order == "desc"
            try:
                filtered.sort(key=lambda x: x.get(sort_by) or "", reverse=reverse)
            except Exception:
                pass

            try:
                per_page = int(params.get("per_page") or params.get("page_size") or 10)
            except Exception:
                per_page = 10
            try:
                page = max(1, int(params.get("page") or 1))
            except Exception:
                page = 1

            total = len(filtered)
            start = (page - 1) * per_page
            end = start + per_page
            items = filtered[start:end]

            return _Resp(True, {"total": total, "items": items}, 200, f"{url}?mock=1")

        return _Resp(False, {"error": "not mocked"}, 502, url)

    monkeypatch.setattr("requests.get", _get)
    yield


@pytest.fixture
def soup():
    def _soup(html_bytes):
        return BeautifulSoup(html_bytes, "html.parser")

    return _soup


def _rows_from_soup(soup_obj):
    return [a.get_text(strip=True) for a in soup_obj.select("tbody tr a.crag-link strong")]


def _page_label(soup_obj):
    label = soup_obj.select_one(".pagination .current")
    if not label:
        return None, None
    m = re.search(r"Page\s+(\d+)\s+of\s+(\d+)", label.get_text(strip=True))
    return (int(m.group(1)), int(m.group(2))) if m else (None, None)


@pytest.fixture
def html_tools():
    return types.SimpleNamespace(
        rows=_rows_from_soup,
        page=_page_label,
    )


def _with_params(base_url: str, **new_params) -> str:
    """
    Return base_url with query params merged/replaced by new_params.
    """
    parts = urlparse(base_url)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    q.update({k: str(v) for k, v in new_params.items() if v is not None})
    new_query = urlencode(q, doseq=True)
    return urlunparse(
        (parts.scheme, parts.netloc, parts.path or "/", parts.params, new_query, parts.fragment)
    )


def collect_all_rows(client, soup, html_tools, base_query: str = "/"):
    """
    Fetch all pages for a query (uses via=pager to avoid reset).
    Works for base_query with or without existing query params.
    """
    rows = []

    r = client.get(base_query)
    assert r.status_code == 200
    s = soup(r.data)
    cur, total = html_tools.page(s)
    rows += html_tools.rows(s)

    for p in range(cur + 1, total + 1):
        url = _with_params(base_query, page=p, via="pager")
        r = client.get(url)
        assert r.status_code == 200
        s = soup(r.data)
        rows += html_tools.rows(s)

    return rows
