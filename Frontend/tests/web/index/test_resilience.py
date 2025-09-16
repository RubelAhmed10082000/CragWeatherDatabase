from urllib.parse import urlparse


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


def test_api_500_renders_empty_state_or_502_without_crashing(client, soup, html_tools, monkeypatch):
    def _get(url, params=None, timeout=10, **kwargs):
        path = urlparse(url).path
        if path.endswith("/api/crags"):
            return _Resp(ok=False, data={"error": "boom"}, status=500, url=url)
        if path.endswith("/api/crags/facets"):
            return _Resp(
                ok=True,
                data={"counties": [], "rock_types": [], "climbing_styles": [], "countries": ["UK"]},
                status=200,
                url=url,
            )
        return _Resp(ok=False, data={"error": "not mocked"}, status=502, url=url)

    monkeypatch.setattr("requests.get", _get)

    r = client.get("/?q=stanage&style=Bouldering")
    if r.status_code == 200:
        s = soup(r.data)
        assert html_tools.rows(s) == []
    else:
        assert r.status_code == 502
        assert r.data
