import glob
import re
from pathlib import Path

import pytest
from bs4 import BeautifulSoup


def _find_template():
    candidates = [
        Path("templates/crag_detail.html"),
        Path("crag_detail.html"),
    ]
    for p in candidates:
        if p.exists():
            return p
    hits = glob.glob("**/crag_detail.html", recursive=True)
    return Path(hits[0]) if hits else None


@pytest.fixture(scope="module")
def detail_html():
    p = _find_template()
    if not p:
        pytest.skip("crag_detail.html not found")
    return p.read_text(encoding="utf-8")


def test_buttons_present_and_default_active(detail_html):
    soup = BeautifulSoup(detail_html, "html.parser")
    bar = soup.select_one(".wx-range-bar")
    assert bar is not None, "Missing .wx-range-bar above the sidebar"

    chips = bar.select(".wx-chip")
    found = {c.get_text(strip=True): c for c in chips}
    assert {"24h", "48h", "72h", "7-Days"} <= set(found.keys()), "Expected 4 range buttons"

    assert "active" in found["24h"].get("class", []), "24h should be active by default"


def test_js_fetch_uses_hours_parameter(detail_html):
    assert re.search(
        r"/api/weather/crags/\${?CRAG_ID}?/forecast\?hours=\${?hours}?", detail_html
    ), "JS should fetch forecast with the selected hours parameter"


def test_js_has_fallback_to_lat_lon(detail_html):
    assert "/api/weather/" in detail_html, "Expected fallback fetch to LAT/LON endpoint"
