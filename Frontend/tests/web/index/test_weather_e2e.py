from __future__ import annotations

import os
import re
import urllib.parse
from typing import Any

import pytest
import requests
from playwright.sync_api import Page


@pytest.fixture(scope="session")
def base_url() -> str:
    raw = os.environ.get("BASE_URL", "http://localhost:8000")
    if not re.match(r"^https?://", raw):  # allow "localhost:8000"
        raw = "http://" + raw
    return raw.rstrip("/")


# Auto-skip E2E tests (those that use Playwright's 'page' fixture) if server not up
@pytest.fixture(autouse=True)
def _require_live_server_for_e2e(base_url, request):
    if "page" in request.fixturenames:
        try:
            requests.get(base_url + "/", timeout=2)
        except Exception:
            pytest.skip(f"E2E needs a live server at {base_url}. Start the app or set BASE_URL.")


# ---------- helpers ----------
def goto_index(page: Page, base_url: str, qs: str = ""):
    url = base_url.rstrip("/") + "/" + (f"?{qs}" if qs else "")
    page.goto(url, wait_until="domcontentloaded")


def visible_cards(page: Page) -> int:
    return page.evaluate(
        "([...document.querySelectorAll('[data-crag-id]')].filter(el => getComputedStyle(el).display !== 'none').length)"
    )


def all_card_ids(page: Page) -> list[str]:
    return page.evaluate(
        "([...document.querySelectorAll('[data-crag-id]')].map(el => el.getAttribute('data-crag-id')).filter(Boolean))"
    )


def set_hours(page: Page, hours: int):
    page.select_option("#wxfWindow", str(hours))


def toggle_chip(page: Page, name: str, on: bool = True):
    sel = f'.wxf-chip[data-filter="{name}"]'
    is_on = page.locator(sel).evaluate("el => el.classList.contains('active')")
    if is_on != on:
        page.click(sel)


def set_temp_range(page: Page, lo: int, hi: int):
    page.evaluate(
        """([lo, hi]) => {
            const sMin = document.getElementById('wxfTempMin');
            const sMax = document.getElementById('wxfTempMax');
            if (!sMin || !sMax) return;
            sMin.value = String(lo);
            sMax.value = String(hi);
            sMin.dispatchEvent(new Event('input', { bubbles: true }));
            sMax.dispatchEvent(new Event('input', { bubbles: true }));
        }""",
        [lo, hi],
    )


def click_search(page: Page):
    page.evaluate(
        """() => {
            const form = document.getElementById('searchForm');
            if (!form) return;
            if (form.requestSubmit) form.requestSubmit(); else form.submit();
        }"""
    )
    page.wait_for_load_state("domcontentloaded")


def wait_weather_applied(page: Page):
    if page.locator("#wxfStatus").count():
        try:
            page.locator("#wxfStatus").wait_for(state="hidden", timeout=5000)
        except Exception:
            pass
    else:
        page.wait_for_timeout(500)


def fetch_forecast(page: Page, crag_id: str, hours: int) -> list[dict[str, Any]]:
    return page.evaluate(
        """async ([cid, h]) => {
            const r = await fetch(`/api/weather/crags/${cid}/forecast?hours=${h}`);
            if (!r.ok) return [];
            return await r.json();
        }""",
        [crag_id, hours],
    )


def considered_daypart(rows):
    from datetime import datetime

    try:
        from zoneinfo import ZoneInfo

        tz = ZoneInfo("Europe/London")
    except Exception:
        tz = None

    out = []
    for r in rows:
        ts = r.get("timestamp") or r.get("date")
        if not ts:
            continue
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        if tz:
            dt = dt.astimezone(tz)
        if 9 <= dt.hour < 21:
            out.append(r)
    return out


def fractions(
    rows: list[dict[str, Any]], tmin: float, tmax: float
) -> tuple[float | None, float | None, float | None, int]:
    cons = considered_daypart(rows)
    n = len(cons)
    if n == 0:
        return None, None, None, 0
    dry = sum(1 for r in cons if float(r.get("precipitation_mm") or 0.0) <= 0.0) / n
    low = sum(1 for r in cons if float(r.get("windspeed_ms") or 0.0) < 8.0) / n
    tmp = (
        sum(
            1
            for r in cons
            if r.get("temperature_c") is not None and tmin <= float(r["temperature_c"]) <= tmax
        )
        / n
    )
    return dry, low, tmp, n


def next_page(page: Page):
    for sel in [
        '.pagination a:has-text("Next")',
        '.pagination button:has-text("Next")',
        '.pagination a:has-text("»")',
    ]:
        if page.locator(sel).count():
            page.click(sel)
            page.wait_for_load_state("domcontentloaded")
            return
    page.evaluate("() => window.gotoPage && window.gotoPage(2)")


def weather_tag_text(page: Page) -> str:
    return page.evaluate(
        "(() => { const chip = document.querySelector('#activeFilters .filter-tag[data-name=\"weather\"]'); return chip ? chip.textContent : ''; })()"
    )


def query_params(page: Page) -> dict[str, str]:
    q = page.evaluate("() => location.search")
    return dict(urllib.parse.parse_qsl(q[1:])) if q.startswith("?") else {}


def visible_card_ids(page: Page) -> list[str]:
    return page.evaluate(
        """() => [...document.querySelectorAll('[data-crag-id]')]
               .filter(el => getComputedStyle(el).display !== 'none')
               .map(el => el.getAttribute('data-crag-id'))
               .filter(Boolean)"""
    )


# ---------- tests ----------


def click_first_visible_crag(page: Page):
    # Click via JS from the first row that's actually visible (display != none)
    page.evaluate("""() => {
      const row = [...document.querySelectorAll('[data-crag-id]')]
        .find(el => getComputedStyle(el).display !== 'none');
      if (!row) return;
      const a = row.querySelector('a.crag-link, a[href]');
      if (a) a.click();
    }""")
    page.wait_for_load_state("domcontentloaded")


def test_01_hours_uses_168(page: Page, base_url: str):
    goto_index(page, base_url)
    set_hours(page, 168)
    toggle_chip(page, "dry", True)
    with page.expect_response(
        lambda r: "/api/weather/crags/" in r.url and "hours=168" in r.url and r.ok
    ):
        click_search(page)
    wait_weather_applied(page)


def test_02_temperature_range_filters(page: Page, base_url: str):
    goto_index(page, base_url)
    set_hours(page, 24)
    toggle_chip(page, "dry", True)
    click_search(page)
    wait_weather_applied(page)
    base = visible_cards(page)

    set_temp_range(page, 16, 18)
    assert visible_cards(page) == base  # not applied yet
    click_search(page)
    wait_weather_applied(page)
    after = visible_cards(page)
    assert after <= base

    ids = visible_card_ids(page)[:5]
    for cid in ids:
        rows = fetch_forecast(page, cid, 24)
        _, _, tmp, n = fractions(rows, 16, 18)
        assert n == 0 or (tmp is not None and tmp >= 0.80)


def test_03_dry_and_low_chips_work(page: Page, base_url: str):
    ids = visible_card_ids(page)[:6]
    for cid in ids:
        rows = fetch_forecast(page, cid, 24)
        dry, low, tmp, n = fractions(rows, -100, 100)
        assert n == 0 or (dry is not None and dry >= 0.80)
        assert n == 0 or (low is not None and low >= 0.80)

    goto_index(page, base_url)
    set_hours(page, 24)
    toggle_chip(page, "dry", True)
    click_search(page)
    wait_weather_applied(page)
    a = visible_cards(page)

    toggle_chip(page, "low-wind", True)
    click_search(page)
    wait_weather_applied(page)
    b = visible_cards(page)
    assert b <= a


def test_04_daypart_80pct_logic(page: Page, base_url: str):
    goto_index(page, base_url)
    set_hours(page, 48)
    toggle_chip(page, "dry", True)
    toggle_chip(page, "low-wind", True)
    set_temp_range(page, 10, 18)
    click_search(page)
    wait_weather_applied(page)

    ids = visible_card_ids(page)[:6]
    for cid in ids:
        rows = fetch_forecast(page, cid, 48)
        dry, low, tmp, n = fractions(rows, 10, 18)
        assert n > 0
        assert (dry or 0) >= 0.80
        assert (low or 0) >= 0.80
        assert (tmp or 0) >= 0.80


def test_05_persist_weather_through_pagination(page: Page, base_url: str):
    goto_index(page, base_url)
    set_hours(page, 24)
    toggle_chip(page, "dry", True)
    set_temp_range(page, 12, 20)
    click_search(page)
    wait_weather_applied(page)
    qp_before = query_params(page)
    assert "wxf_window" in qp_before

    next_page(page)
    wait_weather_applied(page)
    tag_after = weather_tag_text(page)
    qp_after = query_params(page)
    assert tag_after, "Weather tag should persist across pages"
    assert "wxf_window" in qp_after


def test_06_weather_filters_combined(page: Page, base_url: str):
    goto_index(page, base_url)
    set_hours(page, 72)
    toggle_chip(page, "dry", True)
    toggle_chip(page, "low-wind", True)
    set_temp_range(page, 8, 16)
    click_search(page)
    wait_weather_applied(page)
    ids = visible_card_ids(page)[:5]
    for cid in ids:
        rows = fetch_forecast(page, cid, 72)
        dry, low, tmp, n = fractions(rows, 8, 16)
        assert n > 0
        assert (dry or 0) >= 0.80 and (low or 0) >= 0.80 and (tmp or 0) >= 0.80


def test_07_weather_with_other_filters(page: Page, base_url: str):
    goto_index(page, base_url)
    for idx in (0, 1, 2):
        page.locator(".multi-select .ms-trigger").nth(idx).click()
        page.locator(".multi-select.open input[type='checkbox']").first.check()
        page.locator(".multi-select .ms-trigger").nth(idx).click()

    set_hours(page, 24)
    toggle_chip(page, "dry", True)
    click_search(page)
    wait_weather_applied(page)

    def has_chip(page: Page, name: str) -> bool:
        return page.locator(f'#activeFilters .filter-tag[data-name="{name}"]').count() > 0

    assert has_chip(page, "style")
    assert has_chip(page, "rocktype")
    assert has_chip(page, "county")
    assert has_chip(page, "weather")

    assert page.locator('#activeFilters .filter-tag[data-name="style"]').count() > 0
    assert page.locator('#activeFilters .filter-tag[data-name="rocktype"]').count() > 0
    assert page.locator('#activeFilters .filter-tag[data-name="county"]').count() > 0
    assert page.locator('#activeFilters .filter-tag[data-name="weather"]').count() > 0


def test_08_filters_persist_after_back_nav(page: Page, base_url: str):
    goto_index(page, base_url)
    set_hours(page, 24)
    toggle_chip(page, "dry", True)
    set_temp_range(page, 12, 18)
    page.locator(".multi-select .ms-trigger").first.click()
    page.locator(".multi-select.open input[type='checkbox']").first.check()
    page.locator(".multi-select .ms-trigger").first.click()

    click_search(page)
    wait_weather_applied(page)
    tag_before = weather_tag_text(page)
    count_before = visible_cards(page)

    click_first_visible_crag(page)

    page.go_back(wait_until="domcontentloaded")
    wait_weather_applied(page)

    assert weather_tag_text(page) == tag_before
    assert visible_cards(page) == count_before


def test_09_search_button_applies_filters(page: Page, base_url: str):
    goto_index(page, base_url)
    set_hours(page, 24)
    base = visible_cards(page)
    toggle_chip(page, "dry", True)
    assert visible_cards(page) == base  # not applied yet

    click_search(page)
    wait_weather_applied(page)
    after = visible_cards(page)
    assert after <= base


def test_10_reset_button_resets_filters(page: Page, base_url: str):
    goto_index(page, base_url)
    set_hours(page, 48)
    toggle_chip(page, "dry", True)
    toggle_chip(page, "low-wind", True)
    set_temp_range(page, 10, 14)
    page.locator(".multi-select .ms-trigger").first.click()
    page.locator(".multi-select.open input[type='checkbox']").first.check()
    page.locator(".multi-select .ms-trigger").first.click()

    click_search(page)
    wait_weather_applied(page)
    assert weather_tag_text(page)

    if page.locator(".reset-btn").count():
        page.click(".reset-btn")
    else:
        page.get_by_text("Reset", exact=True).click()
    page.wait_for_load_state("domcontentloaded")

    assert weather_tag_text(page) == ""
    qp = query_params(page)
    assert not any(k.startswith("wxf_") for k in qp.keys())
    chips = [t.lower() for t in page.locator("#activeFilters .filter-tag").all_text_contents()]
    assert all(("style" not in t and "rock" not in t and "county" not in t) for t in chips)
