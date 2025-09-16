# tests/detail/test_detail_e2e.py
import json
import os
from datetime import datetime, timedelta

from playwright.sync_api import Page

BASE_URL = os.getenv("E2E_BASE_URL", "http://127.0.0.1:8000")
CRAG_PATH = os.getenv("E2E_CRAG_PATH", "/crags/045b438f-7029-4eb6-af1f-fa75eee6d4db")

CARDS_SEL = "#wx-cards .wx"  # matches your current DOM


def make_hours(n: int):
    base = datetime(2025, 9, 8, 12, 0, 0)
    return [
        {
            "timestamp": (base + timedelta(hours=i)).isoformat(),
            "temperature_c": 12 + (i % 5),
            "precipitation_mm": 0.1 * (i % 6),
            "windspeed_ms": 3.0 + (i % 4),
            "relative_humidity_percentage": 70 + (i % 10),
        }
        for i in range(n)
    ]


def test_weather_range_buttons(page: Page):
    # Clear any previous hour selection + stub Leaflet before page scripts
    page.add_init_script("localStorage.clear()")
    page.add_init_script("""
      if (!window.L) {
        window.L = {
          map(){ return { setView(){return this;} }; },
          tileLayer(){ return { addTo(){return this;} }; },
          marker(){ return { addTo(){ return { bindPopup(){} }; } }; }
        };
      }
    """)

    # Stub APIs (log to prove interception)
    def handle_all(route):
        url = route.request.url
        if "/api/weather/crags/" in url and "/forecast" in url:
            print("STUB forecast:", url)
            route.fulfill(
                status=200,
                headers={"content-type": "application/json"},
                body=json.dumps(make_hours(200)),
            )
            return
        if "/api/weather/" in url:
            print("STUB now:", url)
            route.fulfill(
                status=200,
                headers={"content-type": "application/json"},
                body=json.dumps(make_hours(1)[0]),
            )
            return
        route.fallback()

    page.route("**/*", handle_all)

    # Surface browser-side errors to pytest output
    page.on("pageerror", lambda e: print("PAGEERROR:", e))
    page.on("console", lambda m: print("BROWSER:", m.type, m.text))

    # Navigate (ensure a forecast request happens during navigation)
    with page.expect_request("**/api/weather/crags/**/forecast*"):
        page.goto(BASE_URL + CRAG_PATH)
        page.evaluate("() => window.loadForecastIntoSidebar && window.loadForecastIntoSidebar(24)")
    page.wait_for_load_state("domcontentloaded")

    # Force a fresh render using our stubbed data, in case the first render fell back to "now"
    page.evaluate("() => window.loadForecastIntoSidebar && window.loadForecastIntoSidebar(24)")

    # Default: wait for 24 cards
    page.wait_for_function(f'document.querySelectorAll("{CARDS_SEL}").length === 24', timeout=15000)

    # Helper to click a chip and assert new count (also ensure the correct request fires)
    def click_and_expect(n: int):
        BTN_BY_HOURS = {24: "chip-24", 48: "chip-48", 72: "chip-72", 168: "chip-168"}
        testid = BTN_BY_HOURS[n]

        with page.expect_request(lambda req: "/forecast" in req.url and f"hours={n}" in req.url):
            page.get_by_test_id(testid).click()
            page.wait_for_function(
                f'document.querySelectorAll("{CARDS_SEL}").length === {n}', timeout=15000
            )

    click_and_expect(48)
    click_and_expect(72)
    click_and_expect(168)

    # Spot-check icon + metrics exist
    assert page.locator(".wx-strip img.wx-ic").count() > 0
    meta = page.locator(".wx .wx-meta").first.inner_text()
    for label in ("Wind:", "Rain:", "Hum:"):
        assert label in meta
