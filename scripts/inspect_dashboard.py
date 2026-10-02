"""Browser verification against a live integration dashboard, with screenshot evidence."""

from pathlib import Path
from playwright.sync_api import sync_playwright

Path("reports").mkdir(exist_ok=True)
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 1440, "height": 1000})
    try:
        page.goto("http://localhost:8501", wait_until="domcontentloaded")
        page.get_by_text("Events in horizon", exact=True).wait_for(timeout=60000)
        page.get_by_text("Revenue per 1-minute window", exact=True).wait_for(timeout=60000)
        page.get_by_text("Exact distinct users", exact=True).wait_for(timeout=60000)
        assert page.get_by_text("Analytics database is unavailable.", exact=False).count() == 0
    finally:
        page.screenshot(path="reports/dashboard.png", full_page=True)
        Path("reports/dashboard-visible-text.txt").write_text(
            page.locator("body").inner_text(), encoding="utf-8"
        )
        browser.close()
print("PASS: live dashboard renders real metric cards and chart titles; screenshot saved")
