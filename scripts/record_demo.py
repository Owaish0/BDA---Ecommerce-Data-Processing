"""Record the actual dashboard while a finite Kafka workload updates its metrics."""

import os
from pathlib import Path
import subprocess
import time

from playwright.sync_api import sync_playwright
from integration import PREFIX, accepted_count, command, wait_for


def main():
    Path("reports/demo").mkdir(parents=True, exist_ok=True)
    if os.getenv("DEMO_USE_CI") != "1":
        PREFIX[:] = ["docker", "compose"]
    if command("ps", "--status", "running", "-q", "producer"):
        raise RuntimeError("Stop the continuous producer before recording the controlled demonstration")
    baseline = accepted_count()
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1440, "height": 1000},
            record_video_dir="reports/demo",
            record_video_size={"width": 1440, "height": 1000},
        )
        page = context.new_page()
        video = page.video
        try:
            page.goto("http://localhost:8501", wait_until="domcontentloaded")
            page.get_by_text("Revenue per 1-minute window", exact=True).wait_for(timeout=60000)
            time.sleep(5)  # Reading time in the recorded demonstration.
            with open("reports/demo/producer.log", "w") as log:
                producer = subprocess.Popen(
                    PREFIX + ["run", "--rm", "--no-deps", "producer", "python", "-m",
                              "ecommerce.producer", "--rate", "10", "--count", "120", "--scenario", "burst"],
                    stdout=log, stderr=subprocess.STDOUT,
                )
                try:
                    producer.wait(timeout=150)
                    if producer.returncode:
                        raise RuntimeError("Demo producer failed; inspect reports/demo/producer.log")
                    wait_for(lambda: accepted_count() == baseline + 120, "demo burst reaches metrics")
                finally:
                    if producer.poll() is None:
                        producer.terminate()
                        producer.wait(timeout=15)
            time.sleep(8)
            page.get_by_text("Recent product activity", exact=True).scroll_into_view_if_needed()
            time.sleep(8)
            page.get_by_text("Pipeline health", exact=True).scroll_into_view_if_needed()
            time.sleep(6)
            page.get_by_text("Historical analysis and approximations", exact=True).scroll_into_view_if_needed()
            time.sleep(10)
        finally:
            context.close()
            try:
                video.save_as("reports/demo/CS404-actual-dashboard.webm")
            finally:
                browser.close()
    print("Recorded actual dashboard with 120 acknowledged burst events and verified metric growth")


if __name__ == "__main__":
    main()
