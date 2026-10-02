"""Measure finite workload visibility on an idle running stack; never invent per-event latency."""

import argparse
import json
import math
from pathlib import Path
import platform
import subprocess
import time
import urllib.request

from integration import PREFIX, accepted_count, command, produce, wait_for


def percentile(values, probability):
    return sorted(values)[max(0, math.ceil(len(values) * probability) - 1)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rates", type=int, nargs="+", default=[100, 500, 1000])
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--events", type=int, default=1000)
    parser.add_argument("--ci", action="store_true", help="Use the smaller CI Compose override")
    args = parser.parse_args()
    if min(args.rates) <= 0 or args.trials <= 0 or args.events <= 0:
        parser.error("rates, trials and events must be positive")
    if not args.ci:
        PREFIX[:] = ["docker", "compose"]
    if command("ps", "--status", "running", "-q", "producer"):
        parser.error("Stop the continuous producer first; benchmark needs exclusive input")
    with urllib.request.urlopen("http://localhost:8080/json/", timeout=10) as response:
        cluster = json.load(response)
    records = []
    Path("reports").mkdir(exist_ok=True)
    for rate in args.rates:
        for trial in range(args.trials):
            baseline = accepted_count()
            started = time.monotonic()
            produce(args.events, rate=rate)
            delivered = time.monotonic()
            wait_for(
                lambda: accepted_count() == baseline + args.events,
                "benchmark input becomes visible without count inflation", seconds=600,
            )
            finished = time.monotonic()
            records.append({
                "requested_rate": rate, "trial": trial + 1, "events": args.events,
                "delivery_seconds": delivered - started,
                "all_visible_seconds": finished - started,
                "after_delivery_seconds": finished - delivered,
                "effective_visible_events_per_second": args.events / (finished - started),
            })
            print(json.dumps(records[-1]), flush=True)
    summaries = []
    for rate in args.rates:
        values = [r["all_visible_seconds"] for r in records if r["requested_rate"] == rate]
        summaries.append({"rate": rate, "batch_visibility_p50_seconds": percentile(values, .5),
                          "batch_visibility_p95_seconds": percentile(values, .95)})
    report = {
        "host": platform.platform(), "cluster": cluster,
        "docker_version": subprocess.check_output(["docker", "version", "--format", "{{json .}}"], text=True),
        "measurements": records, "summaries": summaries,
        "definition": "Finite batch launch to all events visible in sales metrics. Includes producer container startup, delivery and polling (up to 5s). Not per-event latency or sustainable throughput. Small-sample percentiles are descriptive only.",
    }
    Path("reports/benchmark.json").write_text(json.dumps(report, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
