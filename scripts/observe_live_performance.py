"""Observe live real replay briefly, restore its paused/running state, retain requested rate."""

import argparse
import json
from pathlib import Path
import time
from ecommerce import control

parser = argparse.ArgumentParser()
parser.add_argument("--rate", type=int, default=1000)
parser.add_argument("--seconds", type=int, default=60)
args = parser.parse_args()
with control.connect() as conn:
    original = conn.execute("SELECT * FROM generator_control WHERE id=1").fetchone()
    if original["source"] != "real":
        raise RuntimeError("This observation requires real replay")
    conn.execute("UPDATE generator_control SET rate=%s,enabled=true WHERE id=1", (args.rate,))
started = time.monotonic()
seen = set()
samples = []
try:
    while time.monotonic() - started < args.seconds:
        with control.connect() as conn:
            rows = conn.execute(
                "SELECT query_name,progress FROM query_progress WHERE updated_at > now()-interval '3 seconds'"
            ).fetchall()
        for row in rows:
            p = row["progress"]
            key = (row["query_name"], p["batchId"])
            if key in seen or not p["numInputRows"]:
                continue
            seen.add(key)
            samples.append(
                {
                    "query": row["query_name"],
                    "rows": p["numInputRows"],
                    "processing_rows_sec": p["processedRowsPerSecond"],
                    "duration_ms": p["durationMs"]["triggerExecution"],
                }
            )
        time.sleep(1)
finally:
    with control.connect() as conn:
        conn.execute("UPDATE generator_control SET enabled=%s WHERE id=1", (original["enabled"],))
        final = conn.execute("SELECT replay_position FROM generator_control WHERE id=1").fetchone()
report = {
    "target_rate": args.rate,
    "observed_seconds": time.monotonic() - started,
    "records_sent": final["replay_position"] - original["replay_position"],
    "samples": samples,
}
Path("/tmp/performance-live.json").write_text(json.dumps(report, indent=2))
print(json.dumps({k: v for k, v in report.items() if k != "samples"}))
for name in sorted({x["query"] for x in samples}):
    group = [x for x in samples if x["query"] == name]
    print(
        name,
        "observed processing rows/sec",
        round(sum(x["rows"] for x in group) * 1000 / sum(x["duration_ms"] for x in group), 1),
        "batches",
        len(group),
    )
