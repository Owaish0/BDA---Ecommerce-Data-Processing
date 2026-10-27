"""Finish the October milestone after its separately running import completes.

Keeps the dashboard responsive and stops only its own Spark container if the
host free-space reserve is crossed. Does not start the remaining six months.
"""

import json
from pathlib import Path
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone

ROOT = Path("data/history")
REPORTS = Path("reports")
NAME = "cs404-history-analysis"
RESERVE = 20 * 2**30


def status(phase, **values):
    target = ROOT / "pipeline-status.json"
    temp = target.with_suffix(".json.tmp")
    temp.write_text(
        json.dumps(
            {"phase": phase, "updated_at": datetime.now(timezone.utc).isoformat(), **values}, indent=2
        ),
        encoding="utf-8",
    )
    temp.replace(target)
    print(phase, flush=True)


def main():
    ROOT.mkdir(parents=True, exist_ok=True)
    REPORTS.mkdir(exist_ok=True)
    started = time.monotonic()
    manifest = ROOT / "months/2019-Oct/manifest.json"
    status("Waiting for validated October import")
    while not manifest.exists():
        if time.monotonic() - started > 7200:
            raise RuntimeError("Import did not complete within two hours; inspect import log")
        time.sleep(5)
    report = json.loads(manifest.read_text())
    if report["source_rows"] < 1_000_000:
        raise RuntimeError("Expected a full publisher month, not a fixture")
    free = shutil.disk_usage(ROOT).free
    if free < RESERVE + 2 * report["parquet_bytes"]:
        raise RuntimeError("Insufficient measured headroom for Spark; keep validated import and stop")
    status("Calculating October customer analytics", imported_rows=report["accepted_rows"])
    cmd = [
        "docker",
        "compose",
        "-f",
        "compose.yaml",
        "-f",
        "compose.history.yaml",
        "run",
        "--rm",
        "--no-deps",
        "--name",
        NAME,
        "historical",
    ]
    samples = []
    minimum = free
    with (REPORTS / "october-spark.log").open("w", encoding="utf-8") as log:
        proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
        try:
            while proc.poll() is None:
                minimum = min(minimum, shutil.disk_usage(ROOT).free)
                if minimum < RESERVE:
                    raise RuntimeError("Stopped Spark to preserve 20 GiB free disk space")
                if time.monotonic() - started > 4 * 3600:
                    raise RuntimeError("October milestone exceeded four hours; previous results preserved")
                measured = subprocess.run(
                    ["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", NAME],
                    capture_output=True,
                    text=True,
                    timeout=15,
                )
                if measured.returncode == 0 and measured.stdout.strip():
                    samples.append(
                        {
                            "time": datetime.now(timezone.utc).isoformat(),
                            "memory": measured.stdout.strip(),
                            "free_disk_bytes": minimum,
                        }
                    )
                time.sleep(10)
            if proc.returncode:
                raise RuntimeError("Spark job failed; inspect reports/october-spark.log")
        finally:
            if proc.poll() is None:
                subprocess.run(["docker", "stop", NAME], capture_output=True, timeout=45)
                proc.wait(timeout=30)
    subprocess.run(
        [sys.executable, "scripts/benchmark_algorithms.py"],
        check=True,
        stdout=(REPORTS / "october-algorithms.log").open("w"),
    )
    (ROOT / "resource-measurements.json").write_text(
        json.dumps(
            {
                "minimum_free_disk_bytes": minimum,
                "spark_memory_samples": samples,
                "elapsed_seconds": round(time.monotonic() - started, 2),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    status(
        "October milestone complete; remaining months await resource review",
        imported_rows=report["accepted_rows"],
        minimum_free_disk_bytes=minimum,
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        status("Stopped; investigation required", error=str(exc))
        raise
