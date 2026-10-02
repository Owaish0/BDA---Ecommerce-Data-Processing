"""Exercise the complete stack. Fails loudly and saves diagnostic service logs."""

import json
import os
from pathlib import Path
import subprocess
import time
import urllib.request

PREFIX = ["docker", "compose", "-f", "compose.yaml", "-f", "compose.ci.yaml"]


def command(*args, timeout=120):
    result = subprocess.run(PREFIX + list(args), text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(" ".join(args) + " failed:\n" + result.stdout[-5000:] + result.stderr[-5000:])
    return result.stdout.strip()


def sql(query):
    return command("exec", "-T", "postgres", "psql", "-U", "commerce", "-d", "commerce", "-Atc", query)


def wait_for(check, description, seconds=300):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(5)
    raise RuntimeError("Timed out: " + description)


def main():
    Path("reports").mkdir(exist_ok=True)
    os.environ.setdefault("POSTGRES_PASSWORD", "ci-test-only")
    # Start without the continuously running producer so input cardinality is controlled.
    command(
        "up",
        "-d",
        "kafka",
        "kafka-init",
        "postgres",
        "namenode",
        "datanode",
        "hdfs-init",
        "spark-master",
        "spark-worker",
        "streaming",
        "dashboard",
        timeout=300,
    )
    command(
        "run",
        "--rm",
        "--no-deps",
        "producer",
        "python",
        "-m",
        "ecommerce.producer",
        "--rate",
        "100",
        "--count",
        "1000",
        "--scenario",
        "normal",
        timeout=120,
    )
    wait_for(
        lambda: int(sql("SELECT coalesce(sum(events),0) FROM window_metrics WHERE query_name='sales-1m-v1'"))
        == 1000,
        "1000 accepted events reach dashboard metrics",
    )
    with urllib.request.urlopen("http://localhost:8501/_stcore/health", timeout=10) as response:
        assert response.status == 200
    before = sql(
        "SELECT coalesce(sum(events),0)||'|'||coalesce(sum(revenue_paise),0) FROM window_metrics WHERE query_name='sales-1m-v1'"
    )
    command("restart", "streaming")
    after = sql(
        "SELECT coalesce(sum(events),0)||'|'||coalesce(sum(revenue_paise),0) FROM window_metrics WHERE query_name='sales-1m-v1'"
    )
    assert before == after, "Restart changed aggregate totals"
    command(
        "run",
        "--rm",
        "--no-deps",
        "producer",
        "python",
        "-m",
        "ecommerce.producer",
        "--rate",
        "100",
        "--count",
        "100",
        "--scenario",
        "normal",
        timeout=120,
    )
    wait_for(
        lambda: int(sql("SELECT coalesce(sum(events),0) FROM window_metrics WHERE query_name='sales-1m-v1'"))
        == 1100,
        "checkpointed queries accept 100 new events without inflating old counts",
    )
    command(
        "run",
        "--rm",
        "--no-deps",
        "batch",
        "/opt/spark/bin/spark-submit",
        "--master",
        "local[2]",
        "--driver-memory",
        "1g",
        "/app/jobs/reconcile.py",
        timeout=180,
    )
    report = {
        "accepted_events": 1100,
        "dashboard_health": "passed",
        "restart_totals_unchanged": True,
        "batch_reconciliation": "passed",
        "scope": "normal in-order fixture; staged lateness/outage cases remain separate",
    }
    Path("reports/integration.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report))


if __name__ == "__main__":
    try:
        main()
    finally:
        result = subprocess.run(PREFIX + ["logs", "--no-color", "--tail=300"], capture_output=True, text=True)
        Path("reports").mkdir(exist_ok=True)
        Path("reports/services.log").write_text(result.stdout + result.stderr, encoding="utf-8")
