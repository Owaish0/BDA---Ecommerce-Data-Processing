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


def accepted_count():
    return int(sql("SELECT coalesce(sum(events),0) FROM window_metrics WHERE query_name='sales-1m-v1'"))


def produce(count, scenario="normal", rate=100):
    command(
        "run", "--rm", "--no-deps", "producer", "python", "-m", "ecommerce.producer",
        "--rate", str(rate), "--count", str(count), "--scenario", scenario, timeout=180,
    )


def recovery_checks():
    measurements = []
    expected = 1100
    for service in ["postgres", "kafka", "hdfs"]:
        started = time.monotonic()
        if service == "postgres":
            command("stop", "postgres")
            try:
                # Kafka accepts input while the reporting database is unavailable.
                produce(100)
            finally:
                command("up", "-d", "--wait", "postgres", timeout=120)
        else:
            targets = ["namenode", "datanode"] if service == "hdfs" else ["kafka"]
            command("restart", *targets, timeout=120)
            command("up", "-d", "--wait", *targets, timeout=180)
            produce(100)
        expected += 100
        wait_for(lambda: accepted_count() == expected, service + " recovery preserves all events", seconds=420)
        measurements.append({"case": service, "recovery_seconds": round(time.monotonic() - started, 3)})
    for scenario, increment in [("duplicates", 90), ("invalid", 95)]:
        produce(100, scenario)
        expected += increment
        wait_for(lambda: accepted_count() == expected, scenario + " fixture accepted cardinality")
    return expected, measurements


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
    expected, recovery = recovery_checks()
    # Every event belongs to five sliding windows; wait for both sinks to drain.
    wait_for(
        lambda: int(sql("SELECT coalesce(sum(events),0) FROM window_metrics WHERE query_name='trending-5m-v1'"))
        == 5 * expected,
        "sliding windows drain before snapshot reconciliation",
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
    command("run", "--rm", "--no-deps", "batch", timeout=180)
    assert int(sql("SELECT count(*) FROM batch_reports")) == 1, "Historical report was not published"
    assert int(sql("SELECT summary->'totals'->>'events' FROM batch_reports")) == expected
    if os.getenv("VERIFY_DASHBOARD_BROWSER") == "1":
        subprocess.run(["python", "scripts/inspect_dashboard.py"], check=True, timeout=210)
    report = {
        "accepted_events": expected,
        "dashboard_health": "passed",
        "restart_totals_unchanged": True,
        "batch_reconciliation": "passed",
        "recovery": recovery,
        "duplicate_and_invalid_fixtures": "passed",
        "scope": "single-host CI; service restart recovery, not host-loss tolerance",
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
        diagnostic_lines = [
            line
            for line in result.stdout.splitlines()
            if any(word in line for word in ["ERROR", "Traceback", "ModuleNotFoundError", "Exception:"])
        ]
        print("\n".join(diagnostic_lines[-50:]), flush=True)
