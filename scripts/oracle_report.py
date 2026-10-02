"""Measure exact fixture totals and sampling errors without any service dependencies."""

import argparse
import json
from pathlib import Path
from ecommerce.events import validate
from ecommerce.reference import aggregate, sample_estimate


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("fixture")
    parser.add_argument("--output", default="reports/oracle.json")
    args = parser.parse_args()
    events = []
    invalid = []
    with open(args.fixture, encoding="utf-8") as stream:
        for line_no, line in enumerate(stream, 1):
            try:
                event = json.loads(line)
                validate(event)
                events.append(event)
            except (ValueError, TypeError) as error:
                invalid.append({"line": line_no, "reason": str(error)})
    windows = aggregate(events)
    total = {key: sum(row[key] for row in windows) for key in ["events", "purchases", "revenue_paise"]}
    samples = []
    for fraction in [0.01, 0.05, 0.1, 0.5, 1]:
        estimate = sample_estimate(events, fraction)
        estimate["event_relative_error"] = (
            (estimate["estimated_events"] - total["events"]) / total["events"] if total["events"] else None
        )
        estimate["revenue_relative_error"] = (
            (estimate["estimated_revenue_paise"] - total["revenue_paise"]) / total["revenue_paise"]
            if total["revenue_paise"]
            else None
        )
        samples.append(estimate)
    report = {
        "kind": "independent fixture oracle, not a Kafka/Spark benchmark",
        "input_lines": len(events) + len(invalid),
        "invalid_records": invalid,
        "duplicate_records": len(events) - total["events"],
        "exact_totals": total,
        "sample_estimates": samples,
        "tumbling_windows": windows,
    }
    path = Path(args.output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"exact_totals": total, "invalid": len(invalid), "output": str(path)}))


if __name__ == "__main__":
    main()
