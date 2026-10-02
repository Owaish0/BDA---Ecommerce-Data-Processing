"""Exercise the real producer CLI and inspect independent fixture results."""

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from ecommerce.events import validate
from ecommerce.reference import aggregate


class WorkloadTests(unittest.TestCase):
    def generate(self, scenario):
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory) / "fixture.jsonl"
            command = [
                sys.executable,
                "-m",
                "ecommerce.producer",
                "--file",
                str(fixture),
                "--count",
                "100",
                "--seed",
                "7",
                "--scenario",
                scenario,
                "--start-time",
                "2026-01-01T10:00:00+00:00",
            ]
            subprocess.run(command, check=True, capture_output=True, text=True)
            return [json.loads(line) for line in fixture.read_text().splitlines()]

    def test_duplicate_scenario_preserves_exact_purchase_totals(self):
        events = self.generate("duplicates")
        self.assertEqual(len(events), 100)
        self.assertEqual(len({e["event_id"] for e in events}), 90)
        rows = aggregate(events)
        self.assertEqual(sum(r["events"] for r in rows), 90)
        first = {e["event_id"]: e for e in events}
        expected = sum(
            e["quantity"] * e["price_paise"] for e in first.values() if e["event_type"] == "purchase"
        )
        self.assertEqual(sum(r["revenue_paise"] for r in rows), expected)

    def test_invalid_scenario_has_known_quarantine_cardinality(self):
        events = self.generate("invalid")
        rejected = 0
        for event in events:
            try:
                validate(event)
            except ValueError:
                rejected += 1
        self.assertEqual(rejected, 5)

    def test_late_scenario_contains_controlled_old_records(self):
        from ecommerce.events import timestamp

        normal = self.generate("normal")
        late = self.generate("late")
        lags = [
            (timestamp(a["event_time"]) - timestamp(b["event_time"])).total_seconds()
            for a, b in zip(normal, late)
        ]
        self.assertEqual(lags.count(180), 5)
        self.assertEqual(lags.count(0), 95)

    def test_nonpositive_rate_is_rejected(self):
        result = subprocess.run(
            [sys.executable, "-m", "ecommerce.producer", "--rate", "0"], capture_output=True
        )
        self.assertNotEqual(result.returncode, 0)
