import unittest
from datetime import datetime, timezone, timedelta
from ecommerce.events import EventGenerator, validate, in_sample
from ecommerce.reference import aggregate, window_starts, sample_estimate


class ContractTests(unittest.TestCase):
    def setUp(self):
        self.time = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
        self.event = EventGenerator(404, "test").next(self.time)

    def test_money_is_integer_and_purchase_only(self):
        e = self.event.copy()
        e.update(event_type="view", price_paise=19999, quantity=3)
        p = dict(e, event_id="purchase", event_type="purchase")
        row = aggregate([e, p])[0]
        self.assertEqual(row["events"], 2)
        self.assertEqual(row["purchases"], 1)
        self.assertEqual(row["revenue_paise"], 59997)

    def test_replayed_ids_do_not_inflate_exact_totals(self):
        rows = aggregate([self.event] * 100)
        self.assertEqual(sum(r["events"] for r in rows), 1)

    def test_tumbling_boundary_is_half_open(self):
        a = dict(self.event, event_id="a", event_time=(self.time + timedelta(seconds=59)).isoformat())
        b = dict(self.event, event_id="b", event_time=(self.time + timedelta(seconds=60)).isoformat())
        self.assertEqual([r["events"] for r in aggregate([a, b])], [1, 1])

    def test_sliding_event_belongs_to_five_windows(self):
        starts = window_starts(self.time + timedelta(seconds=12), 300, 60)
        self.assertEqual(len(starts), 5)
        self.assertEqual(sum(r["events"] for r in aggregate([self.event], 300, 60)), 5)

    def test_invalid_contracts_are_rejected(self):
        bad = [
            {"quantity": True},
            {"price_paise": 2.5},
            {"quantity": 0},
            {"event_type": "refund"},
            {"event_id": " "},
            {"schema_version": 2},
            {"schema_version": 1.0},
            {"event_type": []},
            {"event_time": "2026-01-01T10:00:00"},
            {"price_paise": -1},
            {"category": "x" * 129},
        ]
        for patch in bad:
            with self.subTest(patch=patch), self.assertRaises(ValueError):
                validate(dict(self.event, **patch))

    def test_future_poisoning_is_rejected(self):
        with self.assertRaises(ValueError):
            validate(dict(self.event, event_time=(self.time + timedelta(minutes=6)).isoformat()), self.time)

    def test_seeded_generation_is_repeatable(self):
        a = EventGenerator(1, "fixture")
        b = EventGenerator(1, "fixture")
        self.assertEqual([a.next(self.time) for _ in range(100)], [b.next(self.time) for _ in range(100)])

    def test_sampling_is_stable_and_deduplicated(self):
        self.assertEqual(in_sample("ID", 0.1), in_sample("ID", 0.1))
        self.assertEqual(sample_estimate([self.event] * 10, 1)["sample_count"], 1)
        with self.assertRaises(ValueError):
            in_sample("ID", 0)

    def test_exact_cardinality_overlapping_users(self):
        a = dict(self.event, event_id="a", user_id="u")
        b = dict(self.event, event_id="b", user_id="u")
        self.assertEqual(aggregate([a, b])[0]["active_users_exact"], 1)


if __name__ == "__main__":
    unittest.main()
