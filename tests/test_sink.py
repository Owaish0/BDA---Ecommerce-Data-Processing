"""Real PostgreSQL tests; opt in via TEST_DATABASE_URL (isolated test database)."""

import os
from pathlib import Path
import unittest
from unittest.mock import patch
from datetime import datetime, timezone, timedelta


@unittest.skipUnless(os.getenv("TEST_DATABASE_URL"), "isolated PostgreSQL test database required")
class SinkTests(unittest.TestCase):
    def setUp(self):
        import psycopg

        self.url = os.environ["TEST_DATABASE_URL"]
        self.env = patch.dict(os.environ, {"DATABASE_URL": self.url})
        self.env.start()
        with psycopg.connect(self.url) as conn:
            conn.execute(Path("db/init.sql").read_text())
            conn.execute("TRUNCATE window_metrics, batch_commits")

    def tearDown(self):
        self.env.stop()

    def frame(self, revenue=19999, fail=False):
        class Row:
            def asDict(self, recursive=False):
                at = datetime(2026, 1, 1, tzinfo=timezone.utc)
                return {
                    "window": {"start": at, "end": at + timedelta(minutes=1)},
                    "category": "Home",
                    "product_id": "P1",
                    "events": 2,
                    "purchases": 1,
                    "revenue_paise": revenue,
                    "views": 1,
                    "carts": 0,
                    "active_users_approx": 1,
                }

        class Frame:
            def toLocalIterator(self):
                yield Row()
                if fail:
                    raise RuntimeError("injected failure before commit")

        return Frame()

    def read(self):
        import psycopg

        with psycopg.connect(self.url) as conn:
            return conn.execute("SELECT revenue_paise,epoch_id FROM window_metrics").fetchall()

    def test_retry_and_absolute_updates(self):
        from ecommerce.sink import publish

        publish(self.frame(), 0, "test")
        publish(self.frame(), 0, "test")
        self.assertEqual(self.read(), [(19999, 0)])
        publish(self.frame(39998), 1, "test")
        publish(self.frame(), 0, "test")
        self.assertEqual(self.read(), [(39998, 1)])

    def test_partial_failure_rolls_back_and_retry_succeeds(self):
        from ecommerce.sink import publish

        with self.assertRaises(RuntimeError):
            publish(self.frame(fail=True), 0, "test")
        self.assertEqual(self.read(), [])
        publish(self.frame(), 0, "test")
        self.assertEqual(self.read(), [(19999, 0)])
