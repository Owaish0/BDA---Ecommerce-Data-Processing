"""Versioned event contract. Money is always integer paise, never floating point."""

from datetime import datetime, timezone, timedelta
import hashlib
import json
import random
import uuid

TYPES = {"view", "search", "cart", "purchase"}
CATALOG = [
    ("P001", "Headphones", "Electronics", 249900),
    ("P002", "Smartwatch", "Electronics", 399900),
    ("P003", "Running shoes", "Fashion", 199900),
    ("P004", "Backpack", "Fashion", 129900),
    ("P005", "Coffee maker", "Home", 349900),
    ("P006", "Desk lamp", "Home", 99900),
]


def timestamp(value):
    if not isinstance(value, str):
        raise ValueError("timestamp must be a string")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("timestamp must include timezone")
    return result.astimezone(timezone.utc)


def validate(event, ingested_at=None):
    if not isinstance(event, dict):
        raise ValueError("event must be a JSON object")
    if type(event.get("schema_version")) is not int or event.get("schema_version") != 1:
        raise ValueError("unsupported schema_version")
    for key in ["event_id", "user_id", "session_id", "product_id", "category"]:
        value = event.get(key)
        if not isinstance(value, str) or not value.strip() or len(value) > 128:
            raise ValueError(f"invalid {key}")
    if not isinstance(event.get("event_type"), str) or event.get("event_type") not in TYPES:
        raise ValueError("invalid event_type")
    for key, minimum in [("quantity", 1), ("price_paise", 0)]:
        value = event.get(key)
        maximum = 1000 if key == "quantity" else 100_000_000
        if type(value) is not int or not minimum <= value <= maximum:
            raise ValueError(f"invalid {key}")
    event_time = timestamp(event.get("event_time"))
    if ingested_at and event_time > ingested_at + timedelta(minutes=5):
        raise ValueError("event timestamp too far in the future")
    return event


def in_sample(event_id, fraction):
    if not 0 < fraction <= 1:
        raise ValueError("sample fraction must be in (0, 1]")
    bucket = int(hashlib.sha256(event_id.encode()).hexdigest()[:8], 16)
    return bucket / 2**32 < fraction


class EventGenerator:
    def __init__(self, seed=404, run_id=None):
        self.rng = random.Random(seed)
        self.run_id = run_id or str(uuid.uuid4())
        self.sequence = 0

    def next(self, at=None, trending=False):
        self.sequence += 1
        rng = self.rng
        product = CATALOG[0] if trending and rng.random() < 0.7 else rng.choice(CATALOG)
        user = rng.randint(1, 1000)
        return {
            "schema_version": 1,
            "event_id": f"{self.run_id}-{self.sequence:09d}",
            "event_time": (at or datetime.now(timezone.utc)).isoformat(),
            "user_id": f"U{user:04d}",
            "session_id": f"{self.run_id}-S{user:04d}",
            "event_type": rng.choices(["view", "search", "cart", "purchase"], [55, 15, 20, 10])[0],
            "product_id": product[0],
            "product_name": product[1],
            "category": product[2],
            "quantity": rng.randint(1, 3),
            "price_paise": product[3],
        }


def encode(event):
    return json.dumps(validate(event), separators=(",", ":"), allow_nan=False).encode()
