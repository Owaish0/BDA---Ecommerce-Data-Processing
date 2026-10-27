"""Compare Spark historical output with an independent Python source calculation."""

import csv
import gzip
import json
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

root = Path("data/history-fixture")
with gzip.open(root / "fixture.csv.gz", "rt", newline="") as f:
    rows = list(csv.DictReader(f))
report = json.loads((root / "customer-report.json").read_text())
totals = report["monthly"][0]
assert totals["actions"] == len(rows)
assert totals["customers"] == len({r["user_id"] for r in rows})
assert totals["purchases"] == sum(r["event_type"] == "purchase" for r in rows)
assert Decimal(totals["purchase_price_sum"]) == sum(
    (Decimal(r["price"]) for r in rows if r["event_type"] == "purchase"), Decimal(0)
)
sessions = defaultdict(list)
for r in rows:
    if r["user_session"]:
        sessions[(r["user_id"], r["user_session"])].append(r)
cart = converted = 0
for group in sessions.values():
    carts = [r["event_time"] for r in group if r["event_type"] == "cart"]
    purchases = [r["event_time"] for r in group if r["event_type"] == "purchase"]
    if carts:
        cart += 1
        converted += bool(purchases and max(purchases) >= min(carts))
assert report["session_funnel"] == {"cart_sessions": cart, "converted_sessions": converted}
for product in report["top_products"]:
    group = [r for r in rows if r["product_id"] == product["product_id"]]
    assert product["viewers"] == len({r["user_id"] for r in group if r["event_type"] == "view"})
    matched = set()
    for key, visit in sessions.items():
        same = [r for r in visit if r["product_id"] == product["product_id"]]
        views = [r["event_time"] for r in same if r["event_type"] == "view"]
        purchases = [r["event_time"] for r in same if r["event_type"] == "purchase"]
        if views and purchases and max(purchases) >= min(views):
            matched.add(key[0])
    assert product["matched_buyers"] == len(matched)
print(
    "PASS: exact totals, customers, prices, session funnel, and all displayed product conversion counts reconcile"
)
