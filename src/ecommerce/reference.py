"""Independent exact oracle for deterministic validation; not a Spark replacement."""

from collections import defaultdict
from datetime import datetime, timezone
from .events import timestamp, validate, in_sample


def window_starts(at, size, slide):
    epoch = int(at.timestamp())
    end = epoch - epoch % slide
    return [start for start in range(end, end - size, -slide) if start <= epoch < start + size]


def aggregate(events, size=60, slide=60):
    groups = defaultdict(
        lambda: {"events": 0, "purchases": 0, "revenue_paise": 0, "views": 0, "carts": 0, "users": set()}
    )
    seen = set()
    for event in events:
        validate(event)
        if event["event_id"] in seen:
            continue
        seen.add(event["event_id"])
        for start in window_starts(timestamp(event["event_time"]), size, slide):
            key = (start, event["category"], event["product_id"])
            g = groups[key]
            g["events"] += 1
            g["users"].add(event["user_id"])
            kind = event["event_type"]
            g["purchases"] += int(kind == "purchase")
            g["views"] += int(kind == "view")
            g["carts"] += int(kind == "cart")
            if kind == "purchase":
                g["revenue_paise"] += event["price_paise"] * event["quantity"]
    result = []
    for (start, category, product), g in sorted(groups.items()):
        users = g.pop("users")
        result.append(
            {
                "window_start": datetime.fromtimestamp(start, timezone.utc).isoformat(),
                "window_end": datetime.fromtimestamp(start + size, timezone.utc).isoformat(),
                "category": category,
                "product_id": product,
                **g,
                "active_users_exact": len(users),
            }
        )
    return result


def sample_estimate(events, fraction=0.1):
    seen = set()
    sampled = []
    for event in events:
        validate(event)
        if event["event_id"] not in seen and in_sample(event["event_id"], fraction):
            sampled.append(event)
        seen.add(event["event_id"])
    revenue = sum(e["price_paise"] * e["quantity"] for e in sampled if e["event_type"] == "purchase")
    return {
        "fraction": fraction,
        "sample_count": len(sampled),
        "estimated_events": len(sampled) / fraction,
        "estimated_revenue_paise": revenue / fraction,
    }
