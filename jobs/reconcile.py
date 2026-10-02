"""Compare a drained silver snapshot with the live DB's exact metric columns."""

import argparse
import os
import json
from pathlib import Path
from ecommerce.spark_common import spark, metrics
from ecommerce.sink import connection

parser = argparse.ArgumentParser()
parser.add_argument("--output", default="/tmp/reconciliation.json")
args = parser.parse_args()
s = spark("CS404 Reconciliation")
try:
    root = os.getenv("LAKE_ROOT", "hdfs://namenode:9000/ecommerce")
    events = s.read.parquet(root + "/silver").dropDuplicates(["event_id"]).cache()
    fields = ["events", "purchases", "revenue_paise", "views", "carts"]
    mismatches = []
    for query, size, slide in [
        ("sales-1m-v1", "1 minute", "1 minute"),
        ("trending-5m-v1", "5 minutes", "1 minute"),
    ]:
        expected = {}
        for row in metrics(events, size, slide).toLocalIterator():
            value = row.asDict(recursive=True)
            key = (
                value["window"]["start"].isoformat(),
                value["window"]["end"].isoformat(),
                value["category"],
                value["product_id"],
            )
            expected[key] = tuple(value[f] for f in fields)
        actual = {}
        with connection() as conn:
            cursor = conn.execute(
                "SELECT window_start,window_end,category,product_id,events,purchases,revenue_paise,views,carts FROM window_metrics WHERE query_name=%s",
                (query,),
            )
            for row in cursor:
                key = (
                    row[0].replace(tzinfo=None).isoformat(),
                    row[1].replace(tzinfo=None).isoformat(),
                    row[2],
                    row[3],
                )
                actual[key] = tuple(row[4:])
        for key in expected.keys() | actual.keys():
            if expected.get(key) != actual.get(key):
                mismatches.append(
                    {"query": query, "key": key, "expected": expected.get(key), "actual": actual.get(key)}
                )
    report = {
        "matched": not mismatches,
        "mismatches": mismatches,
        "precondition": "Producer stopped and streaming stages drained. Exact unique silver snapshot compared; late-drop or conflicting-ID differences require investigation.",
    }
    path = Path(args.output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"matched": not mismatches, "mismatch_count": len(mismatches)}))
    if mismatches:
        raise SystemExit(1)
finally:
    s.stop()
