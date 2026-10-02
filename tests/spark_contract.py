"""Run with spark-submit after building the Spark image; compare real Spark to the oracle."""

import json
from datetime import datetime, timezone, timedelta
from pyspark.sql import functions as F, types as T
from ecommerce.events import EventGenerator
from ecommerce.reference import aggregate
from ecommerce.spark_common import spark, parsed, metrics

s = spark("CS404 Spark contract validation")
try:
    base = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    generator = EventGenerator(404, "spark-test")
    events = [generator.next(base + timedelta(seconds=i)) for i in range(180)]
    schema = T.StructType(
        [T.StructField("raw_json", T.StringType()), T.StructField("ingested_at", T.TimestampType())]
    )
    rows = [(json.dumps(e), base.replace(tzinfo=None)) for e in events]
    rows += [
        (json.dumps(dict(events[0], quantity=0)), base.replace(tzinfo=None)),
        ("{bad json", base.replace(tzinfo=None)),
        (json.dumps(dict(events[0], schema_version=2)), base.replace(tzinfo=None)),
    ]
    clean = parsed(s.createDataFrame(rows, schema))
    assert clean.filter(~F.col("valid")).count() == 3
    valid = clean.filter("valid")
    for size, slide, size_seconds, slide_seconds in [
        ("1 minute", "1 minute", 60, 60),
        ("5 minutes", "1 minute", 300, 60),
    ]:
        actual = {}
        for r in metrics(valid, size, slide).collect():
            value = r.asDict(recursive=True)
            key = (
                value["window"]["start"].replace(tzinfo=timezone.utc).isoformat(),
                value["category"],
                value["product_id"],
            )
            actual[key] = tuple(value[f] for f in ["events", "purchases", "revenue_paise", "views", "carts"])
        expected = {
            (r["window_start"], r["category"], r["product_id"]): tuple(
                r[f] for f in ["events", "purchases", "revenue_paise", "views", "carts"]
            )
            for r in aggregate(events, size_seconds, slide_seconds)
        }
        assert actual == expected, "Spark aggregate differs from the independent oracle"
    print("PASS: real Spark validation, tumbling and sliding windows agree with independent oracle")
finally:
    s.stop()
