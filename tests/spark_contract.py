"""Run with spark-submit after building the Spark image; compare real Spark to the oracle."""

import json
from datetime import datetime, timezone, timedelta
import tempfile
from pathlib import Path
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
    with tempfile.TemporaryDirectory() as directory:
        source = Path(directory) / "input"
        source.mkdir()
        stream_schema = T.StructType(
            [T.StructField("event_id", T.StringType()), T.StructField("event_ts", T.TimestampType())]
        )
        source_stream = (
            s.readStream.schema(stream_schema)
            .json(str(source))
            .withWatermark("event_ts", "2 minutes")
            .dropDuplicatesWithinWatermark(["event_id"])
        )
        query = (
            source_stream.writeStream.format("memory")
            .queryName("watermark_contract")
            .outputMode("append")
            .option("checkpointLocation", str(Path(directory) / "checkpoint"))
            .start()
        )

        def send(number, event_id, seconds):
            payload = {"event_id": event_id, "event_ts": (base + timedelta(seconds=seconds)).isoformat()}
            temporary = Path(directory) / "pending.json"
            temporary.write_text(json.dumps(payload) + "\n")
            temporary.rename(source / f"{number:03d}.json")
            query.processAllAvailable()

        try:
            send(1, "original", 10)
            send(2, "original", 10)
            assert s.table("watermark_contract").count() == 1, "Duplicate within watermark was not suppressed"
            send(3, "advance", 300)
            send(4, "within", 270)
            send(5, "too-old", 30)
            ids = {row.event_id for row in s.table("watermark_contract").collect()}
            assert ids == {"original", "advance", "within"}, f"Unexpected watermark results: {ids}"
            print("PASS: staged real Spark duplicate suppression and within/beyond watermark handling")
        finally:
            query.stop()
finally:
    s.stop()
