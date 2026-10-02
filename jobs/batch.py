"""Snapshot historical Parquet, compute exact totals, sampling and approximations."""

import argparse
import os
import uuid
from pyspark.sql import functions as F
from ecommerce.spark_common import spark, metrics
from ecommerce.sink import publish_batch_report

parser = argparse.ArgumentParser()
parser.add_argument("--source", default=os.getenv("LAKE_ROOT", "hdfs://namenode:9000/ecommerce") + "/silver")
parser.add_argument("--output", default=os.getenv("LAKE_ROOT", "hdfs://namenode:9000/ecommerce") + "/batch")
parser.add_argument("--sample-fraction", type=float, default=0.1)
args = parser.parse_args()
if not 0 < args.sample_fraction <= 1:
    parser.error("sample fraction must be in (0, 1]")
s = spark("CS404 Historical Analytics")
try:
    events = s.read.parquet(args.source).dropDuplicates(["event_id"]).cache()
    events.createOrReplaceTempView("events")
    sales = metrics(events)
    sales.write.mode("overwrite").parquet(args.output + "/windows")
    totals = s.sql("""SELECT count(*) AS events,
       sum(CASE WHEN event_type='purchase' THEN 1 ELSE 0 END) AS purchases,
       sum(CASE WHEN event_type='purchase' THEN price_paise*quantity ELSE 0 END) AS revenue_paise,
       count(DISTINCT user_id) AS active_users_exact,
       approx_count_distinct(user_id,0.05) AS active_users_approx FROM events""")
    totals.write.mode("overwrite").json(args.output + "/totals")
    # Same SHA-256 bucket as the independent Python oracle; stable across reruns.
    sampled = events.filter(
        F.conv(F.substring(F.sha2("event_id", 256), 1, 8), 16, 10).cast("long")
        < F.lit(args.sample_fraction * 2**32)
    )
    sample = sampled.agg(
        F.count("event_id").alias("sample_count"),
        F.sum(
            F.when(F.col("event_type") == "purchase", F.col("price_paise") * F.col("quantity")).otherwise(0)
        ).alias("sample_revenue_paise"),
    )
    sample = (
        sample.withColumn("fraction", F.lit(args.sample_fraction))
        .withColumn("estimated_events", F.col("sample_count") / args.sample_fraction)
        .withColumn("estimated_revenue_paise", F.col("sample_revenue_paise") / args.sample_fraction)
    )
    sample.write.mode("overwrite").json(args.output + "/sampling")
    # An observed session funnel: purchase must occur after the first cart event.
    # This is not causal attribution and not the same as purchases/cart-event ratio.
    funnel = s.sql("""WITH sessions AS (
       SELECT session_id, min(CASE WHEN event_type='cart' THEN event_ts END) AS first_cart,
       max(CASE WHEN event_type='purchase' THEN event_ts END) AS last_purchase
       FROM events GROUP BY session_id)
       SELECT count(first_cart) AS cart_sessions,
       sum(CASE WHEN last_purchase >= first_cart THEN 1 ELSE 0 END) AS converted_sessions
       FROM sessions""")
    funnel.write.mode("overwrite").json(args.output + "/session_funnel")
    summary = {
        "source": args.source,
        "totals": totals.first().asDict(),
        "sampling": sample.first().asDict(),
        "session_funnel": funnel.first().asDict(),
        "definition": "Exact unique-event historical snapshot. One purchase event is one single-product order.",
    }
    if os.getenv("DATABASE_URL"):
        publish_batch_report(str(uuid.uuid4()), summary)
    totals.show(truncate=False)
    sample.show(truncate=False)
    funnel.show(truncate=False)
finally:
    s.stop()
