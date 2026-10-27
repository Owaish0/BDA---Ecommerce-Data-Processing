"""Build bounded dashboard summaries from complete historical month partitions."""

import argparse
import json
from pathlib import Path
import time
from datetime import datetime, timezone
from pyspark.sql import SparkSession, functions as F, Window

parser = argparse.ArgumentParser()
parser.add_argument("--root", default="/history")
args = parser.parse_args()
root = Path(args.root)
months = sorted(p.parent for p in (root / "months").glob("*/manifest.json"))
if not months:
    raise RuntimeError("No validated complete months available")
started = time.monotonic()
s = (
    SparkSession.builder.appName("CS404 Customer Analytics")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "24")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)


def records(frame):
    return [r.asDict(recursive=True) for r in frame.collect()]


try:
    frames = [s.read.option("basePath", str(p / "events")).parquet(str(p / "events")) for p in months]
    events = frames[0]
    for frame in frames[1:]:
        events = events.unionByName(frame)
    events = events.withColumn("month", F.date_format("event_time", "yyyy-MM"))
    # IDs identify source rows. Repeated behaviour at different rows is not automatically a duplicate.
    expected = sum(json.loads((p / "manifest.json").read_text())["accepted_rows"] for p in months)
    actual = events.count()
    if actual != expected:
        raise RuntimeError(f"Historical count mismatch: {actual} != {expected}")
    events.createOrReplaceTempView("events")
    monthly = s.sql("""SELECT month,count(*) actions,count(DISTINCT user_id) customers,
        approx_count_distinct(user_id,0.05) estimated_customers,
        sum(CASE WHEN event_type='view' THEN 1 ELSE 0 END) views,
        sum(CASE WHEN event_type='cart' THEN 1 ELSE 0 END) carts,
        sum(CASE WHEN event_type='purchase' THEN 1 ELSE 0 END) purchases,
        sum(CASE WHEN event_type='purchase' THEN price_minor ELSE 0 END)/100.0 purchase_price_sum
        FROM events GROUP BY month ORDER BY month""")
    users = s.sql("""SELECT user_id,month,count(*) actions,
        sum(CASE WHEN event_type='view' THEN 1 ELSE 0 END) views,
        sum(CASE WHEN event_type='cart' THEN 1 ELSE 0 END) carts,
        sum(CASE WHEN event_type='purchase' THEN 1 ELSE 0 END) purchases
        FROM events GROUP BY user_id,month""")
    users = users.withColumn("first_observed_month", F.min("month").over(Window.partitionBy("user_id")))
    users.createOrReplaceTempView("customer_months")
    behaviour = s.sql("""SELECT month,count(*) active_customers,
        sum(CASE WHEN month>first_observed_month THEN 1 ELSE 0 END) returning_customers,
        sum(CASE WHEN views>0 THEN 1 ELSE 0 END) viewers,
        sum(CASE WHEN carts>0 THEN 1 ELSE 0 END) cart_users,
        sum(CASE WHEN purchases>0 THEN 1 ELSE 0 END) purchasers,
        sum(CASE WHEN purchases>1 THEN 1 ELSE 0 END) multiple_purchase_record_customers
        FROM customer_months GROUP BY month ORDER BY month""")
    interests = s.sql("""SELECT category,count(*) views,count(DISTINCT user_id) viewers
        FROM events WHERE event_type='view' GROUP BY category ORDER BY views DESC,category LIMIT 20""")
    # Match view then purchase within the same observed customer/session/product.
    pairs = s.sql("""SELECT user_id,session_id,product_id,
        min(CASE WHEN event_type='view' THEN event_time END) first_view,
        min(CASE WHEN event_type='cart' THEN event_time END) first_cart,
        max(CASE WHEN event_type='purchase' THEN event_time END) last_purchase
        FROM events WHERE session_id IS NOT NULL GROUP BY user_id,session_id,product_id""")
    pairs.createOrReplaceTempView("pairs")
    conversions = s.sql("""SELECT product_id,count(DISTINCT CASE WHEN last_purchase>=first_view THEN user_id END) matched_buyers
        FROM pairs GROUP BY product_id""")
    product_totals = s.sql("""SELECT product_id,max(category) category,
        count(DISTINCT CASE WHEN event_type='view' THEN user_id END) viewers,
        count(DISTINCT CASE WHEN event_type='purchase' THEN user_id END) purchasers,
        sum(CASE WHEN event_type='view' THEN 1 ELSE 0 END) views,
        sum(CASE WHEN event_type='purchase' THEN 1 ELSE 0 END) purchases
        FROM events GROUP BY product_id""")
    products = (
        product_totals.join(conversions, "product_id", "left")
        .fillna(0, subset=["matched_buyers"])
        .withColumn(
            "observed_conversion_pct",
            F.when(F.col("viewers") > 0, 100 * F.col("matched_buyers") / F.col("viewers")),
        )
    )
    products.createOrReplaceTempView("products")
    # First purchase at/after the first view, not the last purchase in a session.
    timings = (
        events.filter("event_type='purchase'")
        .alias("e")
        .join(
            pairs.alias("p"),
            (F.col("e.user_id") == F.col("p.user_id"))
            & (F.col("e.session_id") == F.col("p.session_id"))
            & (F.col("e.product_id") == F.col("p.product_id"))
            & (F.col("e.event_time") >= F.col("p.first_view")),
        )
    )
    timings = timings.select(
        F.col("e.user_id").alias("user_id"),
        F.col("e.session_id").alias("session_id"),
        F.col("e.product_id").alias("product_id"),
        F.col("p.first_view").alias("first_view"),
        F.col("e.event_time").alias("purchase_time"),
    )
    timings = timings.groupBy("user_id", "session_id", "product_id", "first_view").agg(
        F.min("purchase_time").alias("first_purchase")
    )
    timings = timings.withColumn(
        "seconds_to_purchase", F.col("first_purchase").cast("long") - F.col("first_view").cast("long")
    )
    timing_summary = records(
        timings.agg(
            F.count("*").alias("matched_session_products"),
            F.expr("percentile_approx(seconds_to_purchase,0.5)").alias("median_seconds_to_purchase"),
        )
    )[0]
    sessions = s.sql("""SELECT user_id,session_id,min(CASE WHEN event_type='cart' THEN event_time END) first_cart,
        max(CASE WHEN event_type='purchase' THEN event_time END) last_purchase
        FROM events WHERE session_id IS NOT NULL GROUP BY user_id,session_id""")
    funnel = records(
        sessions.agg(
            F.count("first_cart").alias("cart_sessions"),
            F.sum(F.when(F.col("last_purchase") >= F.col("first_cart"), 1).otherwise(0)).alias(
                "converted_sessions"
            ),
        )
    )[0]
    # Real, bounded examples. Keep complete-session counts and flag timeline truncation.
    chosen = records(sessions.orderBy("user_id", "session_id").limit(10))
    journeys = []
    for example in chosen:
        frame = events.filter(
            (F.col("user_id") == example["user_id"]) & (F.col("session_id") == example["session_id"])
        )
        count = frame.count()
        journeys.append(
            {
                "user_id": example["user_id"],
                "session_id": example["session_id"],
                "total_actions": count,
                "timeline_truncated": count > 200,
                "timeline": records(
                    frame.orderBy("event_time", "source_row")
                    .select("event_time", "event_type", "product_id", "category", "price_minor")
                    .limit(200)
                ),
            }
        )
    inferred = events.groupBy("user_id", F.session_window("event_time", "30 minutes")).count()
    session_count = inferred.count()
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "months": [p.name for p in months],
        "accepted_rows": actual,
        "monthly": records(monthly),
        "behaviour": records(behaviour),
        "interests": records(interests),
        "top_products": records(products.orderBy(F.desc("views"), "product_id").limit(100)),
        "journeys": journeys,
        "session_funnel": funnel,
        "timing": timing_summary,
        "inferred_30min_sessions": session_count,
        "elapsed_seconds": round(time.monotonic() - started, 2),
        "definitions": {
            "conversion": "Distinct customers with a purchase at/after a view of the same product within the same observed session, divided by all distinct viewers of that product.",
            "returning": "Observed in an earlier imported month, not necessarily a long-term or newly acquired customer.",
            "sessions": "Source sessions use user ID plus session ID. Inferred sessions use a 30-minute inactivity gap.",
            "prices": "Sum listed purchase prices in unspecified source units; not complete order revenue.",
        },
    }
    output = root / "customer-report.json"
    temp = output.with_suffix(".json.tmp")
    temp.write_text(json.dumps(report, default=str, indent=2), encoding="utf-8")
    temp.replace(output)
    print(f"Published customer report for {actual:,} events across {len(months)} months", flush=True)
finally:
    s.stop()
