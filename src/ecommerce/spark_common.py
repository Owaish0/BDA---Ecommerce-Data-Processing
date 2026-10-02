"""Shared schema and expressions; transformations are used in stream and batch jobs."""

import os
from pyspark.sql import SparkSession, functions as F, types as T

EVENT_SCHEMA = T.StructType(
    [
        T.StructField("schema_version", T.IntegerType()),
        T.StructField("event_id", T.StringType()),
        T.StructField("event_time", T.StringType()),
        T.StructField("user_id", T.StringType()),
        T.StructField("session_id", T.StringType()),
        T.StructField("event_type", T.StringType()),
        T.StructField("product_id", T.StringType()),
        T.StructField("product_name", T.StringType()),
        T.StructField("category", T.StringType()),
        T.StructField("quantity", T.LongType()),
        T.StructField("price_paise", T.LongType()),
    ]
)


def spark(name):
    return (
        SparkSession.builder.appName(name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
        .config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def parsed(raw):
    frame = raw.withColumn("e", F.from_json("raw_json", EVENT_SCHEMA)).select("*", "e.*").drop("e")
    frame = frame.withColumn("event_ts", F.to_timestamp("event_time"))
    valid = (F.col("schema_version") == 1) & F.col("event_type").isin("view", "search", "cart", "purchase")
    for key in ["event_id", "user_id", "session_id", "product_id", "category"]:
        valid = valid & F.col(key).isNotNull() & (F.length(F.trim(F.col(key))) > 0) & (F.length(key) <= 128)
    valid = (
        valid
        & F.col("event_ts").isNotNull()
        & F.col("event_time").rlike(r"(Z|[+-]\d{2}:\d{2})$")
        & F.col("quantity").between(1, 1000)
        & F.col("price_paise").between(0, 100_000_000)
        & (F.col("event_ts") <= F.col("ingested_at") + F.expr("INTERVAL 5 MINUTES"))
    )
    return frame.withColumn("valid", F.coalesce(valid, F.lit(False)))


def metrics(frame, size="1 minute", slide="1 minute"):
    frame = frame.withColumn(
        "revenue",
        F.when(F.col("event_type") == "purchase", F.col("quantity") * F.col("price_paise")).otherwise(
            F.lit(0)
        ),
    )
    return frame.groupBy(F.window("event_ts", size, slide), "category", "product_id").agg(
        F.count("event_id").alias("events"),
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
        F.sum("revenue").alias("revenue_paise"),
        F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("views"),
        F.sum(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias("carts"),
        F.approx_count_distinct("user_id", 0.05).alias("active_users_approx"),
    )
