"""Three durable stages isolate deduplication from aggregate state."""

import os
import signal
import time
from pyspark.sql import functions as F
from ecommerce.spark_common import spark, parsed, metrics
from ecommerce.sink import publish, record_progress

session = spark("CS404 E-Commerce Streaming")
root = os.getenv("LAKE_ROOT", "hdfs://namenode:9000/ecommerce")
checkpoint = os.getenv("CHECKPOINT_ROOT", "hdfs://namenode:9000/checkpoints")
watermark = os.getenv("WATERMARK", "2 minutes")
queries = []
stopped = False


def start(writer, name, mode="append"):
    query = (
        writer.queryName(name)
        .outputMode(mode)
        .option("checkpointLocation", f"{checkpoint}/{name}")
        .trigger(processingTime="5 seconds")
        .start()
    )
    queries.append(query)
    return query


raw = (
    session.readStream.format("kafka")
    .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"))
    .option("subscribe", os.getenv("KAFKA_TOPIC", "ecommerce.events.v1"))
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "true")
    .option("maxOffsetsPerTrigger", os.getenv("MAX_OFFSETS_PER_TRIGGER", "10000"))
    .load()
    .select(
        F.col("value").cast("string").alias("raw_json"),
        "topic",
        "partition",
        "offset",
        F.col("timestamp").alias("ingested_at"),
    )
)
raw_schema = raw.schema
start(raw.writeStream.format("parquet").option("path", f"{root}/bronze"), "bronze-v1")

# Wait for the initial directory because downstream file sources require it at startup.


def wait_path(path):
    started = time.monotonic()
    hadoop_path = session._jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(session._jsc.hadoopConfiguration())
    while not fs.exists(hadoop_path):
        if time.monotonic() - started > 120:
            raise RuntimeError(f"Timed out waiting for {path}; start the producer and inspect query logs")
        for q in queries:
            if q.exception():
                raise RuntimeError(str(q.exception()))
        time.sleep(1)


wait_path(f"{root}/bronze")
bronze = session.readStream.schema(raw_schema).parquet(f"{root}/bronze")
events = parsed(bronze)
invalid = events.filter(~F.col("valid")).select("raw_json", "topic", "partition", "offset", "ingested_at")
start(invalid.writeStream.format("parquet").option("path", f"{root}/quarantine"), "quarantine-v1")
silver = (
    events.filter("valid")
    .drop("valid", "raw_json")
    .withWatermark("event_ts", watermark)
    .dropDuplicatesWithinWatermark(["event_id"])
)
silver_schema = silver.schema
start(silver.writeStream.format("parquet").option("path", f"{root}/silver"), "silver-v1")
wait_path(f"{root}/silver")
for name, size, slide in [
    ("sales-1m-v1", "1 minute", "1 minute"),
    ("trending-5m-v1", "5 minutes", "1 minute"),
]:
    source = (
        session.readStream.schema(silver_schema)
        .parquet(f"{root}/silver")
        .withWatermark("event_ts", watermark)
    )
    result = metrics(source, size, slide)
    start(
        result.writeStream.foreachBatch(lambda frame, epoch, n=name: publish(frame, epoch, n)), name, "update"
    )


def stop(*_):
    global stopped
    stopped = True


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)
last_ids = {}
try:
    while not stopped:
        for query in queries:
            if not query.isActive:
                raise RuntimeError(f"Query stopped: {query.name}: {query.exception()}")
            progress = query.lastProgress
            if progress and last_ids.get(query.name) != progress["batchId"]:
                record_progress(query.name, progress)
                last_ids[query.name] = progress["batchId"]
        time.sleep(2)
finally:
    for query in reversed(queries):
        query.stop()
    session.stop()
