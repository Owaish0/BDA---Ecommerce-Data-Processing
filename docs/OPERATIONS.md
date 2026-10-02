# Operational checks

## Inspect the runtime

Use `docker compose ps` for containers and `docker compose logs --tail=100 streaming`
for failed Spark queries. Spark query progress appears in the dashboard. Open the Spark
UI to verify executors and tasks; a dashboard alone does not prove distributed processing.

HDFS files:

```sh
docker compose exec namenode hdfs dfs -ls /ecommerce
docker compose exec namenode hdfs dfs -ls /checkpoints
docker compose exec namenode hdfs dfs -du -h /ecommerce
```

Quarantine retains original payloads and Kafka topic/partition/offset for investigation.
Inspect a Parquet file with Spark, not a text viewer.

## Reconciliation

Stop the producer. Wait until bronze, silver and aggregate input rates are zero and
their last processed inputs correspond to the known fixture. Then run:

```sh
docker compose run --rm --no-deps batch /opt/spark/bin/spark-submit --master 'local[2]' /app/jobs/reconcile.py
```

This compares exact columns for both tumbling and sliding windows against a unique
silver snapshot. It intentionally fails on mismatches. Do not subtract away differences
caused by upstream watermark drops, inconsistent IDs, or a moving snapshot.

## Capacity and durability

The Kafka retention setting controls seven days of topic history. HDFS and PostgreSQL
currently retain course-project history without automatic deletion. Monitor disk usage
and stop workloads if storage is exhausted. Deleting arbitrary Parquet files can invalidate
streaming file-sink metadata, so archival/retention needs a deliberate migration.

Take consistent backups of PostgreSQL, HDFS NameNode/DataNode volumes, Kafka storage,
and Spark checkpoint directories before disruptive changes. A backup is not verified
until a restore has been tested. The laptop topology offers no protection from host loss.

For a source/schema migration, create a new topic/version and new query checkpoint identity.
For a metric-state change, create a new metric namespace. Preserve the previous database
ledger until its checkpoint identity is retired.
