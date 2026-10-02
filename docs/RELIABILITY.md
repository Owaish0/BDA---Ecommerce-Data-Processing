# Reliability boundaries and recovery

## Delivery and persistence

The producer enables Kafka idempotence and waits for acknowledgements. Delivery errors
terminate the workload with a nonzero exit code rather than reporting success. Topic
retention is seven days. Bronze preserves original payloads and Kafka coordinates.
Kafka `failOnDataLoss=true` makes missing offsets visible rather than silently skipping them.

Spark file sinks use checkpoint metadata and durable HDFS paths. Dedicated checkpoint
directories belong to stable query names. PostgreSQL publishing uses an advisory lock,
absolute aggregate values, and a microbatch ledger in one transaction. A failed batch
rolls back both metric rows and ledger. A committed batch retry is drained and skipped.
These provide idempotent publication for a stable query/checkpoint identity. They do not
justify an unconditional end-to-end exactly-once claim for arbitrary external systems.

## Event time

Validation rejects unsupported schemas, invalid identifiers, missing timezones, invalid
event types, impossible quantities/prices, and event timestamps more than five minutes
ahead of Kafka ingestion time. Invalid raw records retain Kafka coordinates in quarantine.

The watermark is two minutes behind the maximum observed event time. Data within the
allowance can update windows. Older data may be discarded by stateful operators; raw
bronze remains available for forensic inspection and historical rebuilding. Watermarks
advance on event-time observations, not wall-clock time. An idle stream does not finalize
all windows by itself. Current dashboard windows are provisional.

`dropDuplicatesWithinWatermark(event_id)` bounds deduplication state. Duplicates outside
the retained horizon are not promised to be globally suppressed. An exact historical
query deduplicates IDs again; differences from a streaming run must be analysed instead
of hidden. Contradictory payloads sharing an ID currently have first-observed semantics;
the historical job audits canonical payloads in bronze and fails if an ID has multiple
payload variants. Conflicts are written to a Parquet audit report for investigation.

## Recovering services

1. Preserve Docker volumes and checkpoints.
2. Restart the failed service; do not reformat the NameNode.
3. Inspect streaming errors and PostgreSQL commit ledger.
4. Verify metrics and Kafka positions using the recorded fixture.

The HDFS entrypoint formats only when a new empty NameNode volume has no VERSION file.
Deleting a checkpoint and retaining the old database ledger is unsupported: epoch IDs
restart, so writes could be skipped. A full rebuild needs a new query identity plus new
metric namespace or a deliberate, documented migration.

## Laptop topology versus production

The demonstration runs one Kafka broker and one HDFS DataNode with replication one.
This tolerates process restarts when storage survives, not disk or host loss. HDFS
permissions are disabled inside the isolated Compose network for a classroom prototype.
Only dashboard, Spark UI, and HDFS UI are published, bound to loopback. Production would
require TLS, authentication, least-privilege database roles, multi-node replication,
backups, and retention/compaction policies. No remote deployment is authorised or configured.

## Observability

Query progress stores batch IDs, input rates, processing rates, trigger durations,
watermarks, state rows, and late-row counters. Trigger duration is not end-to-end latency.
Dashboard stale-progress warnings distinguish missing activity from proof of failure;
check service logs before declaring a query down. Database progress failures currently
fail the stream service and rely on checkpoint recovery; this is explicit, not silent.
