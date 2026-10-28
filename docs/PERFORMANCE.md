# Performance tuning

Resource limits are configurable in `.env`. The local profile tested with Docker exposing 32 logical CPUs and about 11.7 GiB RAM is:

```dotenv
SPARK_WORKER_CORES=6
SPARK_WORKER_MEMORY=5g
SPARK_EXECUTOR_MEMORY=4g
SPARK_DRIVER_MEMORY=2g
STREAM_TRIGGER=2 seconds
```

Recreate the Spark worker and streaming service after changing resources. Rebuild streaming, dashboard and producer after source changes. The repository example keeps conservative defaults for smaller machines. Allow memory for Kafka, PostgreSQL, HDFS and the dashboard; the optional second worker needs additional memory.

Streaming queries use separate fair scheduling pools. PostgreSQL metric updates use batches of up to 1,000 rows within the existing transaction and retry ledger. The dashboard supports requested input rates up to 2,000 events/sec; this is a control limit, not a guaranteed processing rate.

## Measurements on 28 October 2026

- An isolated PostgreSQL test with 3,000 aggregate rows had median write times of 1.252 seconds before batching and 0.167 seconds after batching (about 7.5 times faster). Transaction rollback and retry tests passed, including failure after the first bulk chunk.
- With six cores and a four GiB executor, reducing the trigger interval from five to two seconds reduced the mean time for all 5,000 synthetic events to become visible at a requested 1,000 events/sec from 25.22 to 19.74 seconds (two trials each).
- A 60.65-second real replay observation delivered 57,499 records at a requested 1,000 events/sec. Observed nonempty batch processing rates were approximately 1,325 rows/sec for bronze, 985 for silver, 832 for sales and 760 for trending. These stages run concurrently and their rates must not be added together.

Finite tests include container startup, delivery and polling. The real observation is short and does not establish a sustainable maximum. Sales and trending were slower than the input during this observation; for longer runs, monitor lag and lower the input rate if it grows. Event generation was returned to paused after testing, with the requested rate saved at 1,000.

Local verification: 36 tests passed and three database tests were skipped by the local suite; those three passed separately against isolated PostgreSQL. Dashboard navigation verification passed Overview and Replay controls, then timed out on a later page under load; complete navigation verification remains outstanding.

Run `scripts/benchmark.py` for finite synthetic trials with the normal producer stopped. `scripts/observe_live_performance.py` runs inside a configured application container, briefly enables real replay, restores the previous enabled state, retains the requested rate and writes `/tmp/performance-live.json`.
