# Validation plan

## Verified locally

Core tests check purchase-only integer revenue, replayed IDs, half-open window boundaries,
sliding membership, invalid data, future timestamp poisoning, reproducible fixtures,
stable sampling, and exact user cardinality. These tests validate the independent oracle
and event contract. They do not prove Kafka/Spark/HDFS integration.

## Required integration gates

| Gate | Procedure | Pass condition |
|---|---|---|
| Startup | Clean Compose build and service checks | All persistent services healthy; expected Spark queries active |
| Kafka | Publish known fixture | Every delivery acknowledged; expected partition offsets |
| Spark schema parity | Send valid and invalid fixture variants | Python and Spark agree; invalid rows preserved in quarantine |
| Windows | Send small known event-time fixture | Counts/revenue agree exactly with oracle for both window types |
| Duplicate handling | Replay within watermark horizon | Silver and metric totals unchanged |
| Lateness | Advance watermark between controlled event batches | Within allowance included; too-late records counted/dropped as documented |
| Sink transaction | Isolated PostgreSQL tests | No partial rows after injected failure; retry succeeds; no stale overwrite |
| Query restart | Restart after committed batches | Same checkpoint resumes; counts unchanged for replay |
| DB outage | Stop Postgres during microbatch, restore | Failure visible; subsequent recovery correct |
| HDFS/Kafka restart | Restart without deleting volumes | Persistent data and checkpoints survive |
| Batch reconciliation | Stop producer, drain stages, snapshot silver | Exact sums and windows match database for accepted event set |
| Approximation | Compare approximate with exact distinct counts | Report measured error, not a strict 5% error guarantee |
| Sampling | Repeat seed and fraction | Same sampled IDs; report errors across seeds and sizes |
| Scaling | One/two workers; 100/500/1000 events/s | Results correct; hardware and throughput recorded |
| UI | Inspect live/empty/database-down dashboard | Readable charts and honest empty/error states |

CI runs 15 contract/workload/PostgreSQL tests, syntax checks, lint, and Compose parsing.
Full-stack CI builds Kafka, Hadoop, Spark and the dashboard, runs actual Spark window
and staged watermark tests, then verifies known input counts, checkpoint recovery,
snapshot reconciliation and browser rendering. Historical reports and extended outage
cases are being validated; consult PROGRESS.md for executed results.
Do not relabel skipped tests as passes or publish benchmark results without measurements.

## Performance experiments

With the stack running, stop the continuous producer and let existing work drain:

```sh
docker compose stop producer
python scripts/benchmark.py --rates 100 500 1000 --trials 3 --events 1000
```

The report is saved to `reports/benchmark.json`, with Spark cluster details and Docker
versions. It reports finite batch visibility time including producer container startup,
delivery and up to five seconds of polling. Percentiles describe batch trials, not
individual event latency. Three trials are only a classroom demonstration, not a
statistically strong capacity claim. Increase trials and workload duration for deeper work.

To compare workers, archive the first report, enable `docker compose --profile scale up
-d spark-worker-2`, verify two live workers in Spark, and repeat the same workload.
Keep machine resources and all settings fixed. Do not assume that another worker on the
same laptop improves performance. The benchmark refuses to run alongside the continuous
producer, and it appends data rather than deleting history.
