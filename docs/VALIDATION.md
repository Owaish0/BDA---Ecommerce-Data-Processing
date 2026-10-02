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

CI currently runs core and real PostgreSQL sink tests, syntax checks, and Compose parsing.
Full-stack CI and staged watermark tests remain to be added after local integration works.
Do not relabel skipped tests as passes or publish benchmark results without measurements.
