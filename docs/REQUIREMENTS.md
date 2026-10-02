# Requirements and acceptance criteria

The scope is the previously discussed CS404 project. No additional professor syllabus
has been supplied; the user confirmed these topics are enough.

| Topic | Implementation | Acceptance evidence required |
|---|---|---|
| Synthetic events | Configurable Python producer, versioned schema, seeded fixtures | Repeatable fixtures; schema tests; real Kafka deliveries |
| Kafka ingestion | KRaft broker, three partitions, topic bootstrap, idempotent producer | Topic inspection, input/output counts, restart test |
| Distributed Spark | Standalone master and worker; optional second worker | Executor/task evidence in Spark UI and scaling experiment |
| PySpark and Spark SQL | Shared DataFrame transforms and historical SQL | Oracle comparison and SQL result artifacts |
| Structured Streaming | Independent bronze, silver, quarantine, sales, and trend queries | Actual query progress and dashboard results |
| Tumbling windows | 1-minute sales windows | Boundary fixture agrees with oracle |
| Sliding windows | 5-minute size, 1-minute slide | Each event belongs to the expected five windows |
| Event time and watermarks | UTC; 2-minute allowance; future timestamp quarantine | Controlled inside/outside watermark tests |
| Deduplication | Event ID dedup within watermark; exact historical dedup | Duplicate scenario leaves sums unchanged within supported horizon |
| Batch and streaming | Silver historical Parquet; batch window queries | Snapshot reconciliation for identical event sets |
| HDFS and Parquet | Persistent NameNode/DataNode, bronze and silver lake | Files visible and survive container restarts |
| Aggregations | Counts, sums, revenue, AOV, category/product activity | Purchase-only revenue and integer-money tests |
| Trending products | View ranking over recent sliding windows | Burst scenario changes product ranking |
| User behaviour | Views, carts, purchases; historical session funnel | Correct definitions and controlled funnel fixture |
| Sampling | Stable hash sample; inverse-probability estimates | Repeatable sample membership, exact-vs-estimate report |
| Approximate algorithms | Approximate distinct users with RSD 0.05 | Compare with exact cardinality across workloads |
| Dashboard | Streamlit, Plotly, database metrics, query health | Browser inspection including empty and unavailable states |
| Reliability | Checkpoints, atomic ledger/upserts, malformed-data quarantine | Retry, interruption, DB outage, Kafka recovery tests |
| Performance | Rates, durations, state metrics; workload experiments | Recorded hardware, versions, inputs, throughput, latency percentiles |
| Submission | Abstract, documentation, demo sequence | Commands reproduced by team on fresh environment |

Average order value assumes a single purchase event is a single-product order.
Revenue counts quantity × price only for purchases. Multi-item order consolidation,
refund accounting, real payments, and customer PII are outside this project scope.
The session funnel is observational, not a causal conversion analysis.
