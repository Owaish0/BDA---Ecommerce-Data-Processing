# Project progress

Updated 2 October 2026 (Asia/Kolkata).

## Confirmed scope

Build the complete CS404 e-commerce analytics project, without heavy ML. Public repository:
Owaish0/BDA---Ecommerce-Data-Processing. User authorized publication, hourly checkpoints,
and continuation after quota resets. Active hourly automation: build-and-checkpoint-cs404-project.

## Implemented and verified

- Kafka synthetic event ingestion, three partitions, seeded workload scenarios.
- PySpark validation, bronze/quarantine/silver Parquet on HDFS, bounded duplicate suppression.
- Structured Streaming tumbling and sliding windows, event-time watermarks and checkpoint recovery.
- Atomic PostgreSQL publication with a transaction ledger and absolute-value upserts.
- Spark SQL historical totals, hash sampling, approximate distinct users and observed session funnel.
- Exact snapshot reconciliation and historical conflicting-ID audit.
- Streamlit dashboard with actual database metrics, charts, historical reports and query progress.
- Docker Compose, Spark standalone worker, optional second worker, Windows startup helpers.

## Executed evidence

- All 15 contract/workload/database tests passed in GitHub. The two database tests are skipped
  locally because no isolated PostgreSQL test database is exposed to the host.
- Run 37041756610 passed the full stack with 1,100 events, restart recovery, reconciliation,
  historical report and browser rendering. Downloaded evidence: outputs/integration-evidence.
- Run 37042717024 passed the extended full-stack checks with exactly 1,585 accepted events.
  PostgreSQL outage, Kafka restart, HDFS restart, duplicates and invalid inputs all recovered
  with correct totals. Reconciliation and historical/browser checks passed afterwards.
  Observed recovery scenario durations: PostgreSQL 35.298s, Kafka 34.970s, HDFS 65.684s.
  These include service actions, input publication and polling; they are not production SLAs.
- Real Spark tests passed independent tumbling/sliding oracle comparisons and staged
  within/beyond-watermark cases, including duplicate suppression.
- User rebuilt locally after HDFS initialization retries were added. Local Spark has one
  live worker and two executor cores; all five streaming queries show recent progress.
  Local dashboard reached 85,324 events in the selected 15-minute horizon while processing
  its earlier backlog. No fabricated dashboard data is used.
- Windows demo helper argument forwarding verified with a mock executable (Batch/Burst).

## Completion status

- Full workflow 37045071846 PASSED for implementation commit 9104f75: tests, real Spark,
  extended integration/recovery, actual video export and both worker benchmarks.
- Final evidence downloaded to outputs/final-verification. The actual named WebM was
  converted to outputs/CS404_Actual_Project_Demo.mp4. Earlier mockup remains separately named.
- Measured results are in docs/RESULTS.md and outputs/CS404_Test_Report.md. Two workers
  allocated four executor cores; the small finite workloads showed no consistent speedup.
- Windows demo helper is published and its forwarding checks passed.
- The local live pipeline is verified. User confirmation of the optional local historical
  batch remains pending; the historical job already passed in CI.
- No further implementation work is required for the agreed classroom scope. Hourly work
  can remain quiet unless the user reports a local issue or requests changes.

## Access and operational constraints

- Local Docker executable access is denied to this session despite working in user PowerShell.
  Do not bypass this restriction. Use GitHub Actions for Docker execution and ask user only
  for required local actions. Docker location: LOCALAPPDATA/Programs/DockerDesktop/resources/bin.
- GitHub connector can publish with create_tree/create_commit/update_ref. Preserve the current
  remote tree and parent; never force-push. Local Git push has no credentials.
- Public fetch works with git -c http.sslBackend=openssl fetch origin main when network is granted.
- Never publish .env, credentials, local data or generated reports containing unintended data.
- Kafka/HDFS replication is one for this laptop project. Service recovery is verified;
  host/disk-loss tolerance, production authentication and backup restore are outside this topology.

## Handover

Use docs/DEMO.md for the professor demonstration and docs/RESULTS.md for evidence.
Do not rerun the full suite for documentation-only changes. Preserve checkpoints and volumes.
Respond to the pending local batch result if the user supplies it; do not repeatedly ask.
