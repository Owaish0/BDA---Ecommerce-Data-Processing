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

## Current work

- Run 37043668154 passed all extended integration checks again. The actual demo ran for
  about 76 seconds, including 120 verified new events, but video.save_as ran after browser.close
  and failed. The raw WebM was retained in artifact 11242979032. Export order is now fixed.
  Rerun and inspect the final recording and one/two-worker benchmarks before claiming completion.
- Earlier outputs/CS404_ECommerce_Demo_Preview.mp4 is a design mockup, not an actual recording.
- User was asked to run scripts/demo.ps1 -Action Batch to populate the local historical panel.
- Unpublished changes: scripts/demo.ps1 and its docs; finalize and publish after current checks.

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

## Remaining completion work

1. Inspect run 37043668154; repair any demo or benchmark failure and rerun affected checks.
2. Download and inspect actual video, screenshots and benchmark reports; deliver usable artifacts.
3. Update README/validation evidence and publish the final Windows helper/docs.
4. Confirm local historical report if the user replies, then summarize the demonstrated features.
5. Stop the hourly build automation once all requested implementation and deliverable work is done.
