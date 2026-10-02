# Project progress

Updated 2 October 2026 (Asia/Kolkata).

## User decisions

- Build all previously discussed BDA topics; no heavy ML.
- Public GitHub repository confirmed: Owaish0/BDA---Ecommerce-Data-Processing.
- Docker is installed per-user at C:/Users/hacke/AppData/Local/Programs/DockerDesktop.
- Hourly continuation and verified-change commits/pushes requested.
- Hourly chat heartbeat created: build-and-checkpoint-cs404-project.

## Implemented, not yet fully integrated

- Versioned contract, seeded producer, normal/burst/duplicate/late/invalid scenarios.
- Exact independent window oracle and deterministic hash sampling.
- Spark bronze raw storage, schema checks, quarantine, watermark dedup, silver storage.
- Tumbling sales and sliding trends; transactional PostgreSQL upserts and commit ledger.
- Batch SQL, exact/approx cardinality, sampling estimates, observed session funnel.
- Streamlit live dashboard and query health.
- Compose stack, persistent volumes, Spark master/worker, optional second worker.
- Windows bootstrap, explicit Git checkpoint script, CI, requirements and reliability docs.

## Evidence

- Run 37039777345 built the corrected Spark image and passed real Spark window/oracle,
  staged duplicate suppression, and within/beyond-watermark tests.
- That run processed 1,100 events through Kafka/Spark/HDFS/PostgreSQL, recovered from
  streaming restart, and passed snapshot reconciliation. The browser test failed by
  checking for the chart before it rendered; explicit waits and failure screenshots added.
- Latest local tests: 13 passed, 2 PostgreSQL tests skipped locally (previous CI passed both).
- Historical conflict audit and producer scenario contract tests added; full CI rerun pending.
- Commit b594632 passed all 15 CI tests and lint. Its full-stack run is 37041756610.
- Local rebuild reached HDFS initialization but failed; NameNode inspection subsequently
  showed safe mode off and one healthy DataNode. Added bounded initialization retries and
  automatic startup diagnostic logs. Awaiting user's rebuild result.
- Extended integration cases for PostgreSQL outage, Kafka/HDFS restarts, duplicates and
  invalid events, plus a finite-batch performance harness. These additions await CI execution.
- Full run 37041756610 PASSED: all services, 1,100 accepted events, checkpoint restart,
  exact reconciliation, historical report and real browser rendering. Evidence downloaded
  to outputs/integration-evidence (outside the repository).
- User rebuilt successfully. Local Spark master reports a running application with two
  executor cores; all five queries have recent progress. Local dashboard displayed 15,968
  events and 1,602 purchases at 17:48 UTC while catching up on earlier queued data.
- Extended recovery run 37042717024 is in progress for commit 93dd610.
- Actual browser video recording added to CI, pending execution; earlier MP4 remains a mockup.

- Nine core unit tests passed locally.
- Two PostgreSQL sink tests skipped locally because no isolated database is available.
- Python syntax checks passed.
- Ruff formatting applied and lint check passed before the latest documentation additions.

## Current access issues

- Docker CLI executable access denied in this session even after requesting read access.
  User asked to run docker version in normal PowerShell. Do not work around denied access.
- Git credential helper has no usable GitHub credential. GitHub connector has repository
  write access and can publish commits through its Git tree/commit/ref APIs.
- Initial code published and verified at commit a01582acf1733d17a0bf35d07e19333127c87422.
- GitHub CI run 37037312901 passed core tests and both real PostgreSQL transaction tests,
  Python syntax checks, and Docker Compose validation on Linux.
- Adding full-stack integration CI so actual Spark/Kafka/HDFS can be tested without local Docker access.
- Full-stack run 37037808077 built all images and passed real Spark SQL window/oracle tests,
  but failed before metrics publication because Spark's runtime Python could not import psycopg.
- Fixing interpreter consistency (python3 -m pip and explicit driver interpreter), adding build-time import check.
- Build check caught a second packaging issue: the Spark image's pip/backend generated an
  UNKNOWN package and silently omitted optional dependencies. Pinning compatible build tooling,
  installing with no build isolation, and checking imports at image build time.
- User started local bootstrap; Hadoop download is still in progress.

## Next work

1. Resolve Docker session access; publish through the authorised GitHub connector.
2. Validate Compose and official image tags; build and fix actual service startup issues.
3. Run real Spark schema/window tests, sink tests, staged late-data and restart fixtures.
4. Add snapshot reconciliation, benchmark harness and full integration CI.
5. Inspect dashboard in browser and refine usability and product labels.
6. Validate retention, failure recovery, conflicting duplicate-ID handling, and backlog behaviour.
7. Record actual demo video after all implementation gates pass.

Continue independently where possible; distinguish implemented code from executed evidence.
Do not claim completion merely because files exist. Do not force-push or commit secrets.
