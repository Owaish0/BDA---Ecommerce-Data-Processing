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
