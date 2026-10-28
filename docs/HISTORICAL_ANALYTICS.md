# Historical customer analytics

## Scope and status

The platform has two distinct data paths:

- Replay: the existing million-row sample -> Kafka -> Spark Streaming -> HDFS -> PostgreSQL.
- History: complete monthly source archives -> validated, date-partitioned Parquet -> Spark customer analytics -> small dashboard reports.

Historical results never include replayed copies. The first deployment targets October 2019. Importing the remaining six months is gated on October storage and processing measurements. Do not describe the seven-month import as completed until all seven manifests exist and have been reconciled.

## Import one month

Install the optional dependencies in a project-local environment: `pip install -e ".[history,test,pipeline]"`.

Run `python scripts/import_history.py --month 2019-Oct` from the repository root. Kaggle is the default archive provider; `--provider rees46` uses the publisher's gzip mirror. Both are streamed to compressed archives, then read without extracting a full CSV.

The default free-space reserve is 20 GiB. The importer checks available space throughout download and conversion. Interrupted downloads resume only with a strong matching ETag and correct byte range; otherwise downloading restarts. Interrupted conversions restart from the compressed archive, removing only their own staging output. A validated month is atomically published and is skipped on a repeated run.

Archive CRC, SHA-256, accepted/rejected counts, source date range, event counts, Parquet bytes, elapsed conversion time and minimum observed disk space are recorded. SHA-256 is a local fingerprint, not verification against a publisher-provided digest. Rejected records retain their source row and reason. Source IDs include the month and row number; identical-looking activity at different source rows is not automatically a duplicate.

`--input-gzip path` accepts either a gzip or ZIP file for tests/local imports; its manifest explicitly says that full-month coverage is unverified. The isolated `data/history-fixture` directory is for integration tests and is never the production dashboard source.

No archives are automatically deleted. After a successful full-month reconciliation, compressed inputs may be removed separately to recover space. Keep the processed Parquet and manifest. A stale import lock may only be removed after confirming no importer is running.

## Analyse validated history

For an 8 GiB Docker allocation, stop producer, streaming, Spark workers, batch worker and HDFS services before heavy historical processing if they are running. Preserve all volumes. PostgreSQL and the dashboard can remain running. The historical job uses local Spark with two cores, a 3 GiB driver and a 6 GiB container memory ceiling. This is resource-limited local Spark, not a claim of multi-machine processing.

Run:

```
docker compose -f compose.yaml -f compose.history.yaml run --rm --no-deps --build historical
python scripts/benchmark_algorithms.py
```

Check host free space before running Spark; the importer reserve guard does not yet supervise Spark's spill files. Historical processing time and spill usage must be measured on October before extending to all months. `data/history/spark-tmp` holds temporary Spark files.

The dashboard reads only atomically published JSON reports. It shows an honest not-ready state until a report exists. Failed jobs leave the previous complete report intact.

## Metric definitions

- Active customer: distinct user ID with any action in a month.
- Returning customer: observed in an earlier **imported** month, not necessarily acquired during this dataset.
- Customer activity columns overlap: a person can view, cart and purchase.
- Product conversion: distinct people with a purchase at/after a view of the same product in the same source session, divided by all distinct viewers of the product.
- Cart-session conversion: user/session pairs with a purchase at/after their first cart event, divided by user/session pairs with a cart event. This does not require the same product.
- Time to purchase: first observed view to first subsequent purchase for a user/session/product, excluding unmatched cases.
- Source sessions and inferred 30-minute inactivity sessions are different measures.
- Price totals sum recorded purchase prices in source units; no complete order quantities or confirmed currency are available.
- Product page contains the 100 most-viewed products. Example journeys are a deterministic set of 10 sessions, capped at 200 displayed actions with truncation labelled.

## Algorithms

Production: tumbling/sliding replay windows, approximate distinct counts (HyperLogLog++), historical session windows, and the existing stable hash sample in replay-history reports.

Bounded experiment: Count-Min Sketch frequency estimates versus exact counts, reservoir sampling, and Bloom-filter membership probes over up to 100,000 imported records. Exact counting selects comparison candidates; this is not a production constant-memory top-k implementation. Bloom results never discard events. Reservoir samples only the experiment stream, not all seven months.

Punctuated windows and sketch-based sampling remain planned. Completed-month manifests are availability markers, not a claim that either algorithm is implemented.

## Validation commands

```
python -m pytest -q
ruff check .
python -m compileall -q src jobs dashboard scripts
docker compose exec -T dashboard python /app/scripts/check_navigation.py
```

A 10,000-record real-source fixture has been checked with `scripts/check_history_fixture.py` against Spark output: totals, distinct customers, purchase-price sum, session funnel and displayed product conversion counts. `check_navigation.py --fixture` exercises the populated historical pages without publishing fixture data to users.

The October milestone can be supervised with `python scripts/finish_october.py` while an October import is already running. It waits for the validated manifest, checks measured storage headroom, runs Spark with periodic memory samples, stops its own container if disk reserve is crossed, and then runs the bounded algorithm experiment. It writes `data/history/pipeline-status.json` and `resource-measurements.json`. It does not automatically start November-April. This process must stay running, and the machine must remain awake; it is not a scheduled service.

### Repeatable demonstrations

Overview has independent Full historical dataset, Live replay (real sample), and Synthetic data views. Viewing a source does not start its producer. Replay controls exposes the event source selector directly. Once the real sample finishes and pending deliveries clear, Replay sample again starts a new numbered run. Each new run has distinct event and session IDs; retries within the run retain their IDs through the pending-delivery outbox. Old results are preserved, live real totals include all demonstration runs, and the full historical report is unchanged. Pause and Start resume the current run. Synthetic generation has no fixed sample limit.
