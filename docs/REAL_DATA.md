# Real e-commerce event replay

Source: [eCommerce behavior data from multi category store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store),
published by mkechinov, collected by [REES46 / Open CDP](https://rees46.com).
The publisher permits free use with links to the dataset and REES46. No raw dataset
is committed to this repository.

## Prepare the bounded sample

From the project folder with Python and this package installed:

```sh
python scripts/prepare_real_dataset.py --rows 1000000
```

The importer reads the first member of the official October 2019 ZIP as a stream,
then closes the download at one million rows rather than downloading the whole
1.7 GB archive / 5.7 GB CSV. The compressed-byte limit is 256 MiB. It verifies the
expected archive member, parses rows and checks price precision and required fields.
It does not validate the full archive CRC because only a prefix is downloaded.
The retained CSV has a local SHA-256 checksum in `data/rees46-manifest.json`.
An already-downloaded official CSV can be supplied with `--csv PATH`.

Prepared files, all ignored by Git:

- `data/rees46-sample.csv`: original source fields for the selected prefix.
- `data/rees46.sqlite`: indexed normalized events for bounded-memory replay.
- `data/rees46-manifest.json`: provenance, counts, dates and checksum.

The sample prepared on 26 October 2026 contains 1,000,000 accepted records:
968,513 views, 14,639 cart events and 16,848 purchases. The original time range is
2019-10-01 00:00:00 through 16:56:07 UTC. This is a chronological prefix, not a
random or representative sample of the complete dataset.

## Run and demonstrate

For a fresh stack, set `DEFAULT_SOURCE=real` in the ignored `.env`. For an existing
stack, pause generation and use **Event source → Real dataset replay → Use selected
source**, then **Start events**. Use the existing speed control. The default is 100/s,
so replaying a million records takes approximately 2 hours 47 minutes at that rate.
The source stops at the end and never silently repeats the dataset.

Original timestamps stay in `source_event_time` in Bronze and Silver. `event_time`
is a separately assigned replay timestamp so the live windows are current. Playback
preserves file order but intentionally changes event spacing; charts describe replay
activity, not the historical store's actual minute-by-minute traffic. Changing speed
changes replay-time windows. Historical batch analysis uses the records already
processed into Silver, not the entire prepared million-row file automatically.
Historical session funnels use original source timestamps so replay pacing does not
change cart/purchase ordering.

## Honest metric definitions

- The retrieved publisher metadata does not specify currency. Display source units,
  without labelling them INR/USD or applying an invented conversion.
- Source quantities and order IDs are absent. The adapter uses an internal weight of
  one and marks `quantity_observed=false`. The sum of recorded purchase prices is a
  purchase-value proxy, not verified order revenue. Mean recorded purchase price is
  not average order value. Purchase records are not necessarily distinct orders.
- The legacy `price_paise` / `revenue_paise` fields store integer hundredths for both
  modes. In real mode these are hundredths of source units, not INR paise. Event
  currency metadata is `SOURCE_UNITS`; the UI uses source-specific labels.
- Real product/user/session IDs are namespaced with `rees46:`. Dashboard queries and
  historical reports filter real and synthetic products separately before summing
  values. A source switch never mixes currencies or resets stored analytics.
- Missing category codes fall back to source category IDs. Missing sessions receive
  a marked row-specific placeholder and are excluded from historical session funnels.
- Stable IDs derive from source file and row number, not payload hashes; two genuine
  identical source rows remain distinct events. Original source IDs remain reversible
  through the documented `rees46:` namespace.
- Synthetic flash-sale, duplicate, invalid and late workloads are disabled in real
  mode at both the dashboard and command-queue layer.

## Resume and reliability

The replay cursor and an in-flight event batch are persisted in PostgreSQL. The cursor
advances only after Kafka acknowledges the entire batch. Retried pending events retain
their IDs and replay timestamps. This is at-least-once delivery; Spark's watermark
deduplication handles timely retries, not an unlimited exactly-once guarantee. A
delivery failure pauses generation and is visible on the dashboard. Source changes
are blocked while a pending batch or control action exists.

`python scripts/check_real_replay.py` inside the dashboard container pauses replay,
compares streaming totals with the indexed source prefix, verifies mode isolation,
runs a real historical job, and restores the original generator settings. The script
requires a dedicated real-data instance whose replay cursor matches its stored lake.

## Verified locally on 26 October 2026

All 26 adapter/regression/database tests passed. A real runtime check reconciled
23,500 replayed events, 317 purchase records and 85,990.57 source units against the
prepared SQLite source, both for streaming metrics and the historical batch report.
Session-funnel results matched an independent calculation using original timestamps.
Pause, source switching, cursor preservation and rejection of artificial scenarios in
real mode were exercised. The dashboard rendered without errors and passed its HTTP
health check. Replay resumed at its original 100 events/second setting after tests.
