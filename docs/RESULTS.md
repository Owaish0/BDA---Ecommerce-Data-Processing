# Verified results

Verified implementation: commit `9104f75bb7ad74652ad1c5562f5c5119d6eee2e7`.
[Complete successful workflow](https://github.com/Owaish0/BDA---Ecommerce-Data-Processing/actions/runs/37045071846).

## Correctness and recovery

- All 15 contract/workload/PostgreSQL tests, syntax, lint and Compose checks passed.
- Real Spark window aggregates matched the independent oracle; staged duplicate and
  within/beyond-watermark tests passed.
- The full Kafka/Spark/HDFS/PostgreSQL pipeline accepted exactly 1,585 events after
  controlled normal, duplicate and invalid workloads, streaming restart, database outage,
  Kafka restart and HDFS restart. Exact historical reconciliation passed for both window types.
- Historical analysis and actual browser rendering passed. The video test then published
  120 additional burst events and verified the corresponding increase in database metrics.
- Recovery scenario durations in the final run: PostgreSQL 34.002s, Kafka 35.007s,
  HDFS 71.526s. Durations include service actions, publication and polling, not only downtime.

## Finite workload comparison

Each trial sent 300 events; three trials were run at each requested rate on one then two
Spark workers. All counts matched. The snapshot confirms two allocated executor cores
for one worker and four allocated executor cores across two workers, with 1 GiB per executor.
The driver used 1 GiB, four shuffle partitions and five-second streaming triggers.
Host: Linux 6.17.0-1022-azure x86_64; Docker client/server 28.0.4; Spark 3.5.6.
Physical host CPU/RAM capacity was not captured. These are classroom CI measurements.

| Requested events/s | One worker median (s) | Two workers median (s) | One worker p95 (s) | Two workers p95 (s) |
|---:|---:|---:|---:|---:|
| 100 | 19.07 | 24.54 | 24.14 | 24.69 |
| 500 | 21.76 | 21.75 | 22.12 | 21.86 |
| 1,000 | 16.27 | 21.36 | 21.46 | 21.47 |

These times measure launch of a finite producer workload until every event is visible in
sales metrics. They include producer startup, delivery, multiple streaming stages and up
to five seconds of polling. They are not per-event latency or sustainable capacity.
With only three samples, nearest-rank p95 is the largest observation. The second worker
provided no consistent improvement for these small workloads. Shared-host contention,
scheduling and trigger timing are possible explanations; this experiment does not isolate them.

## Local Windows check

After rebuilding, the local dashboard displayed real changing counts, charts and recent
progress for all five queries. The Spark master showed an active application with two
executor cores. The user can populate the historical panel with:

```powershell
.\scripts\demo.ps1 -Action Batch
```

The local historical batch still awaits user confirmation; the same job passed in CI.

## Artifacts and demonstration

Download the `integration-evidence` artifact from the successful workflow for raw benchmark
JSON, screenshots, service logs, integration results and `demo/CS404-actual-dashboard.webm`.
The accompanying local deliverables include an MP4 conversion, abstract DOCX/PDF, and a
verification report. The earlier preview MP4 is a design mockup; use the actual recording.
See [demonstration guide](DEMO.md) for the eight-to-ten-minute viva sequence.
