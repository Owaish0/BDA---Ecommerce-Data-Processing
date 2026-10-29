# Integrated dashboard controls

Controls live directly in the main dashboard. There is no separate demo panel.

1. Set a target rate and click **Apply speed**. Use **Start events** and **Pause events**
   to control continuous traffic; Kafka and Spark remain running.
2. Click **Flash sale** for 10,000 purchase records, biased toward Headphones. Observe
   purchase and revenue windows after Spark catches up.
3. Under **Data quality**, send duplicate pairs, invalid records, or late records.
   Read **Action activity** for delivery acknowledgement and Spark progress for
   processing evidence. Delivery success does not mean acceptance by Spark.
4. Click **Run historical analysis** next to its results. The button stays disabled
   while queued/running. The new report appears automatically when published.

Finite scenarios also run while continuous traffic is paused. A pause does not
cancel a finite scenario that is already running. Late timestamps are three minutes
old; compare them with Spark's actual watermark. Do not promise every late record
will be dropped. Current file-sink progress may not report quarantine output counts;
the dashboard displays this as unavailable, never as a negative event count.

## Local verification — 14 October 2026 (India time)

- 18 tests passed, including transactional PostgreSQL tests in a separate test database.
- AppTest exercised the real dashboard against the live database and workers.
- Pause stopped the send counter; Start resumed it; target speed changed to 25/s.
- Flash sale acknowledged 1,000 records; duplicate, invalid and late scenarios each
  acknowledged 100 records. The duplicate workload contains 50 unique IDs twice.
- Historical analysis completed and published its report to PostgreSQL.
- Original generation settings were restored after testing.

Run `python scripts/check_controls.py` inside the dashboard container to repeat the
integration check. It adds synthetic traffic and starts a real historical batch job.
The original CLI producer remains available for deterministic fixtures and benchmarks.

Controls use a persistent PostgreSQL queue, one active scenario and one active batch
at a time. Dedicated workers execute only predefined operations. The dashboard does
not mount Docker's socket. Interrupted running actions are marked failed on worker
restart so they are not silently replayed. Kafka send counts are operational counters,
not an exactly-once audit across a database outage immediately after Kafka delivery.
