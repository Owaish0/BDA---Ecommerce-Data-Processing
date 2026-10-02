# Professor demonstration

Target duration: 8–10 minutes. Run the stack before presenting so image downloads are
not part of the viva. Use real output and keep synthetic data clearly labelled.

1. **Architecture (1 minute).** Explain the producer, Kafka partitions, Spark tasks,
   HDFS bronze/silver layers, PostgreSQL metrics, and Streamlit UI. No classifier training.
2. **Ingestion (1 minute).** Show producer logs and Kafka topic configuration:
   `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic ecommerce.events.v1`.
3. **Live dashboard (2 minutes).** Explain purchase-only revenue, AOV, category sales,
   tumbling windows, and sliding-window trending. Explain why you cannot add per-product
   approximate unique-user counts to get a global user count.
4. **Traffic burst (1 minute).** Stop the background producer. Run a finite 500 events/s
   burst workload. Show resulting throughput and trending Headphones. Record actual
   latency measurements rather than promising instant updates.
5. **Duplicates and malformed data (1 minute).** Run finite `duplicates` and `invalid`
   workloads. Check silver deduplication and quarantine Parquet. Explain bounded deduplication.
6. **Late data and restart (1 minute).** Run the controlled lateness test when available,
   inspect watermark/late counters, restart streaming, and show persisted totals recover.
7. **Batch and approximations (1–2 minutes).** Stop the producer, allow all stages to
   drain, run the batch profile, show exact totals, sample estimates, approximate distinct
   users, and the session funnel. Use reconciliation evidence for the same snapshot.

Finite scenario command (replace scenario as needed):

```sh
docker compose stop producer
docker compose run --rm producer python -m ecommerce.producer --rate 100 --count 2000 --scenario duplicates
docker compose run --rm producer python -m ecommerce.producer --rate 100 --count 2000 --scenario invalid
docker compose run --rm producer python -m ecommerce.producer --rate 100 --count 2000 --scenario late
docker compose restart streaming
docker compose --profile batch run --rm batch
```

The `late` random workload emits some events three minutes old. It illustrates watermark
behaviour but is not a proof of the precise boundary rule. Use a staged deterministic
integration fixture to test that rule before claiming it passed.

Keep screenshots, logs, timings, and reconciliation reports with the final submission.
The earlier video is a design mockup and must not be represented as a recording of this code.
