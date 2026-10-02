import argparse
from datetime import datetime, timezone, timedelta
import logging
import os
from pathlib import Path
import signal
import time
from .events import EventGenerator, encode


def main():
    parser = argparse.ArgumentParser(description="Generate reproducible e-commerce workloads")
    parser.add_argument("--rate", type=float, default=100)
    parser.add_argument("--count", type=int, default=0, help="0 = continuous")
    parser.add_argument("--seed", type=int, default=404)
    parser.add_argument(
        "--scenario", choices=["normal", "burst", "late", "duplicates", "invalid"], default="normal"
    )
    parser.add_argument("--file", help="Write JSONL instead of Kafka for deterministic fixtures")
    parser.add_argument("--start-time", help="Fixed ISO start timestamp for repeatable tests")
    args = parser.parse_args()
    if args.rate <= 0 or args.count < 0:
        parser.error("rate must be positive; count cannot be negative")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    generator = EventGenerator(args.seed, run_id=f"fixture-{args.seed}" if args.file else None)
    stopped = False
    failures = []

    def stop(*_):
        nonlocal stopped
        stopped = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    producer = None
    stream = None
    if args.file:
        Path(args.file).parent.mkdir(parents=True, exist_ok=True)
        stream = open(args.file, "wb")
    else:
        from confluent_kafka import Producer

        producer = Producer(
            {
                "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
                "enable.idempotence": True,
                "acks": "all",
                "delivery.timeout.ms": 120000,
            }
        )

    def delivered(error, _message):
        if error:
            failures.append(str(error))
            logging.error("Kafka delivery failure: %s", error)

    start = datetime.fromisoformat(args.start_time) if args.start_time else None
    previous = None
    count = 0
    deadline = time.monotonic()
    try:
        while not stopped and (not args.count or count < args.count):
            if failures:
                raise RuntimeError(failures[-1])
            at = start + timedelta(seconds=count / args.rate) if start else datetime.now(timezone.utc)
            event = generator.next(at, trending=args.scenario == "burst")
            if args.scenario == "late" and count % 20 == 19:
                event["event_time"] = (at - timedelta(minutes=3)).isoformat()
            if args.scenario == "duplicates" and count % 10 == 9 and previous:
                event = previous.copy()
            payload = encode(event)
            if args.scenario == "invalid" and count % 20 == 19:
                payload = b'{"schema_version":1,"event_type":"purchase"}'
            if stream:
                stream.write(payload + b"\n")
            else:
                while True:
                    try:
                        producer.produce(
                            os.getenv("KAFKA_TOPIC", "ecommerce.events.v1"),
                            key=event["user_id"],
                            value=payload,
                            on_delivery=delivered,
                        )
                        break
                    except BufferError:
                        producer.poll(0.1)
                producer.poll(0)
            previous = event
            count += 1
            if count % 1000 == 0:
                logging.info("Produced %s events", count)
            if not stream:
                deadline += 1 / args.rate
                time.sleep(max(0, deadline - time.monotonic()))
    finally:
        if stream:
            stream.close()
        if producer and producer.flush(30) > 0:
            failures.append("Undelivered records after flush")
    if failures:
        raise RuntimeError("; ".join(failures))
    logging.info("Finished: %s events", count)


if __name__ == "__main__":
    main()
