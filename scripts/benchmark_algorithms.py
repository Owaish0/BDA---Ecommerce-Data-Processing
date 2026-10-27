"""Bounded, explicitly scoped algorithm experiment on imported historical records."""

import argparse
from collections import Counter
import json
from pathlib import Path
import time
import pyarrow.dataset as ds
from ecommerce.algorithms import BloomFilter, CountMinSketch, Reservoir


def run(root, limit=100_000):
    files = sorted((root / "months").glob("*/events/**/*.parquet"))
    if not files:
        raise ValueError("No historical Parquet files")
    sketch = CountMinSketch()
    sample = Reservoir(1000)
    exact = Counter()
    bloom = BloomFilter(5000)
    seen = probes = false_positives = 0
    started = time.monotonic()
    for batch in (
        ds.dataset([str(p) for p in files], format="parquet")
        .scanner(columns=["event_id", "product_id"], batch_size=8192)
        .to_batches()
    ):
        for record in batch.to_pylist():
            if seen >= limit:
                break
            product = record["product_id"]
            event = record["event_id"]
            exact[product] += 1
            sketch.add(product)
            sample.add(record)
            if seen < 5000:
                bloom.add(event)
            else:
                probes += 1
                false_positives += int(bloom.contains(event))
            seen += 1
        if seen >= limit:
            break
    result = {
        "scope": f"First {seen:,} records in sorted Parquet-file scan order; not a random sample of all seven months.",
        "events": seen,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "count_min_sketch": {
            "epsilon": 0.01,
            "failure_probability": 0.01,
            "width": sketch.width,
            "depth": sketch.depth,
            "candidate_method": "Exact counts select top 20 candidates for validation only; not a production bounded-memory top-k tracker.",
            "comparison": [
                {
                    "product": p,
                    "exact": count,
                    "estimate": sketch.estimate(p),
                    "overcount": sketch.estimate(p) - count,
                }
                for p, count in exact.most_common(20)
            ],
        },
        "reservoir": {
            "seen": sample.seen,
            "retained": len(sample.items),
            "size_limit": sample.size,
            "examples": sample.items[:10],
            "definition": "Uniform sample without replacement over this experiment stream, seed 404.",
        },
        "bloom": {
            "inserted": min(5000, seen),
            "absent_probes": probes,
            "false_positives": false_positives,
            "observed_false_positive_pct": 100 * false_positives / probes if probes else None,
            "bit_array_bytes": len(bloom.bits),
            "definition": "Probe disjoint stable source-row IDs. Never used to discard production events.",
        },
    }
    target = root / "algorithm-report.json"
    temp = target.with_suffix(".json.tmp")
    temp.write_text(json.dumps(result, indent=2), encoding="utf-8")
    temp.replace(target)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("data/history"))
    args = parser.parse_args()
    print(json.dumps(run(args.root), indent=2))
