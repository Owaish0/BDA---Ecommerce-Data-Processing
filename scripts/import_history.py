"""Bounded-memory monthly ingestion. Only validated complete months are published.

Run: python scripts/import_history.py --month 2019-Oct
Interrupted conversions restart from the gzip (existing complete months are skipped).
Downloads resume only when a strong ETag and an exact Content-Range match.
"""

import argparse
import csv
import gzip
import io
import zipfile
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import shutil
import time
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

MONTHS = ["2019-Oct", "2019-Nov", "2019-Dec", "2020-Jan", "2020-Feb", "2020-Mar", "2020-Apr"]
BASE = "https://data.rees46.com/datasets/marketplace/"
FIELDS = [
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
]


def atomic_json(path, value):
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, indent=2), encoding="utf-8")
    temp.replace(path)


def space(path, reserve, extra=0):
    free = shutil.disk_usage(path).free
    if free < reserve + extra:
        raise RuntimeError(f"Stopped safely: {free/2**30:.1f} GiB free; reserve is {reserve/2**30:.1f} GiB")
    return free


def download(month, root, reserve, provider="rees46"):
    target = root / (month + (".csv.zip" if provider == "kaggle" else ".csv.gz"))
    url = (
        (
            "https://www.kaggle.com/api/v1/datasets/download/mkechinov/ecommerce-behavior-data-from-multi-category-store/"
            + month
            + ".csv"
        )
        if provider == "kaggle"
        else BASE + target.name
    )
    part = target.with_name(target.name + ".partial")
    meta = target.with_name(target.name + ".download.json")
    if target.exists():
        return target
    prior = json.loads(meta.read_text()) if meta.exists() else {}
    offset = part.stat().st_size if part.exists() else 0
    headers = {"User-Agent": "CS404-Research/1.0", "Accept-Encoding": "identity"}
    etag = prior.get("etag")
    resume = offset and etag and not etag.startswith("W/")
    if resume:
        headers.update({"Range": f"bytes={offset}-", "If-Range": etag})
    with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=90) as response:
        continued = resume and response.status == 206
        if continued and (
            not response.headers.get("Content-Range", "").startswith(f"bytes {offset}-")
            or response.headers.get("ETag") != etag
        ):
            raise RuntimeError("Server returned inconsistent resume metadata; partial file preserved")
        if response.status not in (200, 206) or (response.status == 206 and not continued):
            raise RuntimeError("Unexpected download response")
        length = response.headers.get("Content-Length")
        remaining = int(length) if length else None
        space(root, reserve, remaining or 0)
        atomic_json(meta, {"url": url, "etag": response.headers.get("ETag")})
        received = 0
        last = time.monotonic()
        with part.open("ab" if continued else "wb") as output:
            while True:
                block = response.read(1024 * 1024)
                if not block:
                    break
                space(root, reserve, len(block))
                output.write(block)
                received += len(block)
                if time.monotonic() - last > 20:
                    print(f"Downloaded {output.tell()/2**20:.0f} MiB of {month}", flush=True)
                    last = time.monotonic()
        if remaining is not None and received != remaining:
            raise RuntimeError("Incomplete download; rerun to resume")
    part.replace(target)
    return target


def normalized(row, month, number):
    at = datetime.strptime(row["event_time"], "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc)
    if at.strftime("%Y-%b") != month:
        raise ValueError("timestamp outside requested month")
    if row["event_type"] not in {"view", "cart", "purchase", "remove_from_cart"}:
        raise ValueError("unsupported event type")
    for key in ("product_id", "user_id"):
        if not row.get(key):
            raise ValueError("missing " + key)
    price = Decimal(row["price"]) * 100
    if not price.is_finite() or price != price.to_integral_value() or price < 0 or price > 100_000_000:
        raise ValueError("invalid price")
    return {
        "event_id": f"rees46-{month}-row-{number}",
        "source_row": number,
        "event_time": at,
        "event_date": at.date().isoformat(),
        "event_type": row["event_type"],
        "product_id": row["product_id"],
        "user_id": row["user_id"],
        "session_id": row.get("user_session") or None,
        "category": row.get("category_code") or "category:" + (row.get("category_id") or "unknown"),
        "brand": row.get("brand") or None,
        "price_minor": int(price),
    }


@contextmanager
def source_text(source, month):
    if source.suffix == ".zip":
        with zipfile.ZipFile(source) as archive:
            expected = month + ".csv"
            if archive.namelist() != [expected]:
                raise ValueError("Unexpected ZIP members")
            with archive.open(expected) as raw, io.TextIOWrapper(raw, encoding="utf-8", newline="") as text:
                yield text
    else:
        with gzip.open(source, "rt", encoding="utf-8", newline="") as text:
            yield text


def convert(source, month, root, reserve, batch_size=100_000, official_download=False):
    import pyarrow as pa
    import pyarrow.parquet as pq

    final = root / "months" / month
    if (final / "manifest.json").exists():
        print(f"{month} already complete; skipped", flush=True)
        return json.loads((final / "manifest.json").read_text())
    stage = root / "staging" / month
    stage.mkdir(parents=True, exist_ok=True)
    # A single-writer lock prevents overlapping runs. Staging is never used for analysis.
    lock = root / "import.lock"
    try:
        fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        raise RuntimeError("Import lock exists. Verify no importer is running before removing it.") from None
    os.close(fd)
    start = time.monotonic()
    schema = pa.schema(
        [
            ("event_id", pa.string()),
            ("source_row", pa.int64()),
            ("event_time", pa.timestamp("us", tz="UTC")),
            ("event_date", pa.string()),
            ("event_type", pa.string()),
            ("product_id", pa.string()),
            ("user_id", pa.string()),
            ("session_id", pa.string()),
            ("category", pa.string()),
            ("brand", pa.string()),
            ("price_minor", pa.int64()),
        ]
    )
    counts = {}
    accepted = rejected = number = 0
    chunk = []
    part = 0
    first = last = None
    initial_free = space(root, reserve)
    minimum_free = initial_free

    def flush():
        nonlocal part, minimum_free
        minimum_free = min(minimum_free, space(root, reserve, 64 * 2**20))
        table = pa.Table.from_pylist(chunk, schema=schema)
        pq.write_to_dataset(
            table,
            root_path=str(stage / "events"),
            partition_cols=["event_date"],
            basename_template=f"chunk-{part:06d}-{{i}}.parquet",
            compression="zstd",
        )
        part += 1
        chunk.clear()

    try:
        # Remove only our own incomplete staging tree before restarting conversion.
        events = stage / "events"
        if events.exists():
            expected = root.resolve() / "staging" / month / "events"
            if events.resolve() != expected or events.is_symlink():
                raise RuntimeError("Unexpected staging path; refusing cleanup")
            shutil.rmtree(events)
        with (
            source_text(source, month) as handle,
            (stage / "rejected.jsonl").open("w", encoding="utf-8") as rejects,
        ):
            reader = csv.DictReader(handle)
            if reader.fieldnames != FIELDS:
                raise ValueError("Unexpected CSV schema; no completed month published")
            for number, row in enumerate(reader, 1):
                try:
                    event = normalized(row, month, number)
                except (ValueError, KeyError, InvalidOperation, TypeError) as exc:
                    rejected += 1
                    rejects.write(
                        json.dumps({"source_row": number, "reason": str(exc), "record": row}) + "\n"
                    )
                else:
                    accepted += 1
                    chunk.append(event)
                    counts[event["event_type"]] = counts.get(event["event_type"], 0) + 1
                    first = min(first, event["event_time"]) if first else event["event_time"]
                    last = max(last, event["event_time"]) if last else event["event_time"]
                if number % batch_size == 0:
                    flush()
                    print(f"{month}: {number:,} rows read, {rejected:,} rejected", flush=True)
            if chunk:
                flush()
        if not accepted:
            raise ValueError("No valid rows found")
        # Reading gzip to EOF validates its CRC. Check stored Parquet row counts too.
        files = list((stage / "events").rglob("*.parquet"))
        stored = sum(pq.read_metadata(p).num_rows for p in files)
        if stored != accepted or accepted + rejected != number:
            raise RuntimeError("Row reconciliation failed")
        digest = hashlib.sha256()
        with source.open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
        report = {
            "month": month,
            "status": "complete",
            "source_url": (
                (
                    "https://www.kaggle.com/api/v1/datasets/download/mkechinov/ecommerce-behavior-data-from-multi-category-store/"
                    + month
                    + ".csv"
                )
                if source.suffix == ".zip"
                else BASE + month + ".csv.gz"
            )
            if official_download
            else None,
            "coverage": "Publisher monthly archive"
            if official_download
            else "User-supplied file; full-month coverage unverified",
            "source_sha256": digest.hexdigest(),
            "archive_crc_verified": True,
            "source_rows": number,
            "accepted_rows": accepted,
            "rejected_rows": rejected,
            "event_types": counts,
            "first_event_time": first.isoformat(),
            "last_event_time": last.isoformat(),
            "parquet_bytes": sum(p.stat().st_size for p in files),
            "elapsed_seconds": round(time.monotonic() - start, 2),
            "minimum_observed_free_bytes": minimum_free,
            "reserve_bytes": reserve,
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "scope": "Original timestamps; purchase prices in source units; no observed order quantities.",
        }
        atomic_json(stage / "manifest.json", report)
        final.parent.mkdir(parents=True, exist_ok=True)
        stage.replace(final)
        print(json.dumps(report, indent=2), flush=True)
        return report
    finally:
        lock.unlink()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", choices=MONTHS, required=True)
    parser.add_argument("--root", type=Path, default=Path("data/history"))
    parser.add_argument("--input-gzip", type=Path, help="Local gzip or ZIP archive; coverage is unverified")
    parser.add_argument("--provider", choices=["rees46", "kaggle"], default="kaggle")
    parser.add_argument("--reserve-gib", type=float, default=20)
    parser.add_argument("--batch-size", type=int, default=100_000)
    args = parser.parse_args()
    if args.reserve_gib < 1 or not 1 <= args.batch_size <= 100_000:
        parser.error("Reserve must be >=1 GiB and batch-size must be 1..100000")
    args.root.mkdir(parents=True, exist_ok=True)
    if (args.root / "months" / args.month / "manifest.json").exists():
        print("Month already completed; no changes made")
        return
    downloads = args.root / "downloads"
    downloads.mkdir(exist_ok=True)
    reserve = int(args.reserve_gib * 2**30)
    source = args.input_gzip or download(args.month, downloads, reserve, args.provider)
    convert(
        source, args.month, args.root, reserve, args.batch_size, official_download=args.input_gzip is None
    )


if __name__ == "__main__":
    main()
