"""Stream a prefix of the official Kaggle ZIP; retain raw CSV and a replay index."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import sqlite3
import struct
import urllib.request
import zlib
from datetime import datetime, timezone
from ecommerce.replay import DATASET_URL, FILE_NAME, normalize

URL = 'https://www.kaggle.com/api/v1/datasets/download/mkechinov/ecommerce-behavior-data-from-multi-category-store/2019-Oct.csv'


def lines(response):
    header = response.read(30)
    if len(header) != 30 or header[:4] != b'PK\x03\x04':
        raise ValueError('Expected the official ZIP download, not a login/error page')
    _, _, flags, method, _, _, _, _, _, name_size, extra_size = struct.unpack('<IHHHHHIIIHH', header)
    name = response.read(name_size).decode('utf-8')
    response.read(extra_size)
    if flags & 1 or method != 8 or name != FILE_NAME:
        raise ValueError('Unexpected archive layout; use the documented CSV importer instead')
    decoder = zlib.decompressobj(-15)
    pending = b''
    downloaded = 0
    while True:
        chunk = response.read(65536)
        if not chunk:
            break
        downloaded += len(chunk)
        if downloaded > 256 * 1024 * 1024:
            raise ValueError('Bounded download exceeded 256 MiB; use a smaller sample')
        pending += decoder.decompress(chunk)
        parts = pending.split(b'\n')
        pending = parts.pop()
        for line in parts:
            yield line.decode('utf-8') + '\n'
        if decoder.eof:
            if pending:
                yield pending.decode('utf-8')
            break


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--rows', type=int, default=1_000_000)
    parser.add_argument('--output', default='data')
    parser.add_argument('--csv', help='Optional official CSV already downloaded locally')
    args = parser.parse_args()
    if not 1 <= args.rows <= 1_000_000:
        parser.error('rows must be 1..1000000')
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    final = out / 'rees46.sqlite'
    if final.exists():
        raise FileExistsError('Dataset already prepared; choose a new output directory to preserve it')
    response = open(args.csv, encoding='utf-8', newline='') if args.csv else urllib.request.urlopen(URL, timeout=60)
    with response, sqlite3.connect(out / 'rees46.sqlite.partial') as conn, open(out / 'rees46-sample.csv.partial', 'w', encoding='utf-8', newline='') as raw:
        conn.execute('CREATE TABLE events(position INTEGER PRIMARY KEY,payload TEXT NOT NULL)')
        conn.execute('CREATE TABLE metadata(value TEXT NOT NULL)')
        reader = csv.DictReader(response if args.csv else lines(response))
        writer = csv.DictWriter(raw, fieldnames=reader.fieldnames)
        writer.writeheader()
        accepted = rejected = count = 0
        types = {}
        first = last = None
        for count, row in enumerate(reader, 1):
            writer.writerow(row)
            try:
                event = normalize(row, count)
                accepted += 1
                conn.execute('INSERT INTO events VALUES(?,?)', (accepted, json.dumps(event, separators=(',', ':'))))
                types[event['event_type']] = types.get(event['event_type'], 0) + 1
                first = first or event['source_event_time']
                last = event['source_event_time']
            except (ValueError, KeyError):
                rejected += 1
            if count % 100000 == 0:
                conn.commit()
                print(f'Prepared {count:,} source records', flush=True)
            if count >= args.rows:
                break
        if not accepted:
            raise ValueError('No usable real events found')
        manifest = {'source_url': DATASET_URL, 'publisher': 'REES46 / Open CDP; Kaggle: mkechinov',
                    'source_file': FILE_NAME, 'sampling': 'Chronological file prefix; not a random or representative sample',
                    'source_rows': count, 'accepted_rows': accepted, 'rejected_rows': rejected,
                    'event_types': types, 'first_event_time': first, 'last_event_time': last,
                    'prepared_at': datetime.now(timezone.utc).isoformat(),
                    'currency': 'Unspecified in retrieved publisher metadata; displayed as source units',
                    'quantity': 'Not provided; sum recorded purchase prices, not order revenue',
                    'archive_integrity': 'Partial stream; full archive CRC not verified. Local raw sample SHA256 recorded.'}
        raw.flush()
        with open(out / 'rees46-sample.csv.partial', 'rb') as check:
            digest = hashlib.sha256()
            for chunk in iter(lambda: check.read(1024 * 1024), b''):
                digest.update(chunk)
        manifest['sample_sha256'] = digest.hexdigest()
        conn.execute('INSERT INTO metadata VALUES(?)', (json.dumps(manifest),))
    conn.close()
    (out / 'rees46.sqlite.partial').rename(final)
    (out / 'rees46-sample.csv.partial').rename(out / 'rees46-sample.csv')
    (out / 'rees46-manifest.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    print(json.dumps(manifest, indent=2))


if __name__ == '__main__':
    main()
