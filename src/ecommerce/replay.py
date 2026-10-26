"""Read a bounded, provenance-labelled REES46 sample without loading it into RAM."""
import json
import os
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

DATASET_URL = 'https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store'
FILE_NAME = '2019-Oct.csv'


def dataset_path():
    return Path(os.getenv('REPLAY_DB', '/app/data/rees46.sqlite'))


def open_dataset(path=None):
    path = Path(path or dataset_path()).resolve()
    return sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)


def metadata(path=None):
    if not Path(path or dataset_path()).is_file():
        return None
    with open_dataset(path) as conn:
        return json.loads(conn.execute('SELECT value FROM metadata').fetchone()[0])


def normalize(row, row_number):
    if row['event_type'] not in {'view', 'cart', 'purchase', 'remove_from_cart'}:
        raise ValueError('unsupported event type')
    original = datetime.strptime(row['event_time'], '%Y-%m-%d %H:%M:%S UTC').replace(tzinfo=timezone.utc)
    try:
        price = Decimal(row['price'])
        cents = price * 100
        if not cents.is_finite() or cents != cents.to_integral_value() or not 0 <= cents <= 100_000_000:
            raise ValueError('invalid price precision or range')
    except InvalidOperation as exc:
        raise ValueError('invalid price') from exc
    for field in ['user_id', 'product_id', 'category_id']:
        if not row.get(field, '').strip():
            raise ValueError('missing ' + field)
    session = row.get('user_session', '').strip()
    return {
        'schema_version': 1, 'event_id': f'rees46-oct2019-row-{row_number}',
        'event_time': original.isoformat(), 'source_event_time': original.isoformat(),
        'data_source': 'real', 'currency': 'SOURCE_UNITS', 'source_row': row_number,
        'user_id': 'rees46:' + row['user_id'],
        'session_id': 'rees46:' + (session or f'missing-row-{row_number}'),
        'session_observed': bool(session),
        'event_type': row['event_type'], 'product_id': 'rees46:' + row['product_id'],
        'product_name': row['product_id'],
        'category': row.get('category_code') or 'category:' + row['category_id'],
        'quantity': 1, 'quantity_observed': False, 'price_paise': int(cents),
    }


def read_batch(position, count, path=None):
    with open_dataset(path) as conn:
        rows = conn.execute('SELECT position,payload FROM events WHERE position>? ORDER BY position LIMIT ?',
                            (position, count)).fetchall()
    # Replay time intentionally reflects delivery, not the original market timeline.
    at = datetime.now(timezone.utc).isoformat()
    events = []
    for _, payload in rows:
        event = json.loads(payload)
        event['event_time'] = at
        events.append(event)
    return (rows[-1][0] if rows else position), events
