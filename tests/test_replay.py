import json
import sqlite3
from datetime import datetime, timezone
import pytest
from ecommerce.replay import normalize, read_batch


def row(**changes):
    return dict(event_time='2019-10-01 00:00:00 UTC', event_type='purchase', product_id='123',
                category_id='456', category_code='', brand='', price='12.34', user_id='789', user_session='session', **changes)


def test_real_fields_and_no_invented_currency_or_quantity():
    event = normalize(row(), 1)
    assert event['price_paise'] == 1234
    assert event['quantity_observed'] is False
    assert event['currency'] == 'SOURCE_UNITS'
    assert event['product_id'] == 'rees46:123'
    assert event['category'] == 'category:456'
    assert event['event_id'] != normalize(row(), 2)['event_id']
    assert event['source_event_time'] == '2019-10-01T00:00:00+00:00'


@pytest.mark.parametrize('price', ['NaN', 'Infinity', '-1', '1.001', '1000001'])
def test_bad_price_is_rejected(price):
    source = row()
    source['price'] = price
    with pytest.raises(ValueError):
        normalize(source, 1)


def test_replay_cursor_and_original_time(tmp_path):
    path = tmp_path / 'events.sqlite'
    conn = sqlite3.connect(path)
    conn.execute('CREATE TABLE events(position INTEGER PRIMARY KEY,payload TEXT)')
    conn.executemany('INSERT INTO events VALUES(?,?)', [(i, json.dumps(normalize(row(), i))) for i in range(1, 4)])
    conn.commit()
    conn.close()
    cursor, events = read_batch(0, 2, path)
    assert cursor == 2 and len(events) == 2
    assert events[0]['source_event_time'].startswith('2019-')
    assert abs((datetime.now(timezone.utc)-datetime.fromisoformat(events[0]['event_time'])).total_seconds()) < 5
    cursor, events = read_batch(cursor, 2, path)
    assert cursor == 3 and len(events) == 1
    assert read_batch(cursor, 2, path) == (3, [])


def test_missing_session_is_marked_and_cart_removal_preserved():
    source = row()
    source.update(user_session='', event_type='remove_from_cart')
    result = normalize(source, 1)
    assert result['session_observed'] is False
    assert result['event_type'] == 'remove_from_cart'

def test_repeated_replay_has_new_ids_but_preserves_source(tmp_path):
    path = tmp_path / 'events.sqlite'
    with sqlite3.connect(path) as conn:
        conn.execute('CREATE TABLE events(position INTEGER PRIMARY KEY,payload TEXT)')
        conn.execute('INSERT INTO events VALUES(1,?)', (json.dumps(normalize(row(),1)),))
    _, original = read_batch(0,1,path)
    _, again = read_batch(0,1,path,run_id=1)
    _, retry = read_batch(0,1,path,run_id=1)
    assert original[0]['event_id'] != again[0]['event_id'] == retry[0]['event_id']
    assert original[0]['session_id'] != again[0]['session_id']
    assert original[0]['product_id'] == again[0]['product_id']
    assert original[0]['source_event_time'] == again[0]['source_event_time']
