"""Reconcile real replay with its local source and exercise source isolation."""
import json
import time
from streamlit.testing.v1 import AppTest
from ecommerce import control
from ecommerce.replay import open_dataset


def state():
    with control.connect() as conn:
        return conn.execute('SELECT * FROM generator_control WHERE id=1').fetchone()


def wait_for(fn, timeout=240):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if fn():
            return
        time.sleep(2)
    raise AssertionError('Timed out waiting for replay verification')


original = state()
assert original['source'] == 'real'
app = AppTest.from_file('/app/dashboard/app.py', default_timeout=30).run()
assert not app.exception, app.exception
try:
    if state()['enabled']:
        next(b for b in app.button if b.label == 'Pause events').click().run()
        assert not app.exception, app.exception
    wait_for(lambda: not state()['replay_pending'])
    time.sleep(3)
    position = state()['replay_position']
    assert position > 0
    time.sleep(3)
    assert state()['replay_position'] == position
    expected_purchases = expected_value = 0
    sessions = {}
    with open_dataset() as conn:
        for (payload,) in conn.execute('SELECT payload FROM events WHERE position<=?', (position,)):
            event = json.loads(payload)
            if event['session_observed']:
                session = sessions.setdefault(event['session_id'], {'cart': [], 'purchase': []})
                if event['event_type'] in session:
                    session[event['event_type']].append(event['source_event_time'])
            if event['event_type'] == 'purchase':
                expected_purchases += 1
                expected_value += event['price_paise']
    def reconciled():
        with control.connect() as conn:
            totals = conn.execute("SELECT coalesce(sum(events),0) AS events,coalesce(sum(purchases),0) AS purchases,coalesce(sum(revenue_paise),0) AS value FROM window_metrics WHERE query_name='sales-1m-v1' AND product_id LIKE 'rees46:%'").fetchone()
        if totals['events'] > position:
            raise AssertionError('Metrics exceed source cursor')
        return totals == {'events': position, 'purchases': expected_purchases, 'value': expected_value}
    wait_for(reconciled)
    print(f'PASS exact source reconciliation: {position} events, {expected_purchases} purchases, {expected_value} minor source units', flush=True)
    try:
        control.enqueue('burst')
        raise AssertionError('Artificial workload allowed in real mode')
    except ValueError:
        pass
    control.set_source('synthetic')
    app.run()
    assert not app.exception
    control.set_source('real')
    app.run()
    assert not app.exception and state()['replay_position'] == position
    print('PASS pause, source switching, cursor preservation and scenario isolation', flush=True)
    next(b for b in app.button if b.label == 'Run historical analysis').click().run()
    assert not app.exception
    def historical_done():
        with control.connect() as conn:
            action = conn.execute("SELECT * FROM control_actions WHERE kind='batch' ORDER BY id DESC LIMIT 1").fetchone()
            if action and action['status'] == 'failed':
                raise AssertionError(action['detail'])
            if not action or action['status'] != 'completed':
                return False
            report = conn.execute("SELECT summary FROM batch_reports WHERE summary->>'data_source'='real' AND created_at>=%s ORDER BY created_at DESC LIMIT 1", (action['created_at'],)).fetchone()
            if report:
                totals = report['summary']['totals']
                assert (totals['events'], totals['purchases'], totals['revenue_paise']) == (position, expected_purchases, expected_value)
                funnel = report['summary']['session_funnel']
                assert funnel['cart_sessions'] == sum(bool(s['cart']) for s in sessions.values())
                assert (funnel['converted_sessions'] or 0) == sum(bool(s['cart']) and bool(s['purchase']) and max(s['purchase']) >= min(s['cart']) for s in sessions.values())
            return action and action['status'] == 'completed' and report
    wait_for(historical_done, 900)
    app.run()
    assert not app.exception
    print('PASS real historical report reconciles with source', flush=True)
finally:
    with control.connect() as conn:
        conn.execute('UPDATE generator_control SET source=%s,enabled=%s,rate=%s WHERE id=1',
                     (original['source'], original['enabled'], original['rate']))
