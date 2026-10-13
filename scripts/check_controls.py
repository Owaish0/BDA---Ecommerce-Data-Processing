"""Exercise real dashboard controls against the running stack (generates test traffic)."""
import time
from streamlit.testing.v1 import AppTest
from ecommerce import control


def wait_for(check, timeout=120):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if check():
            return
        time.sleep(1)
    raise AssertionError('Timed out waiting for control result')


def state():
    with control.connect() as conn:
        return conn.execute('SELECT * FROM generator_control WHERE id=1').fetchone()


app = AppTest.from_file('/app/dashboard/app.py', default_timeout=30).run()
assert not app.exception, app.exception


def click(label):
    app.run()
    next(b for b in app.button if b.label == label).click().run()
    assert not app.exception, app.exception


original = state()
try:
    if original['enabled']:
        click('Pause events')
    wait_for(lambda: not state()['enabled'])
    time.sleep(3)
    count = state()['delivered']
    time.sleep(3)
    assert state()['delivered'] == count, 'Pause did not stop normal generation'
    app.number_input(key='rate').set_value(25)
    next(b for b in app.button if b.label == 'Apply speed').click().run()
    assert state()['rate'] == 25
    click('Start events')
    wait_for(lambda: state()['delivered'] > count)
    click('Pause events')
    time.sleep(3)
    for label, kind, expected in [
        ('⚡ Flash sale · 1,000 purchases', 'burst', 1000),
        ('Send duplicates · 50 pairs', 'duplicates', 100),
        ('Send invalid · 100 records', 'invalid', 100),
        ('Send late · 100 records', 'late', 100),
    ]:
        before = state()['delivered']
        click(label)
        def done():
            with control.connect() as conn:
                row = conn.execute('SELECT * FROM control_actions WHERE kind=%s ORDER BY id DESC LIMIT 1', (kind,)).fetchone()
                if row and row['status'] == 'failed':
                    raise AssertionError(row['detail'])
                return row and row['status'] == 'completed'
        wait_for(done)
        assert state()['delivered'] - before == expected
        print('PASS', kind, expected, flush=True)
    click('Run historical analysis')
    def batch_done():
        with control.connect() as conn:
            row = conn.execute("SELECT * FROM control_actions WHERE kind='batch' ORDER BY id DESC LIMIT 1").fetchone()
            if row and row['status'] == 'failed':
                raise AssertionError(row['detail'])
            return row and row['status'] == 'completed'
    wait_for(batch_done, 900)
    app.run()
    assert not app.exception, app.exception
    print('PASS start/pause, speed, four scenarios, historical analysis, dashboard rendering', flush=True)
finally:
    with control.connect() as conn:
        conn.execute('UPDATE generator_control SET enabled=%s,rate=%s WHERE id=1', (original['enabled'], original['rate']))
