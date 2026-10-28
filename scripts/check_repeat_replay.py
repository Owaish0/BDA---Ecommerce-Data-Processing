import sys
import time

sys.path.insert(0, "/app/dashboard")
from streamlit.testing.v1 import AppTest
from ecommerce import control


def state():
    with control.connect() as c:
        return c.execute("SELECT * FROM generator_control WHERE id=1").fetchone()


def wait(check):
    end = time.monotonic() + 90
    while time.monotonic() < end:
        if check():
            return
        time.sleep(1)
    raise AssertionError("Control update timed out")


def click(label):
    next(b for b in app.button if b.label == label).click().run()
    assert not app.exception, app.exception


app = AppTest.from_file("/app/dashboard/app.py", default_timeout=45).run()
app.radio(key="overview_scope").set_value("Synthetic data").run()
assert not app.exception
assert any("Synthetic test events" in x.value for x in app.caption)
app.radio(key="page").set_value("Replay controls").run()
old = state()
assert old["source"] == "real" and not old["enabled"] and old["replay_position"] == 1000000
with control.connect() as c:
    c.execute("UPDATE generator_control SET rate=25 WHERE id=1")
try:
    click("Replay sample again")
    wait(lambda: state()["replay_position"] > 0)
    app.run()
    click("Pause events")
    wait(lambda: not state()["replay_pending"])
    time.sleep(2)
    paused = state()["replay_position"]
    time.sleep(2)
    assert state()["replay_position"] == paused
    assert state()["replay_run"] == old["replay_run"] + 1
    app.run()
    click("Start events")
    wait(lambda: state()["replay_position"] > paused)
    app.run()
    click("Pause events")
    wait(lambda: not state()["replay_pending"])
    with control.connect() as c:
        recent = c.execute(
            "SELECT payload FROM recent_events WHERE payload->>'event_id' LIKE 'replay-%' ORDER BY id DESC LIMIT 1"
        ).fetchone()
    assert recent and recent["payload"]["event_id"].startswith(f"replay-{old['replay_run']+1}:")
    print("PASS synthetic overview, fresh replay, unique run IDs, pause and resume", flush=True)
finally:
    with control.connect() as c:
        c.execute("UPDATE generator_control SET enabled=false,rate=%s WHERE id=1", (old["rate"],))
