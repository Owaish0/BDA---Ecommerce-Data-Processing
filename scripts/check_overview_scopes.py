"""Read-only checks that historical and replay totals stay separate."""

import sys

sys.path.insert(0, "/app/dashboard")
import json
from pathlib import Path
from streamlit.testing.v1 import AppTest
from ecommerce import control
from ecommerce.replay import metadata

app = AppTest.from_file("/app/dashboard/app.py", default_timeout=45).run()
assert not app.exception, app.exception
assert app.radio(key="overview_scope").value == "Full historical dataset"
report = json.loads(Path("/app/data/history/customer-report.json").read_text())
expected = sum(row["actions"] for row in report["monthly"])
assert next(m.value for m in app.metric if m.label == "Actions processed") == f"{expected:,}"
app.radio(key="overview_scope").set_value("Live replay").run()
assert not app.exception, app.exception
with control.connect() as conn:
    actual = conn.execute(
        "SELECT sum(events) AS total FROM window_metrics WHERE query_name='sales-1m-v1' AND product_id LIKE 'rees46:%'"
    ).fetchone()["total"]
assert next(m.value for m in app.metric if m.label == "Actions processed") == f"{actual:,}"
with control.connect() as conn:
    state = conn.execute("SELECT * FROM generator_control WHERE id=1").fetchone()
completed = state["replay_position"] >= metadata()["accepted_rows"]
if completed:
    assert any("demonstration sample completed" in item.value for item in app.success)
app.radio(key="overview_scope").set_value("Full historical dataset").run()
assert next(m.value for m in app.metric if m.label == "Actions processed") == f"{expected:,}"
app.radio(key="page").set_value("Replay controls").run()
assert not app.exception, app.exception
if completed:
    assert any("demonstration sample completed" in item.value for item in app.subheader)
    assert next(b for b in app.button if b.label == "Start events").disabled
    assert any(b.label == "Replay sample again" for b in app.button)
print(
    f"PASS separate historical ({expected:,}) and replay ({actual:,}) totals, scope switching and completed-sample controls"
)
