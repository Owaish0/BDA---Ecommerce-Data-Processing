"""Read-only UI navigation smoke test; does not send events or launch jobs."""

import sys

sys.path.insert(0, "/app/dashboard")
from streamlit.testing.v1 import AppTest

if "--fixture" in sys.argv:
    import customer_pages
    from pathlib import Path

    customer_pages.ROOT = Path("/app/data/history-fixture")
app = AppTest.from_file("/app/dashboard/app.py", default_timeout=30).run()
assert not app.exception, app.exception
pages = list(app.radio(key="page").options)
for page in pages:
    app.radio(key="page").set_value(page).run()
    assert not app.exception, (page, app.exception)
    assert not app.error, (page, [e.value for e in app.error])
    assert app.title, page
    print("PASS", page, flush=True)

if "--fixture" in sys.argv:
    for page in ["Customer behaviour", "Shopping journeys", "Product performance", "Analysis methods"]:
        app.radio(key="page").set_value(page).run()
        assert app.dataframe, (page, "No historical table rendered")
    print("PASS historical pages render populated Spark fixture results")
