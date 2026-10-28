import os
from decimal import Decimal
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import psycopg
from psycopg.rows import dict_row
import streamlit as st

from ecommerce import control
import customer_pages
import appearance
from ecommerce.events import CATALOG
from ecommerce.replay import metadata

PRODUCT_NAMES = {item[0]: item[1] for item in CATALOG}
PAGES = [
    "Overview",
    "Replay controls",
    "Products & categories",
    "Data quality",
    "Historical report",
    "How it works",
    "Customer behaviour",
    "Shopping journeys",
    "Product performance",
    "Analysis methods",
]
PERIODS = {"All processed events": None, "Last 5 minutes": 5, "Last 15 minutes": 15, "Last 30 minutes": 30}
STAGES = {
    "bronze-v1": "Save incoming records",
    "silver-v1": "Validate and remove duplicates",
    "quarantine-v1": "Separate invalid records",
    "sales-1m-v1": "Calculate minute totals",
    "trending-5m-v1": "Calculate recent product activity",
}

st.set_page_config(
    page_title="Commerce Pulse", page_icon="📊", layout="wide", initial_sidebar_state="expanded"
)
appearance.apply()


def load(sql, params=()):
    with psycopg.connect(os.environ["DATABASE_URL"], connect_timeout=5, row_factory=dict_row) as conn:
        return conn.execute(sql, params).fetchall()


def product_name(value):
    return PRODUCT_NAMES.get(value, "Product " + value.removeprefix("rees46:"))


def category_name(value):
    return value.replace(".", " › ").replace("_", " ").capitalize()


def money(value, real):
    return ("" if real else "₹") + f"{(value or 0)/100:,.2f}"


def prices_note(real):
    if real:
        st.caption(
            "Source units: the dataset does not identify a currency. We add the listed price on each purchase record; quantities and complete order totals are unavailable."
        )
    else:
        st.caption("Synthetic revenue = price × quantity for each purchase. Amounts are in INR.")


def submit(kind):
    try:
        action_id = control.enqueue(kind)
        st.toast(f"Request #{action_id} queued. Its status will update below.")
    except psycopg.errors.UniqueViolation:
        st.warning("A request is already queued or running. Wait for it to finish.")
    except ValueError as exc:
        st.warning(str(exc))


def active_actions():
    return load("SELECT * FROM control_actions WHERE status IN ('queued','running')")


def activity(kind=None):
    rows = load(
        "SELECT id,kind,status,detail FROM control_actions WHERE (%s::text IS NULL OR kind=%s) ORDER BY id DESC LIMIT 5",
        (kind, kind),
    )
    names = {
        "batch": "Historical analysis",
        "burst": "Flash sale",
        "duplicates": "Duplicate test",
        "invalid": "Invalid-record test",
        "late": "Late-record test",
    }
    if rows:
        st.dataframe(
            [
                {
                    "Request": r["id"],
                    "Action": names.get(r["kind"], r["kind"]),
                    "Status": r["status"].capitalize(),
                    "Result": r["detail"],
                }
                for r in rows
            ],
            hide_index=True,
            use_container_width=True,
        )


def metrics(real, period):
    minutes = PERIODS[period]
    return pd.DataFrame(
        load(
            """SELECT * FROM window_metrics WHERE query_name='sales-1m-v1'
        AND (product_id LIKE 'rees46:%%')=%s
        AND (%s::integer IS NULL OR window_start >= date_trunc('minute',now())-(%s * interval '1 minute'))
        ORDER BY window_start""",
            (real, minutes, minutes),
        )
    )


def period_picker(key):
    result = st.selectbox("Which period do you want to see?", list(PERIODS), key=key)
    st.caption(
        "Totals use non-overlapping, one-minute windows. Recent periods include the current partial minute; they are not lifetime totals."
        if PERIODS[result]
        else "Totals cover this source’s events processed by this running project, including earlier replay sessions. They do not automatically include the entire downloaded dataset."
    )
    return result


def overview(real, state):
    st.title("Store activity at a glance")
    st.write("See what customers viewed, added to their carts, and purchased.")
    scope = st.radio(
        "Which data do you want to see?",
        ["Full historical dataset", "Live replay", "Synthetic data"],
        key="overview_scope",
        horizontal=True,
    )
    historical_scope = scope == "Full historical dataset"
    real = scope != "Synthetic data"
    if historical_scope:
        data = customer_pages.report()
        if not data:
            st.caption("You can switch to Live replay to see events processed by the demonstration pipeline.")
            return
        st.info(
            "These totals cover all imported and analysed months. They do not include additional copies sent through live replay. November–April are not included until imported and analysed."
        )
        rows = data["monthly"]
        values = {
            key: sum(int(row[field]) for row in rows)
            for key, field in [
                ("events", "actions"),
                ("views", "views"),
                ("carts", "carts"),
                ("purchases", "purchases"),
            ]
        }
        values["revenue_paise"] = int(
            sum((Decimal(row["purchase_price_sum"] or "0") for row in rows), Decimal(0)) * 100
        )
        real = True
    else:
        st.caption(
            "LIVE REPLAY · "
            + ("Real demonstration sample" if real else "Synthetic test events")
            + " · Charts use replay timestamps"
        )
        manifest = metadata() if real else None
        if manifest:
            st.caption(
                f"Real replay run {state['replay_run'] + 1}. Live totals include previous demonstration runs, so they can exceed the sample size. Historical totals remain unchanged."
            )
            st.caption(
                f"Replay source: {manifest['accepted_rows']:,} records from the original demonstration sample. This is separate from the full historical dataset."
            )
            if state["replay_position"] >= manifest["accepted_rows"]:
                st.success(
                    f"{manifest['accepted_rows']:,}-record demonstration sample completed. Spark may still be finishing queued records. Choose Full historical dataset for the larger imported history."
                )
        period = period_picker("overview_period")
        df = metrics(real, period)
        if df.empty:
            st.info(
                "No processed events in this period yet. Choose a wider period, or open Replay controls and select the matching event source to start sending events."
            )
            return
        values = {
            key: int(df[key].sum()) for key in ["events", "views", "carts", "purchases", "revenue_paise"]
        }
    cols = st.columns(4)
    for col, label, key, explanation in zip(
        cols,
        ["Actions processed", "Product views", "Added to cart", "Purchase records"],
        ["events", "views", "carts", "purchases"],
        [
            "All accepted event records in this period. One customer can produce many actions.",
            "Number of view events, not distinct visitors or distinct products.",
            "Number of cart-addition events, not the current number of items inside carts.",
            "Number of purchase events. A real order can contain multiple purchase records.",
        ],
    ):
        col.metric(label, f"{values[key]:,}", help=explanation)
    st.info(
        f"How to read this: Spark processed {values['events']:,} actions in this view. "
        f"Of those, {values['views']:,} were product views and {values['purchases']:,} were purchase records. "
        'These are action counts, not counts of people.'
    )
    st.subheader("What types of actions occurred?")
    other = values["events"] - values["views"] - values["carts"] - values["purchases"]
    chart = pd.DataFrame(
        {
            "Action": ["Viewed a product", "Added to cart", "Purchased", "Other actions"],
            "Count": [values["views"], values["carts"], values["purchases"], other],
        }
    )
    st.plotly_chart(
        px.bar(chart, x="Action", y="Count", color="Action", text_auto=True), use_container_width=True
    )
    st.caption(
        "Each bar counts event records. Other actions can include searches or cart removals. This is not a customer conversion funnel."
    )
    if st.checkbox("Show purchase prices", key="overview_prices"):
        a, b = st.columns(2)
        a.metric(
            "Sum of listed purchase prices" if real else "Sales revenue", money(values["revenue_paise"], real)
        )
        b.metric(
            "Average listed purchase price" if real else "Average synthetic order value",
            money(values["revenue_paise"] / values["purchases"], real) if values["purchases"] else "—",
        )
        prices_note(real)
        st.caption(
            "Average = the displayed total ÷ purchase records. For real data this is not average order value."
        )
    if historical_scope:
        with st.expander("How are these numbers calculated?"):
            st.write(
                "Spark reads the validated historical records using their original shopping timestamps. This view adds the action totals from the completed monthly reports; it does not add replay totals or sum distinct-customer counts across months."
            )
        st.caption(
            "Next: Customer behaviour, Shopping journeys and Product performance explore this historical dataset."
        )
        return
    with st.expander("How are these numbers calculated?"):
        st.write(
            "Kafka receives event records. Spark validates them, handles duplicates, and groups accepted events by minute and product. This page adds those minute totals for your selected period."
        )
        st.write(
            "The downloaded file contains historical activity. Live charts use replay time—the time we send records through this project. Processing happens in batches, so numbers can update a few seconds after sending."
        )
        st.write(
            "Pausing stops new sends. Spark can still finish records already received. Recent-period totals can fall as older minutes leave the selected period."
        )
    st.caption(
        "Next: Replay controls lets you start or pause the stream. Products & categories shows which products attracted activity."
    )


def replay_controls(real, state):
    st.title("Send events into the pipeline")
    st.write("Start or pause the input, choose its speed, and see how far the replay has reached.")
    manifest = metadata()
    healthy = state["heartbeat"] and (datetime.now(timezone.utc) - state["heartbeat"]).total_seconds() < 15
    total = manifest["accepted_rows"] if manifest else 0
    exhausted = real and (not manifest or state["replay_position"] >= total)
    status = (
        f"{total:,}-record demonstration sample completed"
        if real and manifest and state["replay_position"] >= total
        else ("Worker unavailable" if not healthy else "Sending events" if state["enabled"] else "Paused")
    )
    st.subheader(status)
    if not healthy and not exhausted:
        st.warning(
            "The replay service is stopped or restarting. Your saved position is safe. Live controls become available when the service reconnects; completed historical reports remain available."
        )
    if real and manifest:
        st.caption(
            f"This replay uses the original {total:,}-record demonstration sample, not the full historical import. On Overview, choose Full historical dataset to see all imported and analysed records."
        )
        st.progress(
            min(1.0, state["replay_position"] / total),
            text=f"{state['replay_position']:,} of {total:,} demonstration-sample records sent",
        )
        st.caption(
            "This is delivery progress: Kafka has acknowledged these records. Spark may still be processing them. The replay stops at the end; it does not loop automatically."
        )
    elif not real:
        st.metric("Acknowledged sends across both modes", f"{state['delivered']:,}")
        st.caption(
            "This send counter includes synthetic tests and real replay. It is not the number of unique events accepted by Spark."
        )
    if real and manifest:
        st.caption(
            f"Demonstration run {state['replay_run'] + 1}. Live totals include previous runs; full historical totals never change when replaying."
        )
        if exhausted:
            st.info(
                "The sample has finished. Start a new replay below, or select Synthetic events for continuous generated activity."
            )
            if st.button(
                "Replay sample again",
                disabled=not healthy or state["enabled"] or bool(state["replay_pending"]),
            ):
                try:
                    control.restart_replay(total)
                    st.rerun()
                except ValueError as exc:
                    st.warning(str(exc))
    left, right = st.columns(2)
    if left.button(
        "Start events",
        type="primary",
        disabled=state["enabled"] or not healthy or exhausted,
        use_container_width=True,
    ):
        load("UPDATE generator_control SET enabled=true WHERE id=1 RETURNING id")
        st.rerun()
    if right.button("Pause events", disabled=not state["enabled"] or not healthy, use_container_width=True):
        load("UPDATE generator_control SET enabled=false WHERE id=1 RETURNING id")
        st.rerun()
    st.caption(
        "Start continues from the saved replay position. Pause stops new sends after in-flight delivery; it keeps the processing services running."
    )
    with st.form("speed"):
        rate = st.number_input("Target events / second", 1, 2000, int(state["rate"]), key="rate")
        st.caption(
            "For example, 100 means a target of 100 shopping actions sent each second. Actual speed depends on the computer."
        )
        if st.form_submit_button("Apply speed"):
            load("UPDATE generator_control SET rate=%s WHERE id=1 RETURNING id", (rate,))
            st.toast("Speed updated.")
    active = active_actions()
    with st.container():
        st.subheader("Choose the event source")
        choice = st.selectbox(
            "Event source", ["Real dataset replay", "Synthetic events"], index=0 if real else 1
        )
        requested = "real" if choice == "Real dataset replay" else "synthetic"
        st.write(
            "Real replay sends historical store records. Synthetic mode makes artificial events for controlled tests. Their charts are kept separate."
        )
        if st.button(
            "Use selected source", disabled=state["enabled"] or bool(active) or requested == state["source"]
        ):
            try:
                if requested == "real" and not manifest:
                    raise ValueError("The real dataset has not been prepared on this machine.")
                control.set_source(requested)
                st.rerun()
            except ValueError as exc:
                st.warning(str(exc))
        st.caption(
            "Pause and wait for any active request before switching. A source switch preserves replay progress."
        )
    if not real:
        st.subheader("Create a purchase surge")
        st.write(
            "Send 1,000 synthetic purchases, mostly for Headphones, then watch Overview or Products & categories."
        )
        if st.button("⚡ Flash sale · 1,000 purchases", disabled=bool(active) or not healthy):
            submit("burst")
        activity("burst")
    if state["error"]:
        st.error(state["error"])
    st.subheader("What does one event look like?")
    rows = load(
        "SELECT payload,delivered_at FROM recent_events WHERE coalesce(payload->>'data_source','synthetic')=%s ORDER BY id DESC LIMIT 10",
        (state["source"],),
    )
    if not rows:
        st.info("A sample will appear after records are sent.")
    else:
        names = {
            "view": "Viewed a product",
            "cart": "Added to cart",
            "purchase": "Purchased",
            "search": "Searched",
            "remove_from_cart": "Removed from cart",
        }
        st.dataframe(
            [
                {
                    "Action": names.get(r["payload"].get("event_type"), "Unknown"),
                    "Product": product_name(r["payload"].get("product_id", "Missing")),
                    "Sent at (UTC)": r["delivered_at"],
                    "Original date (UTC)": r["payload"].get("source_event_time", "Synthetic event"),
                }
                for r in rows
            ],
            hide_index=True,
            use_container_width=True,
        )
        st.caption(
            "These are the latest sampled records acknowledged by Kafka, before Spark validation. This table is not the full dataset."
        )


def products(real, state):
    st.title("Which products attract activity?")
    st.write("Compare products or categories using one metric at a time.")
    period = period_picker("product_period")
    a, b = st.columns(2)
    group = a.selectbox("Compare", ["Products", "Categories"])
    selection = b.selectbox(
        "Rank by",
        [
            "Product views",
            "Added to cart",
            "Purchase records",
            "Listed purchase value" if real else "Sales revenue",
        ],
    )
    column = {"Product views": "views", "Added to cart": "carts", "Purchase records": "purchases"}.get(
        selection, "revenue_paise"
    )
    df = metrics(real, period)
    if df.empty:
        st.info("No processed records in this period. Choose a wider period or start the replay.")
        return
    key = "product_id" if group == "Products" else "category"
    ranked = df.groupby(key, as_index=False)[column].sum().sort_values(column, ascending=False).head(10)
    ranked[group] = ranked[key].map(product_name if group == "Products" else category_name)
    ranked[selection] = ranked[column] / 100 if column == "revenue_paise" else ranked[column]
    st.subheader(f"Top 10 {group.lower()} by {selection.lower()}")
    st.plotly_chart(
        px.bar(ranked.sort_values(selection), x=selection, y=group, orientation="h"), use_container_width=True
    )
    st.dataframe(ranked[[group, selection]], hide_index=True, use_container_width=True)
    st.caption(
        "We group accepted events by product or category, add the selected metric across minute windows, and sort from largest to smallest."
    )
    if real:
        st.caption(
            "The dataset provides product IDs rather than readable product names. A missing category name is shown using its category ID."
        )
    if column == "revenue_paise":
        prices_note(real)
    with st.expander("Explore the 5-minute sliding window"):
        recent = load(
            """SELECT product_id,views,purchases,window_start,window_end FROM window_metrics
            WHERE query_name='trending-5m-v1' AND (product_id LIKE 'rees46:%%')=%s
            AND window_start=(SELECT max(window_start) FROM window_metrics
              WHERE query_name='trending-5m-v1' AND (product_id LIKE 'rees46:%%')=%s)
            ORDER BY views DESC LIMIT 10""",
            (real, real),
        )
        st.write(
            "Spark also calculates overlapping 5-minute windows, moving forward one minute at a time. Below is the latest available window for this source; it can be old if input is paused."
        )
        if recent:
            st.caption(
                f"Window: {recent[0]['window_start']} to {recent[0]['window_end']} (UTC). Windows remain provisional until the watermark passes."
            )
            st.dataframe(
                [
                    {
                        "Product": product_name(r["product_id"]),
                        "Views": r["views"],
                        "Purchase records": r["purchases"],
                    }
                    for r in recent
                ],
                hide_index=True,
                use_container_width=True,
            )
        else:
            st.info("No sliding-window results yet.")


def quality(real, state):
    st.title("Is the data being processed correctly?")
    st.write("Follow each processing step and test how the pipeline handles unusual records.")
    progress = load("SELECT * FROM query_progress ORDER BY query_name")
    table = []
    for row in progress:
        age = max(0, (datetime.now(timezone.utc) - row["updated_at"]).total_seconds())
        table.append(
            {
                "Step": STAGES.get(row["query_name"], row["query_name"]),
                "Update": "Recent progress" if age < 30 else "Idle or needs checking",
                "Seconds since update": round(age),
            }
        )
    if table:
        st.dataframe(table, hide_index=True, use_container_width=True)
    else:
        st.info("Spark has not reported any progress yet.")
    st.caption(
        "These updates cover the shared pipeline, across both sources. An old update can mean paused input or a problem; it does not by itself prove a service failure."
    )
    st.subheader("What happened in the latest processing batch?")
    silver = next((r["progress"] for r in progress if r["query_name"] == "silver-v1"), {})
    states = silver.get("stateOperators", [])
    a, b = st.columns(2)
    duplicate_values = [s.get("customMetrics", {}).get("numDroppedDuplicateRows") for s in states]
    duplicates = (
        sum(duplicate_values) if duplicate_values and all(v is not None for v in duplicate_values) else None
    )
    late = sum(s.get("numRowsDroppedByWatermark", 0) for s in states) if states else None
    a.metric("Repeated events removed", f"{duplicates:,}" if duplicates is not None else "Not reported")
    b.metric("Too-late events removed", f"{late:,}" if late is not None else "Not reported")
    st.caption(
        "These are counts from the latest Silver processing batch, not lifetime totals. They can return to zero in the next batch. Invalid records are saved separately in quarantine; the file sink may not report their count."
    )
    st.subheader("Test the handling rules")
    if real:
        st.info(
            "These tests create artificial records. To try them, open Replay controls, pause, and switch to Synthetic events."
        )
    active = active_actions()
    cols = st.columns(3)
    for col, label, kind, description in zip(
        cols,
        ["Send duplicates · 50 pairs", "Send invalid · 100 records", "Send late · 100 records"],
        ["duplicates", "invalid", "late"],
        [
            "Sends 50 events twice with the same IDs. Spark should remove timely duplicates.",
            "Sends records missing required fields. Spark saves them in quarantine.",
            "Sends records timestamped 3 minutes ago. Their handling depends on Spark’s current watermark.",
        ],
    ):
        col.write(description)
        if col.button(
            label, disabled=real or any(r["kind"] != "batch" for r in active), use_container_width=True
        ):
            submit(kind)
            st.rerun()
    st.subheader("Recent requests")
    activity()
    st.caption(
        "Completed event requests mean Kafka acknowledged delivery. Spark may still be processing the records. Tests also work while synthetic continuous input is paused."
    )
    with st.expander("Technical details for troubleshooting"):
        for row in progress:
            st.write(STAGES.get(row["query_name"], row["query_name"]))
            st.json(row["progress"])


def historical(real, state):
    st.title("Analyse everything stored so far")
    st.write(
        "Create a snapshot of the selected source’s saved events. This report updates only when you run the analysis."
    )
    active = active_actions()
    busy = any(r["kind"] == "batch" for r in active)
    if st.button("Run historical analysis", disabled=busy, type="primary"):
        submit("batch")
        st.rerun()
    if busy:
        st.info("Historical analysis is queued or running. You can visit another page; the job continues.")
    activity("batch")
    reports = load(
        "SELECT * FROM batch_reports WHERE coalesce(summary->>'data_source','synthetic')=%s ORDER BY created_at DESC LIMIT 1",
        (state["source"],),
    )
    if not reports:
        st.info(
            "No report for this source yet. Send some events, let Spark process them, then run historical analysis."
        )
        return
    report = reports[0]
    summary = report["summary"]
    totals = summary["totals"]
    st.caption(
        "Snapshot created: "
        + report["created_at"].strftime("%d %b %Y, %H:%M UTC")
        + ". It covers processed records stored in Silver, not the entire downloaded file automatically."
    )
    cols = st.columns(3)
    cols[0].metric(
        "Saved unique events",
        f"{totals['events']:,}",
        help="Number of saved records after deduplicating event IDs.",
    )
    cols[1].metric(
        "Distinct customers",
        f"{(totals['active_users_exact'] or 0):,}",
        help="Exact count of distinct user IDs in this snapshot.",
    )
    cols[2].metric("Purchase records", f"{(totals['purchases'] or 0):,}")
    st.subheader("Did cart sessions lead to a purchase?")
    funnel = summary["session_funnel"]
    denominator = funnel["cart_sessions"] or 0
    numerator = funnel["converted_sessions"] or 0
    st.metric("Cart sessions with a later purchase", f"{numerator/denominator:.1%}" if denominator else "—")
    st.write(
        f"{numerator:,} of {denominator:,} sessions with a cart event also had a purchase at or after their first cart event."
    )
    st.caption(
        "Calculation: qualifying sessions ÷ sessions with a cart event × 100. Real-data ordering uses original timestamps; missing source sessions are excluded. This is observed behaviour, not proof that adding to a cart caused a purchase."
    )
    if st.checkbox("Show snapshot purchase prices", key="historical_prices"):
        st.metric(
            "Sum of listed purchase prices" if real else "Sales revenue", money(totals["revenue_paise"], real)
        )
        prices_note(real)
    with st.expander("Compare exact counts with faster estimates"):
        exact = totals.get("active_users_exact") or 0
        approx = totals.get("active_users_approx") or 0
        st.write(f"Counting every distinct user gives {exact:,}. Spark’s approximate count gives {approx:,}.")
        st.metric("Observed estimation error", f"{abs(approx-exact)/exact:.1%}" if exact else "—")
        sample = summary["sampling"]
        st.write(
            f"The sample uses {sample['fraction']:.0%} of events selected by a stable hash of event ID. Its estimates are scaled up to the full snapshot."
        )
        st.dataframe(
            [
                {"Method": "Full snapshot", "Event count": totals["events"]},
                {"Method": "Estimate from sample", "Event count": sample["estimated_events"]},
            ],
            hide_index=True,
            use_container_width=True,
        )
        st.caption(
            "Approximate and sampled results can differ from exact results. These methods demonstrate Big Data analytics trade-offs; no machine-learning model is trained."
        )


def explain(real, state):
    st.title("How this project works")
    st.write("We turn a stream of shopping actions into live charts and historical reports.")
    st.graphviz_chart(
        'digraph { rankdir=LR; node [shape=box style="rounded"]; Input [label="1. Event source"]; Kafka [label="2. Kafka\\nReceives events"]; Spark [label="3. Spark\\nChecks and counts"]; HDFS [label="HDFS\\nStores records"]; DB [label="PostgreSQL\\nStores totals"]; UI [label="4. Dashboard\\nShows results"]; Input -> Kafka -> Spark; Spark -> HDFS; Spark -> DB -> UI; }',
        use_container_width=True,
    )
    st.subheader("Explain it in four steps")
    st.markdown(
        "1. **Send:** Python sends real historical records or synthetic shopping events into Kafka.\n2. **Process:** Spark checks fields, handles duplicates and late arrivals, and calculates counts in time windows.\n3. **Store:** HDFS keeps event records. PostgreSQL holds calculated results that are quick to display.\n4. **Display:** This dashboard reads those results. It refreshes about every five seconds; processing can take longer."
    )
    st.subheader("A simple example")
    st.write(
        "Illustration only: if one person views a product twice, adds it to their cart once and purchases once, that creates 4 actions: 2 views, 1 cart addition and 1 purchase record. It is still only 1 person."
    )
    st.subheader("What each page answers")
    st.dataframe(
        [
            {"Page": p, "Question": q}
            for p, q in zip(
                PAGES,
                [
                    "How many shopping actions have been processed?",
                    "What are we sending, and how quickly?",
                    "Which products or categories have the most activity?",
                    "How are records checked and unusual cases handled?",
                    "What does the saved replay history tell us?",
                    "How can I explain the architecture and calculations?",
                    "Who browsed, purchased and returned?",
                    "What happened during recorded visits?",
                    "Which products turn observed interest into purchases?",
                    "How accurate are the estimates?",
                ],
            )
        ],
        hide_index=True,
        use_container_width=True,
    )
    with st.expander("Dataset, timestamps and price definitions"):
        manifest = metadata()
        if manifest:
            st.write(
                f"Prepared dataset: {manifest['accepted_rows']:,} records from {manifest['source_file']}. This is a chronological prefix, not a random sample."
            )
            st.write(f"Original dates: {manifest['first_event_time']} to {manifest['last_event_time']}.")
        st.markdown(
            "[Dataset published by mkechinov on Kaggle](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) · Data collected by [REES46 / Open CDP](https://rees46.com)."
        )
        st.write(
            "Live windows use replay timestamps so the charts show activity now. Original timestamps remain stored. Changing replay speed changes live windows; these are not the original store’s historical traffic rates."
        )
        st.write(
            "Real records have listed prices but no quantities or complete order IDs. The retrieved publisher metadata does not specify currency. Real price totals are therefore labelled in source units, not INR or USD. Synthetic mode uses INR."
        )
    st.subheader("Suggested presentation order")
    st.write(
        "How it works → Replay controls → Overview → Products & categories → Data quality → Historical report."
    )


try:
    control.initialize()
except psycopg.Error:
    st.error("The analytics database is unavailable. Start the project and refresh this page.")
    st.stop()

with st.sidebar:
    appearance.brand()
    st.caption("Explore your store. Understand every action.")
    page = st.radio(
        "Workspace",
        PAGES,
        key="page",
        format_func=lambda name: f"{appearance.ICONS[PAGES.index(name)]}  {name}",
    )
    st.divider()
    st.caption("CS404 · Big Data Analytics\n\nKafka · Spark · HDFS · PostgreSQL")


@st.fragment(run_every="5s")
def render_page():
    try:
        state = load("SELECT * FROM generator_control WHERE id=1")[0]
        real = state["source"] == "real"
        if page in PAGES[1:6]:
            st.caption(
                ("REAL DATA REPLAY" if real else "SYNTHETIC TEST DATA")
                + " · Updated "
                + datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
            )
        handlers = dict(
            zip(
                PAGES,
                [
                    overview,
                    replay_controls,
                    products,
                    quality,
                    historical,
                    explain,
                    customer_pages.behaviour,
                    customer_pages.journeys,
                    customer_pages.products,
                    customer_pages.methods,
                ],
            )
        )
        appearance.page_banner(page, PAGES.index(page))
        handlers[page](real, state)
    except psycopg.Error:
        st.error(
            "The database is temporarily unavailable. Your saved data and replay position have not been reset."
        )


render_page()
