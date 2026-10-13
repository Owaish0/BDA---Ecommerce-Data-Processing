import os
from datetime import datetime, timezone
import pandas as pd
import plotly.express as px
import psycopg
from psycopg.rows import dict_row
import streamlit as st
from ecommerce.events import CATALOG
from ecommerce import control

PRODUCT_NAMES = {item[0]: item[1] for item in CATALOG}

st.set_page_config(page_title="CS404 Commerce Pulse", page_icon="📊", layout="wide")
st.title("Commerce Pulse")
st.caption("CS404 · Kafka + Spark · Synthetic e-commerce analytics · No machine learning")


def load(sql, params=()):
    with psycopg.connect(os.environ["DATABASE_URL"], connect_timeout=5, row_factory=dict_row) as conn:
        return conn.execute(sql, params).fetchall()


try:
    control.initialize()
except psycopg.Error:
    st.error('Waiting for the analytics database.')
    st.stop()


def submit(kind):
    try:
        action_id = control.enqueue(kind)
        st.toast(f'Action #{action_id} queued. Results appear below.')
    except psycopg.errors.UniqueViolation:
        st.toast('An action is already queued or running. Wait for it to finish.')
    except psycopg.Error:
        st.error('Could not queue the action. Check the database connection.')


@st.fragment(run_every='3s')
def generation_controls():
    try:
        state = load('SELECT * FROM generator_control WHERE id=1')[0]
        active = load("SELECT * FROM control_actions WHERE status IN ('queued','running') AND kind<>'batch'")
        healthy = state['heartbeat'] and (datetime.now(timezone.utc)-state['heartbeat']).total_seconds() < 15
        cols = st.columns([1, 1, 2, 2])
        if cols[0].button('Start events', disabled=state['enabled'] or not healthy, use_container_width=True):
            load('UPDATE generator_control SET enabled=true WHERE id=1 RETURNING id')
            st.rerun()
        if cols[1].button('Pause events', disabled=not state['enabled'] or not healthy, use_container_width=True):
            load('UPDATE generator_control SET enabled=false WHERE id=1 RETURNING id')
            st.rerun()
        with cols[2].form('speed'):
            rate = st.number_input('Target events / second', 1, 500, int(state['rate']), key='rate')
            if st.form_submit_button('Apply speed'):
                load('UPDATE generator_control SET rate=%s WHERE id=1 RETURNING id', (rate,))
                st.toast('Speed updated.')
        cols[3].metric('Kafka-confirmed sends', f"{state['delivered']:,}")
        st.caption(('Generator unavailable' if not healthy else 'Generating' if state['enabled'] else 'Paused')
                   + ' · Counts cover this controlled generator, including duplicates and invalid records. Pause allows in-flight records to finish.')
        if state['error']:
            st.error(state['error'])
        if st.button('⚡ Flash sale · 1,000 purchases', disabled=bool(active) or not healthy):
            submit('burst')
    except psycopg.Error:
        st.warning('Generator controls temporarily unavailable.')


generation_controls()
minutes = st.selectbox('Sales horizon (minutes)', [5, 15, 30, 60], index=1)
st.caption('Generator → Kafka → Spark → HDFS lake + PostgreSQL → Dashboard · UTC event time · INR · Synthetic data')


@st.fragment(run_every="5s")
def live():
    try:
        rows = load(
            """SELECT * FROM window_metrics WHERE query_name='sales-1m-v1'
                       AND window_start >= now()-(%s * interval '1 minute') ORDER BY window_start""",
            (minutes,),
        )
        progress = load("SELECT * FROM query_progress ORDER BY query_name")
        historical = load("SELECT * FROM batch_reports ORDER BY created_at DESC LIMIT 1")
        trending = load("""SELECT product_id, sum(views) AS views, sum(purchases) AS purchases
          FROM window_metrics WHERE query_name='trending-5m-v1'
          AND window_start=(SELECT max(window_start) FROM window_metrics WHERE query_name='trending-5m-v1')
          GROUP BY product_id ORDER BY views DESC LIMIT 10""")
    except psycopg.Error:
        st.error("Analytics database is unavailable. Check PostgreSQL health and DATABASE_URL.")
        return
    st.caption("Refreshed at " + datetime.now(timezone.utc).strftime("%H:%M:%S UTC"))
    if not rows:
        st.info(
            "Waiting for sales metrics. Start the producer and streaming services; allow the first microbatches to complete."
        )
    else:
        df = pd.DataFrame(rows)
        events, purchases, revenue = [int(df[c].sum()) for c in ["events", "purchases", "revenue_paise"]]
        columns = st.columns(4)
        columns[0].metric("Events in horizon", f"{events:,}")
        columns[1].metric("Purchases", f"{purchases:,}")
        columns[2].metric("Revenue", f"₹{revenue/100:,.2f}")
        columns[3].metric("Average order value", f"₹{revenue/100/purchases:,.2f}" if purchases else "—")
        st.caption(
            "One purchase event represents one single-product order. Current windows are provisional until the watermark passes."
        )
        series = df.groupby("window_start", as_index=False)[["revenue_paise", "purchases"]].sum()
        series["Revenue (INR)"] = series["revenue_paise"] / 100
        left, right = st.columns([2, 1])
        left.plotly_chart(
            px.line(
                series, x="window_start", y="Revenue (INR)", markers=True,
                title="Revenue per 1-minute window", labels={"window_start": "Window start (UTC)"}
            ),
            use_container_width=True,
        )
        categories = df.groupby("category", as_index=False)["revenue_paise"].sum()
        categories["Revenue (INR)"] = categories["revenue_paise"] / 100
        right.plotly_chart(
            px.bar(categories, x="category", y="Revenue (INR)", title="Category sales",
                   labels={"category": "Category"}),
            use_container_width=True,
        )
        st.subheader("Recent product activity")
        if trending:
            trend_frame = pd.DataFrame(trending)
            trend_frame["Product"] = trend_frame["product_id"].map(PRODUCT_NAMES).fillna(trend_frame["product_id"])
            st.plotly_chart(
                px.bar(
                    trend_frame,
                    x="Product",
                    y="views",
                    title="Views in latest 5-minute sliding window",
                    labels={"views": "Views"},
                ),
                use_container_width=True,
            )
        st.caption(
            "Approximate distinct users are measured per product/window (5% relative standard deviation). They cannot be summed to get overall unique users."
        )
        st.dataframe(
            df[
                [
                    "window_start",
                    "category",
                    "product_id",
                    "events",
                    "views",
                    "carts",
                    "purchases",
                    "active_users_approx",
                ]
            ],
            use_container_width=True,
        )
    st.subheader("Pipeline health")
    stages = {row['query_name']: row for row in progress}
    status_cols = st.columns(4)
    for col, label, query in zip(status_cols, ['Kafka → Bronze', 'Validated → Silver', 'Sales windows', 'Trending windows'],
                                 ['bronze-v1', 'silver-v1', 'sales-1m-v1', 'trending-5m-v1']):
        row = stages.get(query)
        age = (datetime.now(timezone.utc)-row['updated_at']).total_seconds() if row else None
        col.metric(label, 'Recent' if age is not None and age < 30 else 'Idle / check')
        if age is not None:
            col.caption(f'Last progress {age:.0f}s ago')
    if not progress:
        st.warning("No streaming progress has been recorded yet.")
    for row in progress:
        p = row["progress"]
        age = (datetime.now(timezone.utc) - row["updated_at"]).total_seconds()
        with st.expander(row["query_name"] + f" · {'stale' if age > 30 else 'recent progress'}"):
            st.json(
                {
                    "batch": p.get("batchId"),
                    "input_rows": p.get("numInputRows"),
                    "input_rows_per_second": p.get("inputRowsPerSecond"),
                    "processed_rows_per_second": p.get("processedRowsPerSecond"),
                    "trigger_duration_ms": p.get("durationMs", {}).get("triggerExecution"),
                    "event_time": p.get("eventTime"),
                    "state": p.get("stateOperators"),
                    "progress_age_seconds": round(age, 1),
                }
            )
    st.caption(
        "Stale progress may mean an idle source or a failed query; inspect service logs. Trigger duration is processing time, not end-to-end latency."
    )
    st.subheader('Data quality')
    active = load("SELECT * FROM control_actions WHERE status IN ('queued','running')")
    busy = any(row['kind'] != 'batch' for row in active)
    buttons = st.columns(3)
    for col, label, kind in zip(buttons, ['Send duplicates · 50 pairs', 'Send invalid · 100 records', 'Send late · 100 records'],
                                ['duplicates', 'invalid', 'late']):
        if col.button(label, disabled=busy, use_container_width=True):
            submit(kind)
            st.rerun()
    st.caption('Late records are 3 minutes old. Whether Spark drops them depends on its current watermark. Invalid records omit required fields.')
    quality = []
    for row in progress:
        p = row['progress']
        for operator in p.get('stateOperators', []):
            quality.append({'Query': row['query_name'], 'Batch': p.get('batchId'),
                            'Dropped by watermark (batch)': operator.get('numRowsDroppedByWatermark', 0),
                            'Duplicates dropped (batch)': operator.get('customMetrics', {}).get('numDroppedDuplicateRows', 0)})
    if quality:
        st.dataframe(quality, use_container_width=True)
    quarantine = stages.get('quarantine-v1', {}).get('progress', {})
    output_rows = quarantine.get('sink', {}).get('numOutputRows', -1)
    st.caption(f"Quarantine rows written in latest batch: {output_rows if output_rows >= 0 else 'not reported by the file sink'}. These are batch metrics, not lifetime totals.")
    st.subheader('Recent generated events')
    recent = load('SELECT payload, delivered_at FROM recent_events ORDER BY id DESC LIMIT 20')
    st.caption('Sample of Kafka-confirmed sends, before Spark validation. Invalid events are deliberately visible here.')
    st.dataframe([{'Delivered (UTC)': r['delivered_at'],
                   'Event': r['payload'].get('event_type'),
                   'Product': PRODUCT_NAMES.get(r['payload'].get('product_id'), 'Missing'),
                   'Event time': r['payload'].get('event_time'),
                   'Quantity': r['payload'].get('quantity'),
                   'Unit price (INR)': r['payload'].get('price_paise', 0) / 100,
                   'Event ID': r['payload'].get('event_id', 'Missing')} for r in recent], use_container_width=True)
    st.subheader('Action activity')
    st.dataframe(load('SELECT id,kind,status,detail,created_at FROM control_actions ORDER BY id DESC LIMIT 10'), use_container_width=True)
    st.subheader("Historical analysis and approximations")
    if st.button('Run historical analysis', disabled=any(r['kind']=='batch' for r in active)):
        submit('batch')
        st.rerun()
    if not historical:
        st.info(
            "Run historical analysis to compare exact totals, approximate distinct users, sampling estimates, and the session funnel."
        )
    else:
        report = historical[0]
        summary = report["summary"]
        st.caption("Historical snapshot created at " + report["created_at"].strftime("%Y-%m-%d %H:%M UTC"))
        totals = summary["totals"]
        exact = totals.get("active_users_exact", 0) or 0
        approx = totals.get("active_users_approx", 0) or 0
        left, middle, right = st.columns(3)
        left.metric("Exact distinct users", f"{exact:,}")
        middle.metric("Approximate distinct users", f"{approx:,}")
        right.metric("Observed distinct-count error", f"{abs(approx-exact)/exact:.1%}" if exact else "—")
        sample = summary["sampling"]
        st.write(f"Sampling fraction: {sample['fraction']:.0%} · sampled events: {sample['sample_count']:,}")
        comparison = [
            {"Metric": "Events", "Exact": totals["events"], "Sample estimate": sample["estimated_events"]},
            {
                "Metric": "Revenue (INR)",
                "Exact": (totals["revenue_paise"] or 0) / 100,
                "Sample estimate": (sample["estimated_revenue_paise"] or 0) / 100,
            },
        ]
        st.dataframe(comparison, use_container_width=True)
        funnel = summary["session_funnel"]
        denominator = funnel["cart_sessions"] or 0
        numerator = funnel["converted_sessions"] or 0
        st.metric("Observed cart-session conversion", f"{numerator/denominator:.1%}" if denominator else "—")
        st.caption(
            "A converted session has a purchase after its first cart event. This is observational, not causal attribution. Historical and live horizons may differ."
        )


live()
