import os
from datetime import datetime, timezone
import pandas as pd
import plotly.express as px
import psycopg
from psycopg.rows import dict_row
import streamlit as st
from ecommerce.events import CATALOG

PRODUCT_NAMES = {item[0]: item[1] for item in CATALOG}

st.set_page_config(page_title="CS404 Commerce Pulse", page_icon="📊", layout="wide")
st.title("Commerce Pulse")
st.caption("CS404 · Kafka + Spark · Synthetic e-commerce analytics · No machine learning")


def load(sql, params=()):
    with psycopg.connect(os.environ["DATABASE_URL"], connect_timeout=5, row_factory=dict_row) as conn:
        return conn.execute(sql, params).fetchall()


with st.sidebar:
    st.header("Demo controls")
    st.info(
        "Start and change the producer using the documented commands. This dashboard shows real database results."
    )
    minutes = st.selectbox("Sales horizon (minutes)", [5, 15, 30, 60], index=1)
    st.markdown("**Pipeline**\n\nGenerator → Kafka → Bronze → Silver → Spark windows → PostgreSQL")
    st.caption("Windows use UTC event time. All monetary values are INR. Data is synthetic.")


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
    st.subheader("Historical analysis and approximations")
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
