"""Historical views read an atomically published Spark report, never live replay totals."""

import json
from pathlib import Path
import pandas as pd
import streamlit as st

ROOT = Path("/app/data/history")


def report():
    path = ROOT / "customer-report.json"
    if not path.exists():
        progress = ROOT / "pipeline-status.json"
        if progress.exists():
            state = json.loads(progress.read_text())
            st.caption("Import progress: " + state["phase"])
            if state.get("error"):
                st.warning(state["error"])
        st.info(
            "Historical analysis is not ready yet. A full month must be imported and its Spark analysis completed first."
        )
        for p in sorted((ROOT / "months").glob("*/manifest.json")):
            data = json.loads(p.read_text())
            st.caption(f"Validated import: {data['month']} · {data['accepted_rows']:,} actions")
        return None
    data = json.loads(path.read_text())
    st.caption("HISTORICAL DATA · Original shopping timestamps · Months: " + ", ".join(data["months"]))
    st.caption(f"{data['accepted_rows']:,} accepted records · Report generated {data['generated_at']}")
    st.warning(
        "First observed means first seen in these imported months. Activity before and after this period is unknown."
    )
    return data


def behaviour(*_):
    st.title("Customer behaviour")
    st.write("Who browsed, purchased, and returned in a later month?")
    data = report()
    if not data:
        return
    frame = pd.DataFrame(data["behaviour"])
    st.dataframe(
        frame.rename(
            columns={
                "month": "Month",
                "active_customers": "Active customers",
                "returning_customers": "Observed returning customers",
                "viewers": "Viewed products",
                "cart_users": "Added to cart",
                "purchasers": "Purchased",
                "multiple_purchase_record_customers": "Multiple purchase records",
            }
        ),
        hide_index=True,
        use_container_width=True,
    )
    st.caption(
        "A customer can appear in several columns. Returning = seen in an earlier imported month. Multiple purchase records do not necessarily mean multiple orders."
    )
    st.subheader("Categories attracting interest")
    st.dataframe(
        pd.DataFrame(data["interests"]).rename(
            columns={"category": "Category", "views": "View actions", "viewers": "Distinct viewers"}
        ),
        hide_index=True,
        use_container_width=True,
    )


def journeys(*_):
    st.title("Shopping journeys")
    st.write("Follow actual recorded visits and understand how purchases relate to earlier actions.")
    data = report()
    if not data:
        return
    funnel = data["session_funnel"]
    denominator = funnel["cart_sessions"]
    numerator = funnel["converted_sessions"] or 0
    a, b = st.columns(2)
    a.metric(
        "Observed cart-session conversion",
        f"{100*numerator/denominator:.1f}%" if denominator else "Not available",
    )
    b.metric(
        "Median view-to-purchase time",
        str(data["timing"]["median_seconds_to_purchase"]) + " seconds"
        if data["timing"]["median_seconds_to_purchase"] is not None
        else "Not available",
    )
    st.caption(
        f"{numerator:,} sessions with a purchase at/after their first cart action ÷ {denominator:,} sessions containing a cart action. This does not require the same product."
    )
    st.caption(
        "Time to purchase matches the same customer, source session and product. It measures the first observed view to the first later purchase. Sessions without a matching purchase are excluded."
    )
    st.subheader("Explore real session examples")
    choices = data["journeys"]
    if choices:
        index = st.selectbox(
            "Choose a recorded visit",
            range(len(choices)),
            format_func=lambda i: f"Customer {choices[i]['user_id']} · Session {choices[i]['session_id']}",
        )
        item = choices[index]
        st.caption("Deterministic examples sorted by customer and session ID; not a representative sample.")
        st.write(f"This visit contains {item['total_actions']} recorded actions.")
        if item["timeline_truncated"]:
            st.warning("Only the first 200 actions are displayed.")
        timeline = pd.DataFrame(item["timeline"])
        timeline["listed_price"] = timeline.pop("price_minor") / 100
        st.dataframe(
            timeline.rename(
                columns={
                    "event_time": "Original time (UTC)",
                    "event_type": "Action",
                    "product_id": "Product",
                    "category": "Category",
                    "listed_price": "Listed price (source units)",
                }
            ),
            hide_index=True,
            use_container_width=True,
        )
    with st.expander("What does a session mean?"):
        st.write(
            f"There are {data['inferred_30min_sessions']:,} inferred visits when a gap of more than 30 minutes separates a customer’s activity. These differ from the dataset-provided session IDs used in the timelines and funnel."
        )
        st.write(
            "No observed purchase does not prove permanent abandonment. A purchase may occur outside the available period."
        )


def products(*_):
    st.title("Product performance")
    st.write("Compare the people who viewed a product with the people observed purchasing it.")
    data = report()
    if not data:
        return
    frame = pd.DataFrame(data["top_products"])
    st.caption(
        "Top 100 products by view actions across the imported period. This is a bounded ranking, not the full product catalogue."
    )
    sort = st.selectbox(
        "Rank these products by",
        ["views", "viewers", "purchasers", "observed_conversion_pct"],
        format_func=lambda x: {
            "views": "View actions",
            "viewers": "Distinct viewers",
            "purchasers": "Distinct purchasers",
            "observed_conversion_pct": "Matched conversion rate",
        }[x],
    )
    st.dataframe(
        frame.sort_values(sort, ascending=False).rename(
            columns={
                "product_id": "Product",
                "category": "Category",
                "views": "View actions",
                "purchases": "Purchase records",
                "viewers": "Distinct viewers",
                "purchasers": "Distinct purchasers",
                "matched_buyers": "Viewers later purchasing",
                "observed_conversion_pct": "Matched conversion (%)",
            }
        ),
        hide_index=True,
        use_container_width=True,
    )
    with st.expander("How is conversion calculated?"):
        st.write(data["definitions"]["conversion"])
        st.write(
            "A purchaser without a recorded preceding view does not enter the matched numerator. High interest with few purchases is a finding to investigate, not proof of a product problem."
        )


def methods(*_):
    st.title("Analysis methods")
    st.write("Understand what the algorithms calculate and how we check their results.")
    data = report()
    if not data:
        return
    frame = pd.DataFrame(data["monthly"])
    frame["observed_error_pct"] = (
        100 * (frame["estimated_customers"] - frame["customers"]).abs() / frame["customers"]
    )
    st.subheader("Exact versus estimated distinct customers")
    st.dataframe(
        frame[["month", "customers", "estimated_customers", "observed_error_pct"]].rename(
            columns={
                "month": "Month",
                "customers": "Exact customers",
                "estimated_customers": "Estimated customers",
                "observed_error_pct": "Observed error (%)",
            }
        ),
        hide_index=True,
        use_container_width=True,
    )
    st.caption(
        "Spark HyperLogLog++ uses a requested relative standard deviation of 5%. This is not a guarantee that every result differs by at most 5%."
    )
    st.write(
        "Tumbling/sliding windows run in the live pipeline. The historical job also computes visits using a 30-minute inactivity gap. The imported-period totals are cumulative from the first available month."
    )
    path = ROOT / "algorithm-report.json"
    if path.exists():
        experiment = json.loads(path.read_text())
        st.subheader("Bounded algorithm experiment")
        st.caption(experiment["scope"])
        st.write("Count-Min Sketch: estimated product frequency versus exact frequency")
        st.dataframe(experiment["count_min_sketch"]["comparison"], hide_index=True, use_container_width=True)
        st.caption(experiment["count_min_sketch"]["candidate_method"])
        st.write(
            f"Reservoir sampling kept {experiment['reservoir']['retained']:,} events from {experiment['events']:,} scanned records."
        )
        with st.expander("View sampled records"):
            st.json(experiment["reservoir"])
        st.write(
            f"Bloom filter: {experiment['bloom']['false_positives']:,} false positives in {experiment['bloom']['absent_probes']:,} known-absent probes."
        )
        st.caption(experiment["bloom"]["definition"])
    else:
        st.info(
            "Count-Min Sketch, Bloom filter and reservoir experiments have not yet been run on this historical import."
        )
    st.caption(
        "Completed-import validation marks available periods; it is not a separate streaming punctuated-window algorithm. Sketch-based sampling is not yet implemented."
    )
