"""Presentation only: shared visual styling, with no data or control operations."""

from pathlib import Path

import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import streamlit as st

ICONS = ["◈", "▷", "▥", "✓", "▤", "⌘", "◎", "↗", "◇", "∑"]


def apply():
    st.markdown(f"<style>{Path(__file__).with_name('theme.css').read_text()}</style>", unsafe_allow_html=True)
    pio.templates["commerce"] = go.layout.Template(
        layout=dict(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Inter, Segoe UI, sans-serif", color="#bcc9df", size=13),
            colorway=["#52e0c4", "#8a9cff", "#c9a0ff", "#ffca80"],
            margin=dict(l=20, r=20, t=30, b=25),
            xaxis=dict(gridcolor="#233048", zerolinecolor="#233048", title_standoff=18),
            yaxis=dict(gridcolor="#233048", zerolinecolor="#233048", title_standoff=18),
            hoverlabel=dict(bgcolor="#20304a", font_color="#f3f7ff", bordercolor="#52e0c4"),
            legend=dict(title_text="", orientation="h", y=1.12, x=0),
            bargap=0.35,
        )
    )
    px.defaults.template = "commerce"
    px.defaults.color_discrete_sequence = ["#52e0c4", "#8a9cff", "#c9a0ff", "#ffca80"]


def brand():
    st.markdown(
        '<div class="brand"><span class="brand-mark">◈</span><div>Commerce Pulse'
        "<small>COMMERCE INTELLIGENCE</small></div></div>",
        unsafe_allow_html=True,
    )


def page_banner(page, index):
    # These labels describe navigation only, never pipeline health or data freshness.
    group = "CUSTOMER INSIGHTS" if index >= 6 else "PLATFORM WORKSPACE"
    st.markdown(
        f'<div class="workspace-bar"><span>{group} <b>/</b> {page}</span>'
        '<span class="workspace-tag">CS404 · ANALYTICS</span></div>',
        unsafe_allow_html=True,
    )
