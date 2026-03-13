"""Streamlit dashboard skeleton.

Implement this file only after pipeline and analytics modules are ready.

Planned page build sequence:
1) KPI overview page.
2) Cost and token analysis page.
3) Tool usage and reliability page.
4) User/session behavior page.
5) Error analysis page.

Expected output:
- Interactive dashboard with stakeholder-friendly insights.
"""

from pathlib import Path

import pandas as pd
import streamlit as st

from src.analytics import event_mix, level_cost_stats, overview_kpis, peak_usage_by_hour
from src.config import load_settings
from src.plots import build_cost_by_level_chart, build_event_mix_chart, build_hourly_usage_chart
from src.storage import query_df


st.set_page_config(page_title="Claude Telemetry Analytics", layout="wide")
st.title("Claude Code Telemetry Analytics")

settings = load_settings(".")

st.caption(f"Database: {settings.sqlite_path}")


@st.cache_data(show_spinner=False)
def load_tables(db_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    events = query_df(db_path, "SELECT * FROM fact_events")
    employees = query_df(db_path, "SELECT * FROM dim_employees")
    return events, employees


if not Path(settings.sqlite_path).exists():
    st.warning("Database file not found. Run pipeline first: python scripts/run_pipeline.py")
    st.stop()

try:
    events_df, employees_df = load_tables(str(settings.sqlite_path))
except Exception as exc:
    st.error(f"Failed to load data from SQLite: {exc}")
    st.stop()

if events_df.empty:
    st.warning("fact_events is empty. Run the pipeline to load data.")
    st.stop()

kpis = overview_kpis(events_df)

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Total events", f"{int(kpis['total_events']):,}")
col2.metric("Active users", f"{int(kpis['active_users']):,}")
col3.metric("Sessions", f"{int(kpis['sessions']):,}")
col4.metric("API requests", f"{int(kpis['api_request_count']):,}")
col5.metric("Total API cost", f"${kpis['total_api_cost_usd']:.2f}")

cost_by_level_df = level_cost_stats(events_df, employees_df)
event_mix_df = event_mix(events_df)
hourly_df = peak_usage_by_hour(events_df)

st.subheader("Cost by Seniority Level")
st.plotly_chart(build_cost_by_level_chart(cost_by_level_df), width="stretch")
st.dataframe(cost_by_level_df, width="stretch")

st.subheader("Event Mix")
st.plotly_chart(build_event_mix_chart(event_mix_df), width="stretch")
st.dataframe(event_mix_df.head(20), width="stretch")

st.subheader("Peak Usage by Hour")
st.plotly_chart(build_hourly_usage_chart(hourly_df), width="stretch")
st.dataframe(hourly_df, width="stretch")
