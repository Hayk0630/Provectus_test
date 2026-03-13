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
import plotly.express as px
import streamlit as st

from src.analytics import (
    error_breakdown,
    event_mix,
    level_cost_stats,
    model_usage_summary,
    overview_kpis,
    peak_usage_by_hour,
    retry_summary,
    token_trends_by_segment,
    tool_usage_summary,
)
from src.config import load_settings
from src.ml import detect_session_anomalies, forecast_daily_cost
from src.plots import build_cost_by_level_chart, build_event_mix_chart, build_hourly_usage_chart
from src.storage import query_df


st.set_page_config(page_title="Claude Telemetry Analytics", layout="wide")
st.title("Claude Code Telemetry Analytics")

settings = load_settings(".")

st.caption(f"Database: {settings.sqlite_path}")


@st.cache_data(show_spinner=False)
def load_tables(db_path: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    events = query_df(db_path, "SELECT * FROM fact_events")
    employees = query_df(db_path, "SELECT * FROM dim_employees")
    try:
        sessions = query_df(db_path, "SELECT * FROM fact_sessions")
    except Exception:
        sessions = pd.DataFrame()
    return events, employees, sessions


def apply_filters(events: pd.DataFrame, employees: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, list[str], list[str]]:
    filtered = events.copy()
    filtered["timestamp"] = pd.to_datetime(
        filtered["timestamp"],
        utc=True,
        errors="coerce",
        format="mixed",
    )

    emp_min = filtered["timestamp"].min()
    emp_max = filtered["timestamp"].max()
    if pd.isna(emp_min) or pd.isna(emp_max):
        return filtered, employees, [], []

    meta = employees[["email", "practice", "level", "location"]].copy()
    filtered = filtered.merge(meta, left_on="user_email", right_on="email", how="left")

    st.sidebar.header("Filters")
    show_full_dataset = st.sidebar.checkbox(
        "Show full dataset",
        value=False,
        help="When enabled, date/practice/level filters are ignored and all events are shown.",
    )

    date_range = st.sidebar.date_input(
        "Date range",
        value=(emp_min.date(), emp_max.date()),
        min_value=emp_min.date(),
        max_value=emp_max.date(),
        disabled=show_full_dataset,
    )

    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date = end_date = date_range

    if not show_full_dataset:
        ts_date = filtered["timestamp"].dt.date
        filtered = filtered[(ts_date >= start_date) & (ts_date <= end_date)]

    practices = sorted([p for p in filtered["practice"].dropna().unique().tolist()])
    levels = sorted([l for l in filtered["level"].dropna().unique().tolist()])
    models = sorted([m for m in filtered["model"].dropna().unique().tolist()])
    tools = sorted([t for t in filtered["tool_name"].dropna().unique().tolist()])

    selected_practice = st.sidebar.multiselect("Practice", practices, disabled=show_full_dataset)
    selected_level = st.sidebar.multiselect("Level", levels, disabled=show_full_dataset)
    selected_model = st.sidebar.multiselect("Model", models)
    selected_tool = st.sidebar.multiselect("Tool", tools)

    if not show_full_dataset:
        if selected_practice:
            filtered = filtered[filtered["practice"].isin(selected_practice)]
        if selected_level:
            filtered = filtered[filtered["level"].isin(selected_level)]

    # Model/tool selections are returned for section-level filtering only.
    # Keeping global event filtering to date/practice/level avoids hiding non-API/non-tool events.
    return filtered, employees, selected_model, selected_tool


if not Path(settings.sqlite_path).exists():
    st.warning("Database file not found. Run pipeline first: python scripts/run_pipeline.py")
    st.stop()

try:
    events_df, employees_df, sessions_df = load_tables(str(settings.sqlite_path))
except Exception as exc:
    st.error(f"Failed to load data from SQLite: {exc}")
    st.stop()

filtered_events, employees_df, selected_model, selected_tool = apply_filters(events_df, employees_df)

if filtered_events.empty:
    st.warning("No events for current filters. Adjust filters to see data.")
    st.stop()

kpis = overview_kpis(filtered_events)

st.caption(f"Rows in scope: {len(filtered_events):,} / {len(events_df):,} total events")
st.caption("Tip: turn on 'Show full dataset' in the sidebar to bypass date/practice/level filters.")

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Total events", f"{int(kpis['total_events']):,}")
col2.metric("Active users", f"{int(kpis['active_users']):,}")
col3.metric("Sessions", f"{int(kpis['sessions']):,}")
col4.metric("API requests", f"{int(kpis['api_request_count']):,}")
col5.metric("Total API cost", f"${kpis['total_api_cost_usd']:.2f}")

cost_by_level_df = level_cost_stats(filtered_events, employees_df)
event_mix_df = event_mix(filtered_events)
hourly_df = peak_usage_by_hour(filtered_events)
model_summary_df = model_usage_summary(filtered_events)
tool_summary_df = tool_usage_summary(filtered_events)
error_df = error_breakdown(filtered_events)
retry = retry_summary(filtered_events)
token_trends_df = token_trends_by_segment(filtered_events, employees_df, segment="practice", freq="D")

if selected_model:
    model_summary_df = model_summary_df[model_summary_df["model"].isin(selected_model)]
    error_df = error_df[error_df["model"].isin(selected_model)]

if selected_tool:
    tool_summary_df = tool_summary_df[tool_summary_df["tool_name"].isin(selected_tool)]

# ML controls
st.sidebar.header("ML")
contamination = st.sidebar.slider(
    "Anomaly contamination",
    min_value=0.01,
    max_value=0.20,
    value=0.05,
    step=0.01,
)
forecast_horizon = st.sidebar.slider(
    "Forecast horizon (days)",
    min_value=7,
    max_value=60,
    value=30,
    step=1,
)

st.subheader("Cost by Seniority Level")
st.plotly_chart(build_cost_by_level_chart(cost_by_level_df), width="stretch")
st.dataframe(cost_by_level_df, width="stretch")

st.subheader("Event Mix")
st.plotly_chart(build_event_mix_chart(event_mix_df), width="stretch")
st.dataframe(event_mix_df.head(20), width="stretch")

st.subheader("Peak Usage by Hour")
st.plotly_chart(build_hourly_usage_chart(hourly_df), width="stretch")
st.dataframe(hourly_df, width="stretch")

st.subheader("Token and Cost Trends by Practice")
if token_trends_df.empty:
    st.info("No token trend data for current filters.")
else:
    trend_fig = px.line(
        token_trends_df,
        x="period",
        y="total_input_tokens",
        color="practice",
        markers=True,
        title="Daily Input Tokens by Practice",
    )
    st.plotly_chart(trend_fig, width="stretch")
    st.dataframe(token_trends_df, width="stretch")

st.subheader("Model Usage Summary")
st.dataframe(model_summary_df, width="stretch")
if not model_summary_df.empty:
    model_fig = px.bar(
        model_summary_df,
        x="model",
        y="total_cost_usd",
        title="Total Cost by Model",
    )
    st.plotly_chart(model_fig, width="stretch")

st.subheader("Tool Decision and Result Summary")
st.dataframe(tool_summary_df, width="stretch")
if not tool_summary_df.empty:
    tool_fig = px.bar(
        tool_summary_df,
        x="tool_name",
        y="success_rate_pct",
        title="Tool Success Rate (%)",
    )
    st.plotly_chart(tool_fig, width="stretch")

st.subheader("Error Breakdown")
if error_df.empty:
    st.info("No API errors for current filters.")
else:
    st.dataframe(error_df, width="stretch")
    err_fig = px.bar(
        error_df.head(20),
        x="status_code",
        y="error_count",
        color="model",
        title="Top Error Counts by Status Code",
    )
    st.plotly_chart(err_fig, width="stretch")

st.subheader("Retry Summary")
r1, r2, r3 = st.columns(3)
r1.metric("Total API errors", f"{int(retry['total_api_errors']):,}")
r2.metric("Retries after error", f"{int(retry['retries_after_error']):,}")
r3.metric("Retry rate", f"{retry['retry_rate_pct']:.2f}%")

st.subheader("ML: Session Cost Spike Anomaly Detection")
if sessions_df.empty:
    st.info("Session table is not available. Re-run pipeline to populate fact_sessions.")
else:
    sessions_for_scope = sessions_df[sessions_df["session_id"].isin(filtered_events["session_id"].dropna().unique())]
    anomalies = detect_session_anomalies(sessions_for_scope, contamination=contamination)

    a1, a2 = st.columns(2)
    a1.metric("Sessions analyzed", f"{len(anomalies):,}")
    a2.metric("Anomalies flagged", f"{int(anomalies['is_anomaly'].sum()):,}")

    pca_fig = px.scatter(
        anomalies,
        x="pc1",
        y="pc2",
        color="is_anomaly",
        size="total_cost_usd",
        hover_data=["session_id", "user_email", "total_cost_usd", "api_request_count", "error_count"],
        title="PCA Projection of Session Behavior (Anomalies Highlighted)",
    )
    st.plotly_chart(pca_fig, width="stretch")

    st.markdown("Top anomalous sessions by score")
    st.dataframe(
        anomalies[[
            "session_id",
            "user_email",
            "total_cost_usd",
            "api_request_count",
            "duration_seconds",
            "error_count",
            "anomaly_score",
        ]].sort_values("anomaly_score", ascending=True).head(30),
        width="stretch",
    )

st.subheader("ML: Daily Total API Cost Forecast")
history_df, forecast_df, forecast_metrics = forecast_daily_cost(filtered_events, horizon_days=forecast_horizon)

if history_df.empty:
    st.info("No API request cost history for selected filters.")
else:
    hist_plot = history_df.rename(columns={"daily_total_cost_usd": "value"}).copy()
    hist_plot["series"] = "historical"
    fc_plot = forecast_df.rename(columns={"predicted_daily_total_cost_usd": "value"}).copy()
    fc_plot["series"] = "forecast"
    chart_df = pd.concat([hist_plot[["date", "value", "series"]], fc_plot[["date", "value", "series"]]], axis=0)

    forecast_fig = px.line(
        chart_df,
        x="date",
        y="value",
        color="series",
        markers=True,
        title="Daily Total API Cost: Historical and Forecast",
    )
    st.plotly_chart(forecast_fig, width="stretch")

    if forecast_metrics:
        m1, m2, m3 = st.columns(3)
        m1.metric("Forecast model", str(forecast_metrics.get("model", "n/a")))
        if "rmse" in forecast_metrics:
            m2.metric("Holdout RMSE", f"{forecast_metrics['rmse']:.4f}")
        if pd.notna(forecast_metrics.get("r2")):
            m3.metric("Holdout R²", f"{forecast_metrics['r2']:.4f}")

        with st.expander("Forecast model comparison (lower RMSE is better)"):
            comparison_rows = [
                ("seasonal_naive_7", forecast_metrics.get("rmse_seasonal_naive_7")),
                ("moving_average_7", forecast_metrics.get("rmse_moving_average_7")),
                ("linear_regression_lag_features", forecast_metrics.get("rmse_linear_regression_lag_features")),
            ]
            comparison_df = pd.DataFrame(comparison_rows, columns=["model", "holdout_rmse"])
            comparison_df = comparison_df.dropna(subset=["holdout_rmse"]).sort_values("holdout_rmse")
            st.dataframe(comparison_df, width="stretch")
