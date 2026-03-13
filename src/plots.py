"""Plot builder skeletons.

TODO:
- Implement chart constructors (Plotly/Altair) using analytics dataframes.
- Keep chart styling centralized for consistent dashboard visuals.

Implementation note:
- Keep plotting functions pure: dataframe in, figure out.
- Avoid embedding SQL or business logic in this module.
"""

import plotly.express as px


def build_cost_by_level_chart(df):
    """Return chart object for cost-by-level view.

    Returns:
        object: Visualization object expected by Streamlit plotting call.
    """

    if df.empty:
        return px.bar(title="Cost by Level (no data)")

    return px.bar(
        df,
        x="level",
        y="total_cost_usd",
        hover_data=["request_count", "unique_users", "mean_cost_per_request_usd"],
        title="Total API Cost by Seniority Level",
    )


def build_event_mix_chart(df):
    """Return chart object for event mix view."""

    if df.empty:
        return px.bar(title="Event Mix (no data)")

    return px.bar(
        df.head(20),
        x="event_name",
        y="event_count",
        color="body",
        title="Top Event Types",
    )


def build_hourly_usage_chart(df):
    """Return chart object for hourly activity profile."""

    if df.empty:
        return px.line(title="Hourly Usage (no data)")

    return px.line(
        df,
        x="hour",
        y="event_count",
        markers=True,
        title="Hourly Event Volume",
    )
