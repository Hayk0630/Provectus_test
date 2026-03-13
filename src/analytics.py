"""Analytics layer skeleton.

Implementation order for this module:
1) Implement level_cost_stats (assignment-critical metric).
2) Implement event_mix and peak_usage_by_hour.
3) Add additional insights: model mix, error rate, tool performance.

Acceptance criteria:
- Metrics are based on canonical event table fields only.
- Aggregations are reproducible and documented.
- Output tables are directly consumable by dashboard charts.
"""

import pandas as pd


def level_cost_stats(events: pd.DataFrame, employees: pd.DataFrame) -> pd.DataFrame:
    """Compute API request cost metrics grouped by employee level.

    TODO:
    - Filter to event_name == api_request.
    - Join events to employees on user email.
    - Return request_count, unique_users, total/mean/median cost, and cost_per_user.

    Returns:
        pd.DataFrame: Aggregated cost statistics by level.
    """

    api_requests = events.loc[events["event_name"] == "api_request", ["user_email", "cost_usd"]].copy()
    api_requests["cost_usd"] = pd.to_numeric(api_requests["cost_usd"], errors="coerce")

    joined = api_requests.merge(
        employees[["email", "level"]],
        left_on="user_email",
        right_on="email",
        how="left",
    )

    result = (
        joined.groupby("level", dropna=False)
        .agg(
            request_count=("cost_usd", "count"),
            unique_users=("user_email", "nunique"),
            total_cost_usd=("cost_usd", "sum"),
            mean_cost_per_request_usd=("cost_usd", "mean"),
            median_cost_per_request_usd=("cost_usd", "median"),
        )
        .reset_index()
        .sort_values("total_cost_usd", ascending=False)
    )
    result["cost_per_user_usd"] = result["total_cost_usd"] / result["unique_users"].replace(0, pd.NA)
    return result


def event_mix(events: pd.DataFrame) -> pd.DataFrame:
    """Compute event counts by event body/name.

    Returns:
        pd.DataFrame: Event type distribution table.
    """

    mix = (
        events.groupby(["body", "event_name"], dropna=False)
        .size()
        .reset_index(name="event_count")
        .sort_values("event_count", ascending=False)
    )
    return mix


def peak_usage_by_hour(events: pd.DataFrame) -> pd.DataFrame:
    """Compute usage intensity by hour of day.

    Returns:
        pd.DataFrame: Hourly event volume table.
    """

    hourly = events.copy()
    hourly["timestamp"] = pd.to_datetime(hourly["timestamp"], utc=True, errors="coerce")
    hourly = hourly.dropna(subset=["timestamp"])
    hourly["hour"] = hourly["timestamp"].dt.hour

    result = (
        hourly.groupby("hour", dropna=False)
        .agg(
            event_count=("event_id", "count"),
            active_users=("user_email", "nunique"),
            sessions=("session_id", "nunique"),
        )
        .reset_index()
        .sort_values("hour")
    )
    return result


def overview_kpis(events: pd.DataFrame) -> dict[str, float]:
    """Compute high-level KPI values from event-level table."""

    api_cost = pd.to_numeric(
        events.loc[events["event_name"] == "api_request", "cost_usd"], errors="coerce"
    ).sum()

    return {
        "total_events": float(len(events)),
        "active_users": float(events["user_email"].nunique(dropna=True)),
        "sessions": float(events["session_id"].nunique(dropna=True)),
        "api_request_count": float((events["event_name"] == "api_request").sum()),
        "total_api_cost_usd": float(api_cost),
    }
