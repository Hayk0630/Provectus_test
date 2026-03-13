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


def _api_requests(events: pd.DataFrame) -> pd.DataFrame:
    api = events.loc[events["event_name"] == "api_request"].copy()
    api["timestamp"] = pd.to_datetime(api["timestamp"], utc=True, errors="coerce", format="mixed")
    api["cost_usd"] = pd.to_numeric(api.get("cost_usd"), errors="coerce")
    api["input_tokens"] = pd.to_numeric(api.get("input_tokens"), errors="coerce")
    api["output_tokens"] = pd.to_numeric(api.get("output_tokens"), errors="coerce")
    api["cache_read_tokens"] = pd.to_numeric(api.get("cache_read_tokens"), errors="coerce")
    api["duration_ms"] = pd.to_numeric(api.get("duration_ms"), errors="coerce")
    return api


def _coerce_bool(value: object) -> bool | pd._libs.missing.NAType:
    """Parse mixed boolean encodings from SQLite/text payloads."""

    if pd.isna(value):
        return pd.NA

    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)) and not pd.isna(value):
        if value == 1:
            return True
        if value == 0:
            return False

    text = str(value).strip().lower()
    if text in {"true", "1", "t", "yes", "y"}:
        return True
    if text in {"false", "0", "f", "no", "n"}:
        return False
    return pd.NA


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
    hourly["timestamp"] = pd.to_datetime(hourly["timestamp"], utc=True, errors="coerce", format="mixed")
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


def token_trends_by_segment(
    events: pd.DataFrame,
    employees: pd.DataFrame,
    segment: str = "practice",
    freq: str = "D",
) -> pd.DataFrame:
    """Aggregate token/cost trends over time by employee segment.

    Args:
        segment: employees column to segment by (for example practice/level/location).
        freq: pandas time frequency for bucketing timestamps.
    """

    api = _api_requests(events)

    # If events are already enriched with employee metadata, avoid duplicate merge suffixes.
    if segment in api.columns:
        joined = api.copy()
    else:
        if segment not in employees.columns:
            raise ValueError(f"Unknown segment column: {segment}")
        joined = api.merge(
            employees[["email", segment]],
            left_on="user_email",
            right_on="email",
            how="left",
        )

    joined["period"] = joined["timestamp"].dt.floor(freq)

    result = (
        joined.groupby(["period", segment], dropna=False)
        .agg(
            api_request_count=("event_id", "count"),
            total_input_tokens=("input_tokens", "sum"),
            total_output_tokens=("output_tokens", "sum"),
            total_cache_read_tokens=("cache_read_tokens", "sum"),
            total_cost_usd=("cost_usd", "sum"),
            active_users=("user_email", "nunique"),
        )
        .reset_index()
        .sort_values(["period", segment])
    )
    return result


def model_usage_summary(events: pd.DataFrame) -> pd.DataFrame:
    """Summarize API usage and cost by model."""

    api = _api_requests(events)
    result = (
        api.groupby("model", dropna=False)
        .agg(
            api_request_count=("event_id", "count"),
            total_cost_usd=("cost_usd", "sum"),
            avg_cost_usd=("cost_usd", "mean"),
            avg_duration_ms=("duration_ms", "mean"),
            total_input_tokens=("input_tokens", "sum"),
            total_output_tokens=("output_tokens", "sum"),
        )
        .reset_index()
        .sort_values("total_cost_usd", ascending=False)
    )

    total_reqs = result["api_request_count"].sum()
    total_cost = result["total_cost_usd"].sum()
    result["request_share_pct"] = (result["api_request_count"] / total_reqs * 100).fillna(0)
    result["cost_share_pct"] = (result["total_cost_usd"] / total_cost * 100).fillna(0)
    return result


def tool_usage_summary(events: pd.DataFrame) -> pd.DataFrame:
    """Summarize tool decisions and tool execution outcomes."""

    decisions = events.loc[
        events["event_name"] == "tool_decision", ["tool_name", "decision", "event_id"]
    ].copy()
    results = events.loc[
        events["event_name"] == "tool_result", ["tool_name", "success", "duration_ms", "event_id"]
    ].copy()

    if not results.empty:
        success_as_bool = results["success"].map(_coerce_bool)
        results["success_bool"] = success_as_bool
        results["duration_ms"] = pd.to_numeric(results["duration_ms"], errors="coerce")
    else:
        results["success_bool"] = pd.Series(dtype="boolean")

    dec_agg = (
        decisions.groupby("tool_name", dropna=False)
        .agg(
            decision_events=("event_id", "count"),
            accept_count=("decision", lambda s: int((s == "accept").sum())),
            reject_count=("decision", lambda s: int((s == "reject").sum())),
        )
        .reset_index()
    )

    res_agg = (
        results.groupby("tool_name", dropna=False)
        .agg(
            result_events=("event_id", "count"),
            success_count=("success_bool", lambda s: int((s == True).sum())),
            failure_count=("success_bool", lambda s: int((s == False).sum())),
            avg_duration_ms=("duration_ms", "mean"),
        )
        .reset_index()
    )

    merged = dec_agg.merge(res_agg, on="tool_name", how="outer").fillna(0)
    merged["decision_events"] = pd.to_numeric(merged["decision_events"], errors="coerce").fillna(0)
    merged["result_events"] = pd.to_numeric(merged["result_events"], errors="coerce").fillna(0)
    merged["accept_count"] = pd.to_numeric(merged["accept_count"], errors="coerce").fillna(0)
    merged["success_count"] = pd.to_numeric(merged["success_count"], errors="coerce").fillna(0)

    accept_denom = merged["decision_events"].where(merged["decision_events"] != 0, pd.NA)
    success_denom = merged["result_events"].where(merged["result_events"] != 0, pd.NA)

    merged["accept_rate_pct"] = (merged["accept_count"] / accept_denom * 100).fillna(0)
    merged["success_rate_pct"] = (merged["success_count"] / success_denom * 100).fillna(0)

    return merged.sort_values("decision_events", ascending=False)


def error_breakdown(events: pd.DataFrame) -> pd.DataFrame:
    """Return grouped API error statistics by status/model/error text."""

    errors = events.loc[events["event_name"] == "api_error"].copy()
    if errors.empty:
        return pd.DataFrame(columns=["status_code", "model", "error", "error_count"])

    result = (
        errors.groupby(["status_code", "model", "error"], dropna=False)
        .size()
        .reset_index(name="error_count")
        .sort_values("error_count", ascending=False)
    )
    return result


def retry_summary(events: pd.DataFrame) -> dict[str, float]:
    """Estimate retry behavior using api_error -> api_request transitions in a session."""

    ordered = events.sort_values(["session_id", "timestamp"]).copy()
    ordered["next_event"] = ordered.groupby("session_id")["event_name"].shift(-1)

    total_errors = float((ordered["event_name"] == "api_error").sum())
    retries_after_error = float(
        ((ordered["event_name"] == "api_error") & (ordered["next_event"] == "api_request")).sum()
    )

    retry_rate = (retries_after_error / total_errors * 100) if total_errors else 0.0
    return {
        "total_api_errors": total_errors,
        "retries_after_error": retries_after_error,
        "retry_rate_pct": retry_rate,
    }
