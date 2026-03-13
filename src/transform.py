"""Transformation layer skeleton.

Implementation order for this module:
1) Implement build_events_table first (core fact table).
2) Add typed conversions (timestamp, numeric costs/tokens/durations).
3) Implement build_sessions_table as a derived aggregate.

Acceptance criteria:
- One row in output events table equals one telemetry event.
- Nested message payload fields are flattened into canonical columns.
- Session table is reproducible from events and suitable for dashboard use.
"""

import pandas as pd
import json


_EVENT_COLUMNS = [
    "event_id",
    "timestamp",
    "batch_year",
    "batch_month",
    "batch_day",
    "body",
    "event_name",
    "session_id",
    "user_email",
    "organization_id",
    "terminal_type",
    "model",
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "cache_creation_tokens",
    "cost_usd",
    "duration_ms",
    "tool_name",
    "decision",
    "decision_source",
    "decision_type",
    "success",
    "error",
    "status_code",
    "prompt_length",
    "attempt",
]


def _empty_events_df() -> pd.DataFrame:
    return pd.DataFrame(columns=_EVENT_COLUMNS)


def _safe_parse_message(raw: object) -> dict | None:
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str) or not raw:
        return None
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        return None


def build_events_table(batches: pd.DataFrame) -> pd.DataFrame:
    """Transform telemetry batches into one-row-per-event table.

    TODO:
    - Explode `logEvents` so each event is one row.
    - Parse nested JSON string in event `message`.
    - Flatten required fields into typed analytics columns.
    - Keep only canonical columns needed by SQL and dashboard layers.

    Returns:
        pd.DataFrame: Clean event-level fact table.
    """

    required = {"logEvents", "year", "month", "day"}
    missing = required - set(batches.columns)
    if missing:
        raise ValueError(f"Cannot transform batches. Missing columns: {sorted(missing)}")

    if batches.empty:
        return _empty_events_df()

    exploded = batches[["year", "month", "day", "logEvents"]].explode("logEvents", ignore_index=True)
    exploded = exploded.dropna(subset=["logEvents"])
    if exploded.empty:
        return _empty_events_df()

    event_wrapper = pd.json_normalize(exploded["logEvents"])
    parsed_messages = event_wrapper.get("message", pd.Series(dtype="object")).map(_safe_parse_message)

    valid_mask = parsed_messages.notna()
    if not bool(valid_mask.all()):
        skipped = int((~valid_mask).sum())
        print(f"[TRANSFORM] WARN: skipped {skipped} events with invalid JSON message payload")

    event_wrapper = event_wrapper[valid_mask].reset_index(drop=True)
    parsed_messages = parsed_messages[valid_mask].reset_index(drop=True)
    exploded = exploded[valid_mask].reset_index(drop=True)

    if event_wrapper.empty:
        return _empty_events_df()

    message = pd.json_normalize(parsed_messages)

    events = pd.DataFrame(
        {
            "event_id": event_wrapper.get("id"),
            "timestamp": pd.to_datetime(event_wrapper.get("timestamp"), unit="ms", utc=True, errors="coerce"),
            "batch_year": pd.to_numeric(exploded["year"], errors="coerce"),
            "batch_month": pd.to_numeric(exploded["month"], errors="coerce"),
            "batch_day": pd.to_numeric(exploded["day"], errors="coerce"),
            "body": message.get("body"),
            "event_name": message.get("attributes.event.name"),
            "session_id": message.get("attributes.session.id"),
            "user_email": message.get("attributes.user.email"),
            "organization_id": message.get("attributes.organization.id"),
            "terminal_type": message.get("attributes.terminal.type"),
            "model": message.get("attributes.model"),
            "input_tokens": pd.to_numeric(message.get("attributes.input_tokens"), errors="coerce"),
            "output_tokens": pd.to_numeric(message.get("attributes.output_tokens"), errors="coerce"),
            "cache_read_tokens": pd.to_numeric(message.get("attributes.cache_read_tokens"), errors="coerce"),
            "cache_creation_tokens": pd.to_numeric(message.get("attributes.cache_creation_tokens"), errors="coerce"),
            "cost_usd": pd.to_numeric(message.get("attributes.cost_usd"), errors="coerce"),
            "duration_ms": pd.to_numeric(message.get("attributes.duration_ms"), errors="coerce"),
            "tool_name": message.get("attributes.tool_name"),
            "decision": message.get("attributes.decision"),
            "decision_source": message.get("attributes.decision_source"),
            "decision_type": message.get("attributes.decision_type"),
            "success": message.get("attributes.success"),
            "error": message.get("attributes.error"),
            "status_code": message.get("attributes.status_code"),
            "prompt_length": pd.to_numeric(message.get("attributes.prompt_length"), errors="coerce"),
            "attempt": pd.to_numeric(message.get("attributes.attempt"), errors="coerce"),
        }
    )

    events["success"] = events["success"].map(
        lambda v: True if str(v).lower() == "true" else (False if str(v).lower() == "false" else pd.NA)
    )

    for col in _EVENT_COLUMNS:
        if col not in events.columns:
            events[col] = pd.NA

    return events[_EVENT_COLUMNS]


def build_sessions_table(events: pd.DataFrame) -> pd.DataFrame:
    """Create optional session-level aggregates from events.

    TODO:
    - Group by session_id.
    - Derive session duration, event counts, and cost/token rollups.

    Returns:
        pd.DataFrame: Session-level aggregate table for quick dashboard retrieval.
    """

    if events.empty:
        return pd.DataFrame(
            columns=[
                "session_id",
                "user_email",
                "start_time",
                "end_time",
                "duration_seconds",
                "total_events",
                "api_request_count",
                "error_count",
                "total_cost_usd",
                "total_input_tokens",
                "total_output_tokens",
                "total_cache_read_tokens",
                "unique_models",
                "unique_tools",
            ]
        )

    session_events = events.dropna(subset=["session_id"]).copy()
    if session_events.empty:
        return pd.DataFrame()

    session_events = session_events.sort_values(["session_id", "timestamp"])

    grouped = session_events.groupby("session_id", dropna=False)
    summary = grouped.agg(
        user_email=("user_email", "first"),
        start_time=("timestamp", "min"),
        end_time=("timestamp", "max"),
        total_events=("event_id", "count"),
        api_request_count=("event_name", lambda s: int((s == "api_request").sum())),
        error_count=("event_name", lambda s: int((s == "api_error").sum())),
        total_cost_usd=("cost_usd", "sum"),
        total_input_tokens=("input_tokens", "sum"),
        total_output_tokens=("output_tokens", "sum"),
        total_cache_read_tokens=("cache_read_tokens", "sum"),
        unique_models=("model", lambda s: int(s.dropna().nunique())),
        unique_tools=("tool_name", lambda s: int(s.dropna().nunique())),
    ).reset_index()

    summary["duration_seconds"] = (
        (summary["end_time"] - summary["start_time"]).dt.total_seconds().fillna(0)
    )

    ordered_cols = [
        "session_id",
        "user_email",
        "start_time",
        "end_time",
        "duration_seconds",
        "total_events",
        "api_request_count",
        "error_count",
        "total_cost_usd",
        "total_input_tokens",
        "total_output_tokens",
        "total_cache_read_tokens",
        "unique_models",
        "unique_tools",
    ]
    return summary[ordered_cols]
