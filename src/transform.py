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


def _get_nested(data: dict, path: tuple[str, ...]) -> object:
    current: object = data
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _extract_message_fields(message: dict) -> dict[str, object]:
    attributes = message.get("attributes") if isinstance(message.get("attributes"), dict) else {}

    def _attr_value(dotted_key: str, nested_path: tuple[str, ...] | None = None) -> object:
        if dotted_key in attributes:
            return attributes.get(dotted_key)
        if nested_path:
            return _get_nested(attributes, nested_path)
        return None

    return {
        "body": _get_nested(message, ("body",)),
        "event_name": _attr_value("event.name", ("event", "name")),
        "session_id": _attr_value("session.id", ("session", "id")),
        "user_email": _attr_value("user.email", ("user", "email")),
        "organization_id": _attr_value("organization.id", ("organization", "id")),
        "terminal_type": _attr_value("terminal.type", ("terminal", "type")),
        "model": _attr_value("model"),
        "input_tokens": _attr_value("input_tokens"),
        "output_tokens": _attr_value("output_tokens"),
        "cache_read_tokens": _attr_value("cache_read_tokens"),
        "cache_creation_tokens": _attr_value("cache_creation_tokens"),
        "cost_usd": _attr_value("cost_usd"),
        "duration_ms": _attr_value("duration_ms"),
        "tool_name": _attr_value("tool_name"),
        "decision": _attr_value("decision"),
        "decision_source": _attr_value("decision_source"),
        "decision_type": _attr_value("decision_type"),
        "success": _attr_value("success"),
        "error": _attr_value("error"),
        "status_code": _attr_value("status_code"),
        "prompt_length": _attr_value("prompt_length"),
        "attempt": _attr_value("attempt"),
    }


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

    message_values = event_wrapper.get("message", pd.Series(dtype="object"))
    parsed_records: list[dict[str, object]] = []
    skipped = 0

    for raw_message in message_values.tolist():
        parsed = _safe_parse_message(raw_message)
        if parsed is None:
            skipped += 1
            parsed_records.append({})
        else:
            parsed_records.append(_extract_message_fields(parsed))

    if skipped:
        print(f"[TRANSFORM] WARN: skipped {skipped} events with invalid JSON message payload")

    parsed_df = pd.DataFrame(parsed_records)

    events = pd.DataFrame(
        {
            "event_id": event_wrapper.get("id"),
            "timestamp": pd.to_datetime(event_wrapper.get("timestamp"), unit="ms", utc=True, errors="coerce"),
            "batch_year": pd.to_numeric(exploded["year"], errors="coerce"),
            "batch_month": pd.to_numeric(exploded["month"], errors="coerce"),
            "batch_day": pd.to_numeric(exploded["day"], errors="coerce"),
            "body": parsed_df.get("body"),
            "event_name": parsed_df.get("event_name"),
            "session_id": parsed_df.get("session_id"),
            "user_email": parsed_df.get("user_email"),
            "organization_id": parsed_df.get("organization_id"),
            "terminal_type": parsed_df.get("terminal_type"),
            "model": parsed_df.get("model"),
            "input_tokens": pd.to_numeric(parsed_df.get("input_tokens"), errors="coerce"),
            "output_tokens": pd.to_numeric(parsed_df.get("output_tokens"), errors="coerce"),
            "cache_read_tokens": pd.to_numeric(parsed_df.get("cache_read_tokens"), errors="coerce"),
            "cache_creation_tokens": pd.to_numeric(parsed_df.get("cache_creation_tokens"), errors="coerce"),
            "cost_usd": pd.to_numeric(parsed_df.get("cost_usd"), errors="coerce"),
            "duration_ms": pd.to_numeric(parsed_df.get("duration_ms"), errors="coerce"),
            "tool_name": parsed_df.get("tool_name"),
            "decision": parsed_df.get("decision"),
            "decision_source": parsed_df.get("decision_source"),
            "decision_type": parsed_df.get("decision_type"),
            "success": parsed_df.get("success"),
            "error": parsed_df.get("error"),
            "status_code": parsed_df.get("status_code"),
            "prompt_length": pd.to_numeric(parsed_df.get("prompt_length"), errors="coerce"),
            "attempt": pd.to_numeric(parsed_df.get("attempt"), errors="coerce"),
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
