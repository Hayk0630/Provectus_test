import json

import pandas as pd

from src.transform import build_events_table, build_sessions_table


def test_build_events_table_flattens_payload() -> None:
    payload = {
        "body": "claude_code.api_request",
        "attributes": {
            "event.name": "api_request",
            "session.id": "s1",
            "user.email": "u@example.com",
            "model": "claude-haiku-4-5-20251001",
            "cost_usd": "0.12",
            "duration_ms": "1500",
            "input_tokens": "12",
            "output_tokens": "7",
        },
    }

    batches = pd.DataFrame(
        {
            "year": [2026],
            "month": [1],
            "day": [1],
            "logEvents": [[{"id": "e1", "timestamp": 1700000000000, "message": json.dumps(payload)}]],
        }
    )

    events = build_events_table(batches)
    assert len(events) == 1
    assert events.loc[0, "event_name"] == "api_request"
    assert events.loc[0, "user_email"] == "u@example.com"
    assert float(events.loc[0, "cost_usd"]) == 0.12


def test_build_sessions_table_aggregates_counts() -> None:
    events = pd.DataFrame(
        {
            "event_id": ["1", "2", "3"],
            "timestamp": pd.to_datetime(
                ["2026-01-01T00:00:00Z", "2026-01-01T00:00:05Z", "2026-01-01T00:00:08Z"],
                utc=True,
            ),
            "body": ["claude_code.user_prompt", "claude_code.api_request", "claude_code.api_error"],
            "event_name": ["user_prompt", "api_request", "api_error"],
            "session_id": ["s1", "s1", "s1"],
            "user_email": ["u@example.com", "u@example.com", "u@example.com"],
            "cost_usd": [pd.NA, 0.5, pd.NA],
            "input_tokens": [pd.NA, 10, pd.NA],
            "output_tokens": [pd.NA, 5, pd.NA],
            "cache_read_tokens": [pd.NA, 2, pd.NA],
            "model": [pd.NA, "m1", pd.NA],
            "tool_name": [pd.NA, pd.NA, pd.NA],
        }
    )

    sessions = build_sessions_table(events)
    assert len(sessions) == 1
    assert int(sessions.loc[0, "total_events"]) == 3
    assert int(sessions.loc[0, "api_request_count"]) == 1
    assert int(sessions.loc[0, "error_count"]) == 1
