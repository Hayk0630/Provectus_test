import pandas as pd

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


def _sample_events() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "event_id": ["1", "2", "3", "4", "5", "6", "7", "8"],
            "timestamp": pd.to_datetime(
                [
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T01:00:00Z",
                    "2026-01-01T01:05:00Z",
                    "2026-01-01T02:00:00Z",
                    "2026-01-01T02:01:00Z",
                    "2026-01-01T02:02:00Z",
                    "2026-01-01T02:03:00Z",
                    "2026-01-01T03:00:00Z",
                ],
                utc=True,
            ),
            "body": [
                "claude_code.user_prompt",
                "claude_code.api_request",
                "claude_code.api_request",
                "claude_code.api_error",
                "claude_code.api_request",
                "claude_code.tool_decision",
                "claude_code.tool_result",
                "claude_code.api_error",
            ],
            "event_name": [
                "user_prompt",
                "api_request",
                "api_request",
                "api_error",
                "api_request",
                "tool_decision",
                "tool_result",
                "api_error",
            ],
            "session_id": ["s1", "s1", "s2", "s2", "s2", "s2", "s2", "s3"],
            "user_email": [
                "a@example.com",
                "a@example.com",
                "b@example.com",
                "b@example.com",
                "b@example.com",
                "b@example.com",
                "b@example.com",
                "a@example.com",
            ],
            "cost_usd": [pd.NA, 0.2, 0.3, pd.NA, 0.1, pd.NA, pd.NA, pd.NA],
            "input_tokens": [pd.NA, 10, 20, pd.NA, 5, pd.NA, pd.NA, pd.NA],
            "output_tokens": [pd.NA, 5, 7, pd.NA, 3, pd.NA, pd.NA, pd.NA],
            "cache_read_tokens": [pd.NA, 2, 4, pd.NA, 1, pd.NA, pd.NA, pd.NA],
            "model": [pd.NA, "m1", "m2", pd.NA, "m2", pd.NA, pd.NA, "m1"],
            "duration_ms": [pd.NA, 100, 120, 50, 90, pd.NA, 200, 70],
            "tool_name": [pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, "Read", "Read", pd.NA],
            "decision": [pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, "accept", pd.NA, pd.NA],
            "success": [pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, True, pd.NA],
            "error": [pd.NA, pd.NA, pd.NA, "rate limit", pd.NA, pd.NA, pd.NA, "internal"],
            "status_code": [pd.NA, pd.NA, pd.NA, "429", pd.NA, pd.NA, pd.NA, "500"],
        }
    )


def _sample_employees() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "email": ["a@example.com", "b@example.com"],
            "level": ["L4", "L5"],
            "full_name": ["A", "B"],
            "practice": ["Backend Engineering", "Data Engineering"],
            "location": ["US", "PL"],
        }
    )


def test_level_cost_stats_has_expected_columns() -> None:
    df = level_cost_stats(_sample_events(), _sample_employees())
    expected = {
        "level",
        "request_count",
        "unique_users",
        "total_cost_usd",
        "mean_cost_per_request_usd",
        "median_cost_per_request_usd",
        "cost_per_user_usd",
    }
    assert expected.issubset(df.columns)


def test_event_mix_counts_rows() -> None:
    mix = event_mix(_sample_events())
    assert int(mix["event_count"].sum()) == 8


def test_peak_usage_by_hour_extracts_hours() -> None:
    hourly = peak_usage_by_hour(_sample_events())
    assert set(hourly["hour"].tolist()) == {0, 1, 2, 3}


def test_overview_kpis_returns_expected_keys() -> None:
    kpis = overview_kpis(_sample_events())
    assert set(kpis.keys()) == {
        "total_events",
        "active_users",
        "sessions",
        "api_request_count",
        "total_api_cost_usd",
    }


def test_token_trends_by_segment_returns_expected_columns() -> None:
    trends = token_trends_by_segment(_sample_events(), _sample_employees(), segment="practice")
    expected = {
        "period",
        "practice",
        "api_request_count",
        "total_input_tokens",
        "total_output_tokens",
        "total_cache_read_tokens",
        "total_cost_usd",
        "active_users",
    }
    assert expected.issubset(trends.columns)


def test_model_usage_summary_has_share_columns() -> None:
    summary = model_usage_summary(_sample_events())
    assert {"model", "request_share_pct", "cost_share_pct"}.issubset(summary.columns)


def test_tool_usage_summary_contains_rate_metrics() -> None:
    tools = tool_usage_summary(_sample_events())
    assert {"tool_name", "accept_rate_pct", "success_rate_pct"}.issubset(tools.columns)


def test_error_breakdown_returns_counts() -> None:
    breakdown = error_breakdown(_sample_events())
    assert int(breakdown["error_count"].sum()) == 2


def test_retry_summary_detects_retry_after_error() -> None:
    summary = retry_summary(_sample_events())
    assert summary["total_api_errors"] == 2.0
    assert summary["retries_after_error"] == 1.0
