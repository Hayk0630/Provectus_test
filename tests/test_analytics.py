import pandas as pd

from src.analytics import event_mix, level_cost_stats, overview_kpis, peak_usage_by_hour


def _sample_events() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "event_id": ["1", "2", "3", "4"],
            "timestamp": pd.to_datetime(
                [
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T01:00:00Z",
                    "2026-01-01T01:05:00Z",
                    "2026-01-01T02:00:00Z",
                ],
                utc=True,
            ),
            "body": [
                "claude_code.user_prompt",
                "claude_code.api_request",
                "claude_code.api_request",
                "claude_code.api_error",
            ],
            "event_name": ["user_prompt", "api_request", "api_request", "api_error"],
            "session_id": ["s1", "s1", "s2", "s2"],
            "user_email": ["a@example.com", "a@example.com", "b@example.com", "b@example.com"],
            "cost_usd": [pd.NA, 0.2, 0.3, pd.NA],
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
    assert int(mix["event_count"].sum()) == 4


def test_peak_usage_by_hour_extracts_hours() -> None:
    hourly = peak_usage_by_hour(_sample_events())
    assert set(hourly["hour"].tolist()) == {0, 1, 2}


def test_overview_kpis_returns_expected_keys() -> None:
    kpis = overview_kpis(_sample_events())
    assert set(kpis.keys()) == {
        "total_events",
        "active_users",
        "sessions",
        "api_request_count",
        "total_api_cost_usd",
    }
