import pandas as pd

from src.ml import (
    build_daily_cost_series,
    build_session_feature_matrix,
    detect_session_anomalies,
    forecast_daily_cost,
)


def _sample_sessions() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "session_id": ["s1", "s2", "s3", "s4"],
            "user_email": ["a@example.com", "b@example.com", "c@example.com", "d@example.com"],
            "total_cost_usd": [0.5, 0.8, 10.0, 0.6],
            "api_request_count": [5, 8, 80, 6],
            "total_input_tokens": [100, 120, 4000, 110],
            "total_output_tokens": [60, 70, 2000, 55],
            "total_cache_read_tokens": [30, 50, 1500, 40],
            "duration_seconds": [50, 60, 1500, 55],
            "error_count": [0, 1, 12, 0],
            "unique_models": [1, 1, 3, 1],
            "unique_tools": [2, 2, 8, 2],
        }
    )


def _sample_events(days: int = 40) -> pd.DataFrame:
    rows = []
    base_date = pd.Timestamp("2026-01-01T00:00:00Z")
    for i in range(days):
        rows.append(
            {
                "event_id": str(i),
                "timestamp": base_date + pd.Timedelta(days=i),
                "event_name": "api_request",
                "cost_usd": 10 + (i % 7) * 0.5,
            }
        )
    return pd.DataFrame(rows)


def test_build_session_feature_matrix_has_expected_columns() -> None:
    features = build_session_feature_matrix(_sample_sessions())
    expected_cols = {
        "total_cost_usd",
        "api_request_count",
        "duration_seconds",
        "cost_per_request",
        "error_rate_pct",
        "tokens_per_request",
    }
    assert expected_cols.issubset(set(features.columns))


def test_detect_session_anomalies_adds_outputs() -> None:
    out = detect_session_anomalies(_sample_sessions(), contamination=0.25)
    expected = {"anomaly_label", "is_anomaly", "anomaly_score", "pc1", "pc2"}
    assert expected.issubset(out.columns)


def test_build_daily_cost_series_aggregates() -> None:
    daily = build_daily_cost_series(_sample_events(10))
    assert len(daily) == 10
    assert "daily_total_cost_usd" in daily.columns


def test_forecast_daily_cost_outputs_horizon() -> None:
    history, forecast, metrics = forecast_daily_cost(_sample_events(60), horizon_days=14)
    assert len(history) > 0
    assert len(forecast) == 14
    assert "model" in metrics
