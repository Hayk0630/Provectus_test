import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config import load_settings
from src.ml import detect_session_anomalies, forecast_daily_cost
from src.storage import query_df


def main() -> None:
    settings = load_settings(".")

    events = query_df(settings.sqlite_path, "SELECT * FROM fact_events")
    sessions = query_df(settings.sqlite_path, "SELECT * FROM fact_sessions")

    anomalies = detect_session_anomalies(sessions, contamination=0.05)
    history, forecast, metrics = forecast_daily_cost(events, horizon_days=30)

    print("[ML] sessions analyzed:", len(anomalies))
    print("[ML] anomalies flagged:", int(anomalies["is_anomaly"].sum()))
    print("[ML] top anomalies:")
    print(anomalies[["session_id", "user_email", "total_cost_usd", "anomaly_score"]].head(10).to_string(index=False))

    print("[ML] forecast model:", metrics.get("model", "n/a"))
    if "rmse" in metrics:
        print("[ML] holdout RMSE:", round(metrics["rmse"], 6))
    if "r2" in metrics:
        print("[ML] holdout R2:", round(metrics["r2"], 6))
    print("[ML] forecast rows:", len(forecast))
    if not forecast.empty:
        print(forecast.head(5).to_string(index=False))


if __name__ == "__main__":
    main()
