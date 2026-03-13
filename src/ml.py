from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.ensemble import IsolationForest
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.preprocessing import StandardScaler


SESSION_BASE_FEATURE_COLUMNS = [
    "total_cost_usd",
    "api_request_count",
    "total_input_tokens",
    "total_output_tokens",
    "total_cache_read_tokens",
    "duration_seconds",
    "error_count",
    "unique_models",
    "unique_tools",
]

SESSION_DERIVED_FEATURE_COLUMNS = [
    "cost_per_request",
    "error_rate_pct",
    "tokens_per_request",
]

SESSION_FEATURE_COLUMNS = SESSION_BASE_FEATURE_COLUMNS + SESSION_DERIVED_FEATURE_COLUMNS


def build_session_feature_matrix(sessions: pd.DataFrame) -> pd.DataFrame:
    """Build numeric feature matrix used for anomaly detection and PCA."""

    if sessions.empty:
        return pd.DataFrame(columns=SESSION_FEATURE_COLUMNS)

    features = sessions.copy()
    for col in SESSION_BASE_FEATURE_COLUMNS:
        if col not in features.columns:
            features[col] = 0
        features[col] = pd.to_numeric(features[col], errors="coerce").fillna(0.0)

    req_denom = features["api_request_count"].replace(0, np.nan)
    features["cost_per_request"] = (features["total_cost_usd"] / req_denom).fillna(0.0)
    features["error_rate_pct"] = (features["error_count"] / req_denom * 100.0).fillna(0.0)
    token_total = features["total_input_tokens"] + features["total_output_tokens"]
    features["tokens_per_request"] = (token_total / req_denom).fillna(0.0)

    return features[SESSION_FEATURE_COLUMNS]


def detect_session_anomalies(
    sessions: pd.DataFrame,
    contamination: float = 0.05,
    random_state: int = 42,
) -> pd.DataFrame:
    """Detect anomalous sessions with IsolationForest and add PCA coordinates."""

    if sessions.empty:
        return pd.DataFrame(
            columns=list(sessions.columns) + ["anomaly_label", "is_anomaly", "anomaly_score", "pc1", "pc2"]
        )

    features = build_session_feature_matrix(sessions)
    scaler = StandardScaler()
    scaled = scaler.fit_transform(features)

    model = IsolationForest(
        n_estimators=200,
        contamination=contamination,
        random_state=random_state,
        n_jobs=-1,
    )
    labels = model.fit_predict(scaled)
    scores = model.decision_function(scaled)

    pca = PCA(n_components=2, random_state=random_state)
    pcs = pca.fit_transform(scaled)

    out = sessions.copy()
    out["anomaly_label"] = labels
    out["is_anomaly"] = out["anomaly_label"] == -1
    out["anomaly_score"] = scores
    out["pc1"] = pcs[:, 0]
    out["pc2"] = pcs[:, 1]

    return out.sort_values("anomaly_score", ascending=True)


def build_daily_cost_series(events: pd.DataFrame) -> pd.DataFrame:
    """Aggregate daily total API cost from event-level data."""

    if events.empty:
        return pd.DataFrame(columns=["date", "daily_total_cost_usd"])

    df = events.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce", format="mixed")
    df["cost_usd"] = pd.to_numeric(df.get("cost_usd"), errors="coerce")

    api = df[df["event_name"] == "api_request"].dropna(subset=["timestamp"])
    if api.empty:
        return pd.DataFrame(columns=["date", "daily_total_cost_usd"])

    daily = (
        api.groupby(api["timestamp"].dt.floor("D"))
        .agg(daily_total_cost_usd=("cost_usd", "sum"))
        .reset_index()
        .rename(columns={"timestamp": "date"})
    )
    daily["date"] = pd.to_datetime(daily["date"])

    daily = daily.sort_values("date").reset_index(drop=True)
    return daily


def _add_forecast_features(daily: pd.DataFrame) -> pd.DataFrame:
    df = daily.copy()
    df["dow"] = df["date"].dt.dayofweek.astype(float)
    df["lag_1"] = df["daily_total_cost_usd"].shift(1)
    df["lag_7"] = df["daily_total_cost_usd"].shift(7)
    df["roll_7"] = df["daily_total_cost_usd"].rolling(7).mean()
    return df


def _rmse(y_true: pd.Series, y_pred: np.ndarray) -> float:
    return float(np.sqrt(mean_squared_error(y_true, y_pred)))


def _seasonal_naive_forecast(history_values: list[float], horizon_days: int) -> list[float]:
    preds: list[float] = []
    vals = history_values.copy()
    for _ in range(horizon_days):
        if len(vals) >= 7:
            pred = float(vals[-7])
        else:
            pred = float(vals[-1])
        pred = max(pred, 0.0)
        preds.append(pred)
        vals.append(pred)
    return preds


def _moving_average_forecast(history_values: list[float], horizon_days: int) -> list[float]:
    preds: list[float] = []
    vals = history_values.copy()
    for _ in range(horizon_days):
        window = vals[-7:] if len(vals) >= 7 else vals
        pred = max(float(np.mean(window)), 0.0)
        preds.append(pred)
        vals.append(pred)
    return preds


def forecast_daily_cost(
    events: pd.DataFrame,
    horizon_days: int = 14,
    random_state: int = 42,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, float]]:
    """Forecast daily total API cost using backtested baseline model selection."""

    del random_state

    daily = build_daily_cost_series(events)
    if daily.empty:
        return daily, pd.DataFrame(columns=["date", "predicted_daily_total_cost_usd"]), {}

    if len(daily) < 14:
        last_val = float(daily["daily_total_cost_usd"].iloc[-1])
        future_dates = pd.date_range(daily["date"].max() + pd.Timedelta(days=1), periods=horizon_days, freq="D")
        forecast = pd.DataFrame(
            {
                "date": future_dates,
                "predicted_daily_total_cost_usd": [last_val] * horizon_days,
            }
        )
        return daily, forecast, {"model": "naive_last_value"}

    holdout = max(7, int(len(daily) * 0.2))
    if holdout >= len(daily):
        holdout = len(daily) - 1

    history_train = daily.iloc[:-holdout].copy()
    history_test = daily.iloc[-holdout:].copy()

    train_values = history_train["daily_total_cost_usd"].astype(float).tolist()

    # Candidate 1: seasonal naive (weekly pattern).
    sn_preds = np.array(_seasonal_naive_forecast(train_values, len(history_test)))
    sn_rmse = _rmse(history_test["daily_total_cost_usd"], sn_preds)
    sn_mae = float(mean_absolute_error(history_test["daily_total_cost_usd"], sn_preds))

    # Candidate 2: moving average.
    ma_preds = np.array(_moving_average_forecast(train_values, len(history_test)))
    ma_rmse = _rmse(history_test["daily_total_cost_usd"], ma_preds)
    ma_mae = float(mean_absolute_error(history_test["daily_total_cost_usd"], ma_preds))

    # Candidate 3: linear regression with lag-only features (no time trend term).
    full = _add_forecast_features(daily)
    trainable = full.dropna().copy()

    lr_rmse = float("inf")
    lr_mae = float("inf")
    lr_r2 = float("nan")
    lr_model: LinearRegression | None = None

    if len(trainable) >= 14:
        feature_cols = ["dow", "lag_1", "lag_7", "roll_7"]
        holdout_lr = max(7, int(len(trainable) * 0.2))
        if holdout_lr < len(trainable):
            train_df = trainable.iloc[:-holdout_lr]
            test_df = trainable.iloc[-holdout_lr:]

            lr_model = LinearRegression()
            lr_model.fit(train_df[feature_cols], train_df["daily_total_cost_usd"])

            test_pred = lr_model.predict(test_df[feature_cols])
            lr_rmse = _rmse(test_df["daily_total_cost_usd"], test_pred)
            lr_mae = float(mean_absolute_error(test_df["daily_total_cost_usd"], test_pred))
            lr_r2 = float(lr_model.score(test_df[feature_cols], test_df["daily_total_cost_usd"]))

            # Refit for future forecasting.
            lr_model.fit(trainable[feature_cols], trainable["daily_total_cost_usd"])

    scores = {
        "seasonal_naive_7": sn_rmse,
        "moving_average_7": ma_rmse,
        "linear_regression_lag_features": lr_rmse,
    }
    best_model = min(scores, key=scores.get)

    history = daily.copy()
    forecast_rows: list[dict[str, float | pd.Timestamp]] = []

    rolling_series = history["daily_total_cost_usd"].astype(float).tolist()
    last_date = history["date"].max()

    if best_model == "seasonal_naive_7":
        preds = _seasonal_naive_forecast(rolling_series, horizon_days)
    elif best_model == "moving_average_7":
        preds = _moving_average_forecast(rolling_series, horizon_days)
    else:
        preds = []
        assert lr_model is not None
        for step in range(1, horizon_days + 1):
            current_date = last_date + pd.Timedelta(days=step)
            lag_1 = rolling_series[-1]
            lag_7 = rolling_series[-7] if len(rolling_series) >= 7 else rolling_series[-1]
            roll_7 = float(np.mean(rolling_series[-7:])) if len(rolling_series) >= 7 else float(np.mean(rolling_series))

            row = pd.DataFrame(
                {
                    "dow": [float(current_date.dayofweek)],
                    "lag_1": [float(lag_1)],
                    "lag_7": [float(lag_7)],
                    "roll_7": [float(roll_7)],
                }
            )

            pred = max(float(lr_model.predict(row)[0]), 0.0)
            preds.append(pred)
            rolling_series.append(pred)

    for idx, pred in enumerate(preds, start=1):
        forecast_rows.append(
            {
                "date": last_date + pd.Timedelta(days=idx),
                "predicted_daily_total_cost_usd": float(pred),
            }
        )

    forecast = pd.DataFrame(forecast_rows)

    # Use holdout metrics from the selected model.
    if best_model == "seasonal_naive_7":
        selected_rmse = sn_rmse
        selected_mae = sn_mae
        selected_r2 = float("nan")
    elif best_model == "moving_average_7":
        selected_rmse = ma_rmse
        selected_mae = ma_mae
        selected_r2 = float("nan")
    else:
        selected_rmse = lr_rmse
        selected_mae = lr_mae
        selected_r2 = lr_r2

    metrics = {
        "model": best_model,
        "holdout_days": float(holdout),
        "rmse": selected_rmse,
        "mae": selected_mae,
        "r2": selected_r2,
        "rmse_seasonal_naive_7": sn_rmse,
        "rmse_moving_average_7": ma_rmse,
        "rmse_linear_regression_lag_features": lr_rmse,
    }

    return history, forecast, metrics
