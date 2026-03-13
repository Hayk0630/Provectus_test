# Claude Code Usage Analytics Platform

End-to-end telemetry analytics project for synthetic Claude Code usage data.

This repository implements:
- data ingestion and validation,
- transformation to analytics-ready event/session tables,
- SQLite storage,
- a Streamlit dashboard with business and reliability insights,
- and ML components for anomaly detection + daily API cost forecasting.

## 1) Project Goal

Build a practical analytics platform to answer:
- How is Claude Code being used (events, sessions, users, tools, models)?
- Where does API cost come from (level, model, team segment)?
- What reliability issues exist (errors, retries, tool failures)?
- Which sessions are anomalous (cost spikes / unusual behavior)?
- What is a reasonable short-horizon daily cost forecast?

## 2) Dataset and Semantics

### Input files
- `telemetry_logs.jsonl`: JSONL where each line is one **transport batch** containing `logEvents`.
- `employees.csv`: employee directory (`email`, `full_name`, `practice`, `level`, `location`).

### Important modeling note
One JSONL line is **not** one interaction. It is a batch that can contain multiple event records.

Pipeline semantics:
- **Batch level**: one JSONL line.
- **Event level**: one row after exploding `logEvents`.
- **Session level**: one row after grouping by `session_id`.

## 3) Repository Structure

```text
.
├─ app.py
├─ scripts/
│  ├─ run_pipeline.py
│  └─ run_ml.py
├─ src/
│  ├─ config.py
│  ├─ ingestion.py
│  ├─ validation.py
│  ├─ transform.py
│  ├─ storage.py
│  ├─ analytics.py
│  ├─ plots.py
│  └─ ml.py
├─ tests/
│  ├─ test_validation.py
│  ├─ test_transform.py
│  ├─ test_analytics.py
│  └─ test_ml.py
├─ telemetry_logs.jsonl
├─ employees.csv
├─ analytics.db
└─ requirements.txt
```

## 4) Data Pipeline

Pipeline entrypoint: `scripts/run_pipeline.py`

### Steps
1. Load file paths from `src/config.py`.
2. Ingest source data using `src/ingestion.py`.
3. Validate schema and quality with `src/validation.py`.
4. Transform into canonical fact tables via `src/transform.py`.
5. Persist to SQLite with indexes using `src/storage.py`.

### Output tables
- `dim_employees`
- `fact_events` (canonical event table)
- `fact_sessions` (session aggregates)

## 5) Canonical Event Schema (fact_events)

Key columns include:
- event identity and time: `event_id`, `timestamp`
- user/session dimensions: `user_email`, `session_id`, `organization_id`, `terminal_type`
- event classification: `body`, `event_name`
- API metrics: `model`, `cost_usd`, `input_tokens`, `output_tokens`, `cache_read_tokens`, `duration_ms`
- tool metrics: `tool_name`, `decision`, `decision_source`, `decision_type`, `success`
- reliability fields: `error`, `status_code`, `attempt`

## 6) Dashboard Capabilities

Main app: `app.py`

Implemented sections:
- KPI overview (events, users, sessions, API requests, total cost)
- Cost by seniority level
- Event mix and hourly usage
- Token/cost trends by segment
- Model usage summary
- Tool decision/result summary with success rates
- Error breakdown and retry summary
- ML anomaly section (session outliers + PCA)
- ML forecast section (daily total API cost)

Filters:
- date range, practice, level,
- model and tool,
- full-dataset toggle.

## 7) ML Components

Module: `src/ml.py`

### 7.1 Session anomaly detection
- Algorithm: `IsolationForest`
- Features include session totals and derived behavior rates:
	- `total_cost_usd`, `api_request_count`, `duration_seconds`, `error_count`,
	- `cost_per_request`, `error_rate_pct`, `tokens_per_request`, and more.
- PCA projection (`pc1`, `pc2`) is used for 2D visualization.

### 7.2 Daily cost forecasting
- Target: daily total API cost (`api_request` only).
- Forecasting now uses holdout-backtested model selection among:
	- `seasonal_naive_7`
	- `moving_average_7`
	- `linear_regression_lag_features`
- Dashboard displays selected model + RMSE comparison table.

### 7.3 About contamination
`contamination` in IsolationForest is the expected outlier proportion (for example `0.05` means about 5% sessions flagged).
It is conceptually a strictness knob, but not a p-value alpha from hypothesis testing.

## 8) Setup and Run

### Prerequisites
- Python 3.10+
- Recommended: virtual environment

### Install
```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### Run pipeline
```bash
python scripts/run_pipeline.py
```

### Run tests
```bash
python -m pytest -q tests
```

### Run dashboard
```bash
python -m streamlit run app.py
```

### Run ML smoke script
```bash
python scripts/run_ml.py
```

## 9) Configuration

Optional environment overrides (`src/config.py`):
- `TELEMETRY_PATH`
- `EMPLOYEES_PATH`
- `SQLITE_PATH`

Example:
```bash
set TELEMETRY_PATH=C:\path\to\telemetry_logs.jsonl
set EMPLOYEES_PATH=C:\path\to\employees.csv
set SQLITE_PATH=C:\path\to\analytics.db
python scripts/run_pipeline.py
```

## 10) Testing and Quality

Current automated tests cover:
- validation logic,
- transformations,
- analytics aggregations,
- ML helpers.

Run all checks:
```bash
python -m pytest -q tests
```

## 11) Presentation Prep (3-5 pages)

Suggested slide flow:
1. Problem + data semantics (batch vs event vs session)
2. Architecture and pipeline design
3. Core findings (cost, model/tool usage, errors/retries)
4. ML outcomes (anomaly examples + forecast behavior)
5. Recommendations and next steps

## 12) Known Limitations / Next Steps

- Forecasting is baseline-focused; production forecasting may use richer seasonality and exogenous features.
- Anomaly labels are unsupervised (no ground-truth fraud/incident labels).
- Additional deliverables (formal report/video) can be built on top of this README and dashboard exports.
