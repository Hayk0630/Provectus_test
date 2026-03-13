# Claude Code Analytics Platform

End-to-end analytics platform for synthetic Claude Code telemetry data.

This project ingests telemetry batches from JSONL, structures them into analytics-ready event and session tables, stores them in SQLite, and presents interactive insights through a Streamlit dashboard. It also includes ML components for anomaly detection and daily API cost forecasting.

## Table of Contents

- Overview
- Key Results
- Assignment Coverage
- Architecture
- Repository Structure
- Quick Start
- Data Pipeline
- Dashboard Capabilities
- ML Components
- Database Schema
- Technologies
- Configuration
- Submission Documents

## Overview

The goal of this project is to turn raw Claude Code telemetry into actionable insights about:
- developer behavior,
- API cost concentration,
- model and tool usage,
- reliability issues,
- and unusual session patterns.

The implementation is aligned with the assignment requirements:
- data processing and storage,
- analytics and insight extraction,
- interactive visualization,
- error handling and validation,
- and bonus ML functionality.

## Key Results

| Metric | Value |
|------|------:|
| Total events | 454,428 |
| API requests | 118,014 |
| Tool decisions | 151,461 |
| Tool results | 148,418 |
| User prompts | 35,173 |
| API errors | 1,362 |
| Employees | 100 |
| Sessions | 5,000 |
| Total API cost | $6,001.43 |
| Raw JSONL size | 521.4 MB |

## Assignment Coverage

### Required components
- Data processing: implemented with ingestion, validation, transformation, and SQLite persistence.
- Analytics and insights: implemented with KPI, cost, usage, reliability, and trend aggregations.
- Visualization: implemented with a Streamlit dashboard and Plotly charts.
- Technical implementation: includes validation, error handling, modular architecture, and tests.

### Bonus components
- Predictive analytics: implemented with anomaly detection and cost forecasting.
- AI-first workflow: documented in the LLM usage log with prompt examples and validation steps.

## Architecture

```text
generate_fake_data.py
	|
	v
Raw files: telemetry_logs.jsonl + employees.csv
	|
	v
scripts/run_pipeline.py
	|
	+--> src/ingestion.py     (load raw files)
	+--> src/validation.py    (schema + quality checks)
	+--> src/transform.py     (batch -> event -> session)
	+--> src/storage.py       (SQLite + indexes)
	|
	v
analytics.db
	|
	+--> src/analytics.py     (aggregations and KPI logic)
	+--> src/ml.py            (anomalies + forecasting)
	+--> src/plots.py         (chart builders)
	|
	v
app.py                           (interactive dashboard)
```

Design principles:
- layered structure,
- reproducible scripts,
- minimal hidden state,
- validation before persistence,
- testable analytics and ML helpers.

## Repository Structure

```text
.
├─ app.py
├─ presentation/
│  ├─ presentation.md
│  └─ LLM_USAGE_LOG.md
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
│  ├─ ml.py
│  └─ __init__.py
├─ tests/
│  ├─ test_validation.py
│  ├─ test_transform.py
│  ├─ test_analytics.py
│  └─ test_ml.py
├─ generate_fake_data.py
├─ README_fake_data.md
├─ README.md
└─ requirements.txt
```

Generated local artifacts that do not need to be committed:
- telemetry_logs.jsonl
- employees.csv
- analytics.db

## Quick Start

### Prerequisites
- Python 3.10+
- virtual environment recommended

### 1. Create environment and install dependencies

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Generate the synthetic dataset

```bash
python generate_fake_data.py --num-users 100 --num-sessions 5000 --days 60 --seed 42
```

Additional data-generation details are documented in `README_fake_data.md`.

The pipeline auto-detects generated files in either the project root or `output/`.

### 3. Run the ETL pipeline

```bash
python scripts/run_pipeline.py
```

This produces:
- `dim_employees`
- `fact_events`
- `fact_sessions`
- SQLite database file: `analytics.db`

### 4. Run tests

```bash
python -m pytest -q tests
```

### 5. Launch dashboard

```bash
python -m streamlit run app.py
```

### 6. Run ML smoke script

```bash
python scripts/run_ml.py
```

## Data Pipeline

### Input semantics
- `telemetry_logs.jsonl`: each line is a transport batch containing `logEvents`.
- `employees.csv`: employee metadata used for segmentation and joins.

Important modeling distinction:
- Batch level: one JSONL line.
- Event level: one exploded log event.
- Session level: one grouped unit by `session_id`.

### Pipeline steps
1. Read CSV and JSONL sources.
2. Validate required fields and basic data quality.
3. Explode nested `logEvents`.
4. Parse nested `message` JSON payloads.
5. Flatten canonical event fields.
6. Derive session aggregates.
7. Persist into SQLite with retrieval-oriented indexes.

## Dashboard Capabilities

The dashboard is implemented in `app.py` and includes:
- KPI overview,
- cost by seniority level,
- event mix,
- hourly usage analysis,
- token and cost trends by segment,
- model usage summary,
- tool decision and result summary,
- error breakdown,
- retry summary,
- anomaly detection with PCA view,
- daily API cost forecasting.

Available filters:
- date range,
- practice,
- level,
- model,
- tool,
- full-dataset toggle.

## ML Components

ML module: `src/ml.py`

### Session anomaly detection
- algorithm: `IsolationForest`
- session features include:
  - total cost,
  - request count,
  - duration,
  - token totals,
  - error count,
  - derived efficiency / rate features
- PCA (`pc1`, `pc2`) is used for visualization.

### Daily cost forecasting
- target: daily total API cost for `api_request` events
- backtested model selection among:
  - `seasonal_naive_7`
  - `moving_average_7`
  - `linear_regression_lag_features`
- dashboard exposes selected model and holdout RMSE comparison.

### Notes
- `contamination` in IsolationForest controls expected anomaly fraction.
- It is a model sensitivity parameter, not a statistical p-value threshold.

## Database Schema

### Tables
- `dim_employees`
- `fact_events`
- `fact_sessions`

### Key `fact_events` columns
- `event_id`, `timestamp`
- `body`, `event_name`
- `user_email`, `session_id`, `organization_id`, `terminal_type`
- `model`, `cost_usd`, `input_tokens`, `output_tokens`, `cache_read_tokens`, `duration_ms`
- `tool_name`, `decision`, `decision_source`, `decision_type`, `success`
- `error`, `status_code`, `attempt`

### Indexing
SQLite indexes are created for common query paths such as:
- event name,
- session id,
- user email,
- timestamp,
- model,
- tool name.

## Technologies

| Category | Technology |
|------|------|
| Language | Python |
| Data processing | pandas |
| Dashboard | Streamlit |
| Charts | Plotly |
| Storage | SQLite |
| ML | scikit-learn |
| Testing | pytest |
| PDF / assignment docs parsing | pypdf |

## Configuration

Optional environment variables supported in `src/config.py`:
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

## Shipping Note

It is acceptable to ship this project without committing raw generated datasets.

Recommended submission contents:
- source code,
- `generate_fake_data.py`,
- `README_fake_data.md`,
- `README.md`,
- `presentation/presentation.md`,
- `presentation/LLM_USAGE_LOG.md`.

This keeps the repository lighter while still allowing the reviewer to regenerate the exact dataset locally.

## Submission Documents

- Presentation: `presentation/presentation.md`
- LLM usage log: `presentation/LLM_USAGE_LOG.md`

