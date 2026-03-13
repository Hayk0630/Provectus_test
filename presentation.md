# Claude Code Usage Analytics Platform

## Page 1 - Executive Summary

### Objective
Build an end-to-end analytics platform for Claude Code telemetry to answer:
- how usage is distributed across users, sessions, tools, and models,
- where API cost is concentrated,
- how reliable workflows are,
- and how to detect unusual sessions and forecast cost.

### Work Completed
- Designed and implemented a modular Python pipeline:
  - ingestion,
  - validation,
  - transformation,
  - SQLite storage,
  - analytics layer,
  - Streamlit dashboard,
  - ML extensions.
- Added automated tests for validation, transform, analytics, and ML modules.
- Added quality improvements after debugging:
  - fixed tool success-rate parsing,
  - improved forecast logic using holdout-backtested model selection,
  - improved anomaly features for cost-spike behavior.

### Final Scope Metrics
- Total events: 454,428
- Active users: 100
- Sessions: 5,000
- API requests: 118,014
- Total API cost: $6,001.43

### Main Outcome
The project now provides a reliable decision-support dashboard that can be used by engineering leadership to track cost, reliability, and anomalous usage patterns.

---

## Page 2 - Data Understanding and Engineering Work

### Source Data
- telemetry_logs.jsonl: each line is a batch containing multiple log events.
- employees.csv: user metadata (name, practice, level, location).

### Critical Data Insight
A single JSONL line is not one interaction.
- Batch level: transport envelope.
- Event level: one exploded log event (analytics unit).
- Session level: grouped behavioral unit for productivity/reliability analysis.

### Engineering Pipeline
1. Ingest raw CSV and JSONL safely.
2. Validate schema and quality with warnings vs hard errors.
3. Explode nested telemetry and parse message payloads.
4. Build canonical fact_events and derived fact_sessions.
5. Store in SQLite with indexes for dashboard performance.

### Deliverables Built
- Reproducible CLI pipeline run.
- Queryable local analytics database.
- Chart-ready aggregation functions.
- End-to-end test suite for core logic stability.

### Why This Matters
Without this normalization step, analytics would be inaccurate (batch-level distortions, wrong interaction counts, incorrect joins with employees).

---

## Page 3 - Findings: Usage, Cost, Reliability

### Cost Distribution by Model
Top cost drivers:
- claude-opus-4-5-20251101: $2,193.69
- claude-opus-4-6: $2,064.51
- claude-sonnet-4-5-20250929: $1,365.51

Interpretation:
- A relatively small set of higher-capability models drives most spend.
- Cost optimization should prioritize model-routing policy before broad user-level limits.

### Cost Distribution by Seniority Level
Top level contributors:
- L6: $1,321.12 (25,858 requests)
- L5: $1,225.48 (24,352 requests)
- L4: $859.42 (16,627 requests)

Interpretation:
- Mid-to-senior engineers are the largest usage and cost segment.
- Governance should be segment-aware, not one-size-fits-all.

### Tool Reliability
Most used tools and success rates:
- Read: 46,015 decisions, 98.51% success
- Bash: 43,214 decisions, 93.04% success
- Edit: 19,127 decisions, 99.11% success
- Grep: 11,565 decisions, 98.96% success

Interpretation:
- Bash has materially lower success than other common tools and should be a reliability focus area.

### API Error and Retry Behavior
- Total API errors: 1,362
- Retries after error: 1,362
- Observed retry rate: 100%

Interpretation:
- Retry handling is very aggressive.
- Potential next step: verify idempotency and avoid hidden retry loops for expensive calls.

---

## Page 4 - ML Findings, Forecasting, and Next Steps

### Session Anomaly Detection
Approach:
- Isolation Forest on session-level behavioral features,
- PCA projection for visualization,
- contamination set to 0.05.

Results:
- Sessions analyzed: 5,000
- Anomalies flagged: 250
- Most anomalous sessions typically combine:
  - high total cost,
  - high request volume,
  - long duration,
  - elevated error count.

Business Use:
- Investigate outliers for runaway sessions, workflow inefficiencies, or model misuse.

### Daily API Cost Forecast
Improved method:
- Compare multiple baseline models on holdout RMSE,
- select best model automatically.

Current result:
- Selected model: linear_regression_lag_features
- Holdout RMSE: 13.54
- Historical daily mean: 100.02
- Forecast daily mean: 99.47

Interpretation:
- Forecast is now aligned with historical scale and avoids unrealistic drift.

### Recommended Next Steps
1. Add alerting rules for anomaly score + cost thresholds.
2. Investigate Bash-specific failure patterns to improve tool reliability.
3. Add policy simulation for model routing (cost vs latency tradeoff).
4. Track weekly forecast error and retrain model selection policy over time.
5. Add product-facing KPIs (cost per active user, cost per successful session).

## Appendix - Talking Points for Live Demo
- Start with KPI overview and filter controls.
- Show cost concentration by model and level.
- Show tool success-rate chart and discuss Bash gap.
- Show anomaly table plus PCA chart for outlier explainability.
- End with forecast chart and model comparison panel.
