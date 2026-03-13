import sys
from pathlib import Path

"""Pipeline entrypoint skeleton.

Global implementation sequence for the whole project:
1) src/config.py
2) src/ingestion.py
3) src/validation.py
4) src/transform.py
5) src/storage.py
6) scripts/run_pipeline.py
7) src/analytics.py
8) src/plots.py
9) app.py

Expected pipeline outputs when complete:
- SQLite database with cleaned analytics-ready tables.
- Row-count and quality summary logs.
"""

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config import load_settings
from src.ingestion import read_employees_csv, read_telemetry_jsonl
from src.transform import build_events_table, build_sessions_table
from src.validation import raise_on_issues, validate_batches, validate_employees
from src.storage import write_tables


def main() -> None:
    """Orchestrate end-to-end data pipeline.

    TODO execution order:
    1) Load settings.
    2) Ingest employees and telemetry raw data.
    3) Run validation; fail fast on critical issues.
    4) Transform telemetry to canonical event table.
    5) Persist dimension/fact tables to SQLite.
    6) Print or log row-count summary.

    Returns:
        None
    """

    settings = load_settings(".")

    print("[PIPELINE] Loading source files...")
    employees = read_employees_csv(settings.employees_path)
    batches = read_telemetry_jsonl(settings.telemetry_path)

    print("[PIPELINE] Validating source data...")
    issues = validate_employees(employees) + validate_batches(batches)
    raise_on_issues(issues)

    print("[PIPELINE] Transforming telemetry into analytics tables...")
    events = build_events_table(batches)
    sessions = build_sessions_table(events)

    print("[PIPELINE] Writing tables to SQLite...")
    summary = write_tables(
        db_path=settings.sqlite_path,
        employees=employees,
        events=events,
        sessions=sessions,
        if_exists="replace",
    )

    print("[PIPELINE] Completed successfully.")
    print(f"[PIPELINE] Database: {settings.sqlite_path}")
    for table_name, row_count in summary.items():
        print(f"[PIPELINE] {table_name}: {row_count} rows")


if __name__ == "__main__":
    main()
