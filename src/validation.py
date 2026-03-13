"""Validation layer skeleton.

Implementation order for this module:
1) Implement employee schema and quality checks.
2) Implement telemetry envelope checks.
3) Introduce warning vs error severity split.

Acceptance criteria:
- All required columns are checked explicitly.
- Critical issues block pipeline; non-critical issues are reported.
- Join-readiness checks are available (email completeness/uniqueness).
"""

import pandas as pd


REQUIRED_BATCH_COLUMNS = {
    "messageType",
    "owner",
    "logGroup",
    "logStream",
    "subscriptionFilters",
    "logEvents",
    "year",
    "month",
    "day",
}

REQUIRED_EMPLOYEE_COLUMNS = {"email", "full_name", "practice", "level", "location"}


def _missing_columns(df: pd.DataFrame, required: set[str]) -> list[str]:
    """Compute missing columns helper.

    Returns:
        list[str]: Required column names absent from input dataframe.
    """

    return sorted(required - set(df.columns))


def validate_employees(employees: pd.DataFrame) -> list[str]:
    """Validate employees dataframe quality and schema.

    TODO:
    - Check required columns.
    - Check null and duplicate email values.
    - Check allowed domain constraints (level values, non-empty practice/location).

    Returns:
        list[str]: Human-readable validation issues; empty when valid.
    """

    issues: list[str] = []

    missing = _missing_columns(employees, REQUIRED_EMPLOYEE_COLUMNS)
    if missing:
        issues.append(f"ERROR: employees.csv missing columns: {missing}")
        return issues

    if employees.empty:
        issues.append("ERROR: employees.csv is empty")

    email = employees["email"].astype("string")
    if email.isna().any() or (email.str.strip() == "").any():
        issues.append("ERROR: employees.csv contains null/blank email values")

    duplicate_count = int(email.duplicated().sum())
    if duplicate_count > 0:
        issues.append(f"ERROR: employees.csv contains {duplicate_count} duplicate emails")

    for col in ["full_name", "practice", "level", "location"]:
        series = employees[col].astype("string")
        if series.isna().any() or (series.str.strip() == "").any():
            issues.append(f"WARN: employees.csv column `{col}` has null/blank values")

    valid_level_mask = employees["level"].astype("string").str.fullmatch(r"L([1-9]|10)")
    invalid_levels = int((~valid_level_mask.fillna(False)).sum())
    if invalid_levels > 0:
        issues.append(f"WARN: employees.csv contains {invalid_levels} rows with non-standard level values")

    return issues


def validate_batches(batches: pd.DataFrame) -> list[str]:
    """Validate raw telemetry batch dataframe.

    TODO:
    - Check required envelope columns.
    - Verify logEvents is list-typed.
    - Validate date partition fields and basic bounds.

    Returns:
        list[str]: Human-readable validation issues; empty when valid.
    """

    issues: list[str] = []

    missing = _missing_columns(batches, REQUIRED_BATCH_COLUMNS)
    if missing:
        issues.append(f"ERROR: telemetry_logs.jsonl missing columns: {missing}")
        return issues

    if batches.empty:
        issues.append("ERROR: telemetry_logs.jsonl produced an empty dataframe")

    not_list_count = int((~batches["logEvents"].map(lambda x: isinstance(x, list))).sum())
    if not_list_count > 0:
        issues.append(f"ERROR: found {not_list_count} rows where logEvents is not a list")

    empty_list_count = int(batches["logEvents"].map(lambda x: isinstance(x, list) and len(x) == 0).sum())
    if empty_list_count > 0:
        issues.append(f"WARN: found {empty_list_count} rows with empty logEvents")

    invalid_partitions = batches[
        (~pd.to_numeric(batches["month"], errors="coerce").between(1, 12))
        | (~pd.to_numeric(batches["day"], errors="coerce").between(1, 31))
        | (pd.to_numeric(batches["year"], errors="coerce").isna())
    ]
    if not invalid_partitions.empty:
        issues.append(
            "WARN: telemetry_logs.jsonl contains rows with invalid year/month/day partition values"
        )

    if "messageType" in batches.columns:
        non_data_message = int((batches["messageType"].astype("string") != "DATA_MESSAGE").sum())
        if non_data_message > 0:
            issues.append(f"WARN: found {non_data_message} rows with messageType != DATA_MESSAGE")

    return issues


def raise_on_issues(issues: list[str]) -> None:
    """Raise a single exception when validation issues exist.

    TODO:
    - Separate warnings from hard errors.
    - Emit structured logging payload for CI diagnostics.
    """

    errors = [i for i in issues if i.startswith("ERROR:")]
    warnings = [i for i in issues if i.startswith("WARN:")]

    for warning in warnings:
        print(f"[VALIDATION] {warning}")

    if errors:
        formatted = "\n - ".join([""] + errors)
        raise ValueError(f"Validation failed:{formatted}")
