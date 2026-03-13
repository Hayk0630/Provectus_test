"""Ingestion layer skeleton.

Implementation order for this module:
1) Implement read_employees_csv.
2) Implement read_telemetry_jsonl with line-by-line parsing.
3) Add structured parse error reporting.

Acceptance criteria:
- Returns a dataframe for employees with expected columns loaded.
- Returns a dataframe for telemetry where each row is one JSONL batch.
- Gracefully handles empty lines and reports malformed JSONL rows.
"""

from pathlib import Path
import json

import pandas as pd


class JsonLineParseError(RuntimeError):
    """Raised when telemetry JSONL parsing fails.

    Attributes:
        error_count: Number of malformed lines encountered.
        sample_errors: Up to first 5 line-level parse error messages.
    """

    def __init__(self, message: str, error_count: int, sample_errors: list[str]):
        super().__init__(message)
        self.error_count = error_count
        self.sample_errors = sample_errors


def _ensure_file_exists(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    if not path.is_file():
        raise ValueError(f"Path is not a file: {path}")


def read_employees_csv(path: str | Path) -> pd.DataFrame:
    """Load the employee dimension dataset.

    Returns:
        pd.DataFrame: Raw employees table as loaded from CSV.
    """

    resolved = Path(path).resolve()
    _ensure_file_exists(resolved)

    # Keep employee fields as strings to avoid accidental type coercion.
    return pd.read_csv(resolved, dtype=str)


def read_telemetry_jsonl(path: str | Path) -> pd.DataFrame:
    """Load telemetry batches from JSONL.

    Returns:
        pd.DataFrame: Batch-level telemetry table.
    """

    resolved = Path(path).resolve()
    _ensure_file_exists(resolved)

    records: list[dict] = []
    errors: list[str] = []

    with resolved.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            raw = line.strip()
            if not raw:
                continue

            try:
                obj = json.loads(raw)
                if not isinstance(obj, dict):
                    errors.append(f"line {line_number}: expected object, got {type(obj).__name__}")
                    continue
                records.append(obj)
            except json.JSONDecodeError as exc:
                errors.append(f"line {line_number}: {exc}")

    if errors:
        sample = errors[:5]
        summary = (
            f"Failed to parse telemetry JSONL. "
            f"Malformed lines: {len(errors)}. "
            f"Sample errors: {' | '.join(sample)}"
        )
        raise JsonLineParseError(summary, error_count=len(errors), sample_errors=sample)

    return pd.DataFrame(records)
