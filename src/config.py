import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    """Project configuration values.

    Environment variable overrides supported:
    - TELEMETRY_PATH
    - EMPLOYEES_PATH
    - SQLITE_PATH
    """

    base_dir: Path
    telemetry_path: Path
    employees_path: Path
    sqlite_path: Path


def load_settings(base_dir: str | Path = ".") -> Settings:
    """Build default settings for local development.

    Returns:
        Settings: Resolved file system paths and database location.
    """

    base = Path(base_dir).resolve()

    telemetry_path = Path(os.getenv("TELEMETRY_PATH", base / "telemetry_logs.jsonl"))
    employees_path = Path(os.getenv("EMPLOYEES_PATH", base / "employees.csv"))
    sqlite_path = Path(os.getenv("SQLITE_PATH", base / "analytics.db"))

    if not telemetry_path.is_absolute():
        telemetry_path = (base / telemetry_path).resolve()
    if not employees_path.is_absolute():
        employees_path = (base / employees_path).resolve()
    if not sqlite_path.is_absolute():
        sqlite_path = (base / sqlite_path).resolve()

    return Settings(
        base_dir=base,
        telemetry_path=telemetry_path,
        employees_path=employees_path,
        sqlite_path=sqlite_path,
    )
