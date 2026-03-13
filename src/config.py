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


def _resolve_input_path(base: Path, env_name: str, default_filename: str) -> Path:
    """Resolve input file location with an output/ fallback.

    Resolution order:
    1) Explicit environment variable path
    2) <base>/<default_filename>
    3) <base>/output/<default_filename>
    """

    env_value = os.getenv(env_name)
    if env_value:
        path = Path(env_value)
        resolved = path if path.is_absolute() else (base / path).resolve()
        if resolved.exists():
            return resolved

    default_path = (base / default_filename).resolve()
    if default_path.exists():
        return default_path

    return (base / "output" / default_filename).resolve()


def load_settings(base_dir: str | Path = ".") -> Settings:
    """Build default settings for local development.

    Returns:
        Settings: Resolved file system paths and database location.
    """

    base = Path(base_dir).resolve()

    telemetry_path = _resolve_input_path(base, "TELEMETRY_PATH", "telemetry_logs.jsonl")
    employees_path = _resolve_input_path(base, "EMPLOYEES_PATH", "employees.csv")
    sqlite_path = Path(os.getenv("SQLITE_PATH", base / "analytics.db"))

    if not sqlite_path.is_absolute():
        sqlite_path = (base / sqlite_path).resolve()

    return Settings(
        base_dir=base,
        telemetry_path=telemetry_path,
        employees_path=employees_path,
        sqlite_path=sqlite_path,
    )
