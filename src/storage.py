"""Storage layer skeleton (SQLite).

Implementation order for this module:
1) Implement connection helper.
2) Implement table writes (dim_employees, fact_events, optional fact_sessions).
3) Implement indexes and query helper.

Acceptance criteria:
- Database file is created automatically if missing.
- Writes are deterministic and idempotent for replace mode.
- Indexed queries for dashboard filters are performant.
"""

import sqlite3
from pathlib import Path

import pandas as pd


def get_connection(db_path: str | Path) -> sqlite3.Connection:
    """Open SQLite connection.

    TODO:
    - Enable pragmas for performance (journal mode, synchronous, cache).
    - Centralize connection timeout and isolation level config.

    Returns:
        sqlite3.Connection: Live DB connection object.
    """

    resolved = Path(db_path).resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(resolved, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row

    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA foreign_keys = ON;")

    return conn


def write_tables(
    db_path: str | Path,
    employees: pd.DataFrame,
    events: pd.DataFrame,
    sessions: pd.DataFrame | None = None,
    if_exists: str = "replace",
) -> dict[str, int]:
    """Persist dataframes into SQLite tables.

    Returns:
        dict[str, int]: Row counts written per table.
    """

    if if_exists not in {"replace", "append"}:
        raise ValueError("if_exists must be either 'replace' or 'append'")

    summary = {
        "dim_employees": int(len(employees)),
        "fact_events": int(len(events)),
        "fact_sessions": int(len(sessions)) if sessions is not None else 0,
    }

    with get_connection(db_path) as conn:
        employees.to_sql("dim_employees", conn, if_exists=if_exists, index=False)
        events.to_sql("fact_events", conn, if_exists=if_exists, index=False)
        if sessions is not None:
            sessions.to_sql("fact_sessions", conn, if_exists=if_exists, index=False)

        create_indexes(conn)

    return summary


def create_indexes(conn: sqlite3.Connection) -> None:
    """Create retrieval-oriented indexes.

    TODO:
    - Add indexes for common dashboard filters and joins.
    - Revisit index set after profiling query plans.
    """

    statements = [
        "CREATE INDEX IF NOT EXISTS idx_employees_email ON dim_employees(email);",
        "CREATE INDEX IF NOT EXISTS idx_events_event_name ON fact_events(event_name);",
        "CREATE INDEX IF NOT EXISTS idx_events_session_id ON fact_events(session_id);",
        "CREATE INDEX IF NOT EXISTS idx_events_user_email ON fact_events(user_email);",
        "CREATE INDEX IF NOT EXISTS idx_events_timestamp ON fact_events(timestamp);",
        "CREATE INDEX IF NOT EXISTS idx_events_model ON fact_events(model);",
        "CREATE INDEX IF NOT EXISTS idx_events_tool_name ON fact_events(tool_name);",
        "CREATE INDEX IF NOT EXISTS idx_events_body ON fact_events(body);",
        "CREATE INDEX IF NOT EXISTS idx_sessions_session_id ON fact_sessions(session_id);",
        "CREATE INDEX IF NOT EXISTS idx_sessions_user_email ON fact_sessions(user_email);",
        "CREATE INDEX IF NOT EXISTS idx_sessions_start_time ON fact_sessions(start_time);",
    ]

    for sql in statements:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:
            # Some tables may not exist yet (for example fact_sessions in partial runs).
            continue


def query_df(db_path: str | Path, sql: str, params: tuple | list | None = None) -> pd.DataFrame:
    """Run SQL query and return dataframe.

    Returns:
        pd.DataFrame: Query result set.
    """

    with get_connection(db_path) as conn:
        return pd.read_sql_query(sql, conn, params=params)
