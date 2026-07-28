"""Read-only execution for validated analytics SQL."""

from __future__ import annotations

import pandas as pd
from sqlalchemy.engine import Engine


def execute_read_only_query(engine: Engine, sql: str) -> pd.DataFrame:
    """Execute a validated SELECT query."""

    connection = engine.raw_connection()
    try:
        return pd.read_sql_query(sql, connection)
    finally:
        connection.close()
