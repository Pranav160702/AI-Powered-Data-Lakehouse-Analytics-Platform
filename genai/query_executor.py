"""Read-only execution for validated analytics SQL."""

from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


def execute_read_only_query(engine: Engine, sql: str) -> pd.DataFrame:
    """Execute a validated SELECT query."""

    with engine.connect() as connection:
        return pd.read_sql_query(text(sql), connection)
