"""SQLAlchemy connection utilities for the PostgreSQL serving layer."""

from __future__ import annotations

import logging
import time
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine
from sqlalchemy.exc import OperationalError

from config.settings import Settings, get_settings

logger = logging.getLogger(__name__)


def create_postgres_engine(settings: Settings | None = None) -> Engine:
    """Create a pooled SQLAlchemy engine for PostgreSQL."""

    resolved_settings = settings or get_settings()
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=resolved_settings.postgres_user,
        password=resolved_settings.postgres_password,
        host=resolved_settings.postgres_host,
        port=resolved_settings.postgres_port,
        database=resolved_settings.postgres_db,
    )
    return create_engine(
        url,
        pool_pre_ping=True,
        pool_size=resolved_settings.postgres_pool_size,
        max_overflow=resolved_settings.postgres_max_overflow,
        connect_args={"connect_timeout": resolved_settings.postgres_connect_timeout},
        future=True,
    )


def get_engine_with_retry(
    settings: Settings | None = None,
    retries: int = 3,
    retry_delay_seconds: float = 2.0,
) -> Engine:
    """Create and validate a PostgreSQL engine with bounded retry handling."""

    last_error: OperationalError | None = None
    for attempt in range(1, retries + 1):
        engine = create_postgres_engine(settings)
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            logger.info("PostgreSQL connection validated on attempt %s", attempt)
            return engine
        except OperationalError as exc:
            last_error = exc
            logger.warning(
                "PostgreSQL connection attempt %s/%s failed: %s",
                attempt,
                retries,
                exc,
            )
            engine.dispose()
            if attempt < retries:
                time.sleep(retry_delay_seconds)

    raise ConnectionError("Unable to connect to PostgreSQL after retries.") from last_error


def execute_sql_file(engine: Engine, sql_file: Path) -> None:
    """Execute a SQL file inside one transaction."""

    if not sql_file.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file}")
    sql_text = sql_file.read_text(encoding="utf-8")
    with engine.begin() as connection:
        connection.execute(text(sql_text))
    logger.info("Executed SQL file: %s", sql_file)
