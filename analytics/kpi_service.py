"""Dashboard data-access service for PostgreSQL serving tables."""

from __future__ import annotations

import logging
import warnings
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import pandas as pd
from sqlalchemy.engine import Engine

from analytics import queries
from database.connection import get_engine_with_retry

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DashboardData:
    """Container for dashboard-ready DataFrames."""

    overview_kpis: pd.DataFrame
    daily_sales: pd.DataFrame
    monthly_sales: pd.DataFrame
    product_performance: pd.DataFrame
    category_performance: pd.DataFrame
    customer_360: pd.DataFrame
    customer_segments: pd.DataFrame
    inventory_health: pd.DataFrame
    inventory_status: pd.DataFrame
    realtime_metrics: pd.DataFrame


def run_query(
    engine: Engine,
    sql: str,
    params: Mapping[str, Any] | None = None,
) -> pd.DataFrame:
    """Execute one read-only SQL query and return a pandas DataFrame."""

    connection = engine.raw_connection()
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="pandas only supports SQLAlchemy connectable",
                category=UserWarning,
            )
            return pd.read_sql_query(sql, connection, params=params)
    finally:
        connection.close()


def create_dashboard_engine() -> Engine:
    """Create a PostgreSQL engine for dashboard use."""

    return get_engine_with_retry(retries=1)


def load_dashboard_data(engine: Engine, limit: int = 25) -> DashboardData:
    """Load all batch-dashboard datasets from PostgreSQL."""

    logger.info("Loading dashboard data from PostgreSQL")
    return DashboardData(
        overview_kpis=run_query(engine, queries.OVERVIEW_KPIS),
        daily_sales=run_query(engine, queries.DAILY_SALES),
        monthly_sales=run_query(engine, queries.MONTHLY_SALES),
        product_performance=run_query(
            engine, queries.PRODUCT_PERFORMANCE, params={"limit": limit}
        ),
        category_performance=run_query(engine, queries.CATEGORY_PERFORMANCE),
        customer_360=run_query(engine, queries.CUSTOMER_360, params={"limit": limit}),
        customer_segments=run_query(engine, queries.CUSTOMER_SEGMENTS),
        inventory_health=run_query(engine, queries.INVENTORY_HEALTH, params={"limit": limit}),
        inventory_status=run_query(engine, queries.INVENTORY_STATUS),
        realtime_metrics=run_query(engine, queries.REALTIME_METRICS, params={"limit": limit}),
    )
