"""FastAPI REST API for dashboard analytics."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy.exc import SQLAlchemyError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from analytics.kpi_service import DashboardData, create_dashboard_engine, load_dashboard_data
from config.logging_config import configure_logging

configure_logging()

app = FastAPI(
    title="Lakehouse Analytics API",
    description="REST API over PostgreSQL serving tables for the Streamlit dashboard.",
    version="1.0.0",
)


def dataframe_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Convert a DataFrame into JSON-serializable records."""

    return json.loads(df.to_json(orient="records", date_format="iso"))


def dashboard_data_payload(data: DashboardData) -> dict[str, list[dict[str, Any]]]:
    """Convert dashboard data into an API response payload."""

    return {
        "overview_kpis": dataframe_records(data.overview_kpis),
        "daily_sales": dataframe_records(data.daily_sales),
        "monthly_sales": dataframe_records(data.monthly_sales),
        "product_performance": dataframe_records(data.product_performance),
        "category_performance": dataframe_records(data.category_performance),
        "customer_360": dataframe_records(data.customer_360),
        "customer_segments": dataframe_records(data.customer_segments),
        "inventory_health": dataframe_records(data.inventory_health),
        "inventory_status": dataframe_records(data.inventory_status),
        "realtime_metrics": dataframe_records(data.realtime_metrics),
    }


@app.get("/health")
def health() -> dict[str, str]:
    """Return API health."""

    return {"status": "ok"}


@app.get("/api/v1/dashboard-data")
def get_dashboard_data(
    limit: int = Query(default=25, ge=1, le=500),
) -> dict[str, list[dict[str, Any]]]:
    """Return all dashboard datasets from the PostgreSQL serving layer."""

    try:
        engine = create_dashboard_engine()
        try:
            data = load_dashboard_data(engine, limit=limit)
        finally:
            engine.dispose()
    except (ConnectionError, SQLAlchemyError) as exc:
        raise HTTPException(
            status_code=503,
            detail="PostgreSQL serving layer is not ready.",
        ) from exc

    return dashboard_data_payload(data)
