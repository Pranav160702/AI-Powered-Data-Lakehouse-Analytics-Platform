"""HTTP client for the FastAPI analytics serving layer."""

from __future__ import annotations

from typing import Any

import httpx
import pandas as pd

from analytics.kpi_service import DashboardData


DATASET_NAMES = (
    "overview_kpis",
    "daily_sales",
    "monthly_sales",
    "product_performance",
    "category_performance",
    "customer_360",
    "customer_segments",
    "inventory_health",
    "inventory_status",
    "realtime_metrics",
)


def _frame(payload: dict[str, list[dict[str, Any]]], name: str) -> pd.DataFrame:
    return pd.DataFrame(payload.get(name, []))


def load_dashboard_data_from_api(base_url: str, limit: int = 25) -> DashboardData:
    """Load dashboard datasets from the FastAPI analytics API."""

    url = f"{base_url.rstrip('/')}/api/v1/dashboard-data"
    with httpx.Client(timeout=30.0) as client:
        response = client.get(url, params={"limit": limit})
        response.raise_for_status()
        payload: dict[str, list[dict[str, Any]]] = response.json()

    return DashboardData(
        overview_kpis=_frame(payload, "overview_kpis"),
        daily_sales=_frame(payload, "daily_sales"),
        monthly_sales=_frame(payload, "monthly_sales"),
        product_performance=_frame(payload, "product_performance"),
        category_performance=_frame(payload, "category_performance"),
        customer_360=_frame(payload, "customer_360"),
        customer_segments=_frame(payload, "customer_segments"),
        inventory_health=_frame(payload, "inventory_health"),
        inventory_status=_frame(payload, "inventory_status"),
        realtime_metrics=_frame(payload, "realtime_metrics"),
    )
