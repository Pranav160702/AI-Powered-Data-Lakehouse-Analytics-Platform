"""Unit tests for the FastAPI analytics serving layer."""

from __future__ import annotations

from datetime import date

import pandas as pd

from analytics.api_client import load_dashboard_data_from_api
from analytics.kpi_service import DashboardData
from api.app import dashboard_data_payload


def _dashboard_data() -> DashboardData:
    return DashboardData(
        overview_kpis=pd.DataFrame([{"total_revenue": 100.0, "total_orders": 2}]),
        daily_sales=pd.DataFrame([{"sales_date": date(2026, 7, 29), "total_revenue": 100.0}]),
        monthly_sales=pd.DataFrame([{"sales_month": date(2026, 7, 1), "total_revenue": 100.0}]),
        product_performance=pd.DataFrame([{"product_id": 1, "net_revenue": 100.0}]),
        category_performance=pd.DataFrame([{"category_id": 1, "revenue": 100.0}]),
        customer_360=pd.DataFrame([{"customer_id": 1, "lifetime_value": 100.0}]),
        customer_segments=pd.DataFrame([{"customer_segment": "premium", "customer_count": 1}]),
        inventory_health=pd.DataFrame([{"product_id": 1, "inventory_status": "healthy"}]),
        inventory_status=pd.DataFrame([{"inventory_status": "healthy", "product_count": 1}]),
        realtime_metrics=pd.DataFrame(
            [{"window_start": pd.Timestamp("2026-07-29T10:00:00"), "orders_per_window": 2}]
        ),
    )


def test_dashboard_data_payload_serializes_all_datasets() -> None:
    """API payload should include every dashboard dataset as records."""

    payload = dashboard_data_payload(_dashboard_data())

    assert set(payload) == {
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
    }
    assert payload["daily_sales"][0]["sales_date"].startswith("2026-07-29")
    assert payload["realtime_metrics"][0]["window_start"].startswith("2026-07-29")


def test_load_dashboard_data_from_api_builds_dashboard_data(monkeypatch) -> None:
    """API client should convert API JSON into DashboardData frames."""

    payload = dashboard_data_payload(_dashboard_data())

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self):
            return payload

    class FakeClient:
        def __init__(self, timeout: float):
            self.timeout = timeout

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def get(self, url: str, params: dict):
            assert url == "http://api.example/api/v1/dashboard-data"
            assert params == {"limit": 10}
            return FakeResponse()

    monkeypatch.setattr("analytics.api_client.httpx.Client", FakeClient)

    data = load_dashboard_data_from_api("http://api.example/", limit=10)

    assert not data.overview_kpis.empty
    assert data.product_performance.iloc[0]["product_id"] == 1
