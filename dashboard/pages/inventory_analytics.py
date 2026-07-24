"""Inventory analytics dashboard page."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from analytics.kpi_service import DashboardData
from dashboard.components.charts import bar_chart, pie_chart


def render(data: DashboardData) -> None:
    """Render inventory health analytics."""

    pie_chart(
        data.inventory_status,
        "inventory_status",
        "product_count",
        "Inventory status",
    )
    risk = data.inventory_health[
        data.inventory_health["inventory_status"].isin(["out_of_stock", "low_stock"])
    ]
    bar_chart(
        risk,
        "product_name",
        "stock_quantity",
        "Low-stock products",
        color="inventory_status",
    )
    st.dataframe(data.inventory_health, use_container_width=True, hide_index=True)
