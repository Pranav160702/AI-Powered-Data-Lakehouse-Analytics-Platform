"""Product analytics dashboard page."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from analytics.kpi_service import DashboardData
from dashboard.components.charts import bar_chart


def render(data: DashboardData) -> None:
    """Render product performance analytics."""

    bar_chart(
        data.product_performance.head(15),
        "product_name",
        "net_revenue",
        "Top products by revenue",
        color="category_name",
    )
    bar_chart(
        data.product_performance.head(15),
        "product_name",
        "units_sold",
        "Top products by units sold",
        color="category_name",
    )
    st.dataframe(data.product_performance, use_container_width=True, hide_index=True)
