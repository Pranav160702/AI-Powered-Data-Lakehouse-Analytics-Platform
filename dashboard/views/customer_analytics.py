"""Customer analytics dashboard page."""

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
    """Render customer analytics views."""

    pie_chart(
        data.customer_segments,
        "customer_segment",
        "customer_count",
        "Customer segmentation",
    )
    bar_chart(
        data.customer_segments,
        "customer_segment",
        "average_order_value",
        "Average order value by segment",
    )
    st.dataframe(data.customer_360, use_container_width=True, hide_index=True)
