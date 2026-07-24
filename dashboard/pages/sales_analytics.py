"""Sales analytics dashboard page."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from analytics.kpi_service import DashboardData
from dashboard.components.charts import bar_chart, line_chart
from dashboard.components.filters import apply_date_range, date_range_filter


def render(data: DashboardData) -> None:
    """Render sales analytics views."""

    start, end = date_range_filter(data.daily_sales, "sales_date")
    daily_sales = apply_date_range(data.daily_sales, "sales_date", start, end)

    line_chart(daily_sales, "sales_date", "total_revenue", "Daily revenue trend")
    line_chart(data.monthly_sales, "sales_month", "total_revenue", "Monthly revenue trend")
    bar_chart(data.category_performance, "category_name", "revenue", "Revenue by category")
    st.dataframe(daily_sales, use_container_width=True, hide_index=True)
