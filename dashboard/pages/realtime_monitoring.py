"""Real-time monitoring dashboard page."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from analytics.kpi_service import DashboardData
from dashboard.components.charts import line_chart
from dashboard.components.metric_cards import format_currency, format_number, render_metric_row


def _latest_metrics(df: pd.DataFrame) -> dict:
    """Return the latest real-time metrics row as a dict."""

    if df.empty:
        return {}
    ordered = df.sort_values("window_start", ascending=False)
    return ordered.iloc[0].to_dict()


def render(data: DashboardData) -> None:
    """Render real-time monitoring analytics."""

    realtime = data.realtime_metrics.copy()
    if realtime.empty:
        st.info("No real-time metrics loaded yet. Run Kafka streams and load `realtime_metrics` to PostgreSQL.")
        return

    realtime["window_start"] = pd.to_datetime(realtime["window_start"])
    latest = _latest_metrics(realtime)
    render_metric_row(
        [
            ("Live Revenue", format_currency(latest.get("revenue_per_window"))),
            ("Live Orders", format_number(latest.get("orders_per_window"))),
            ("Active Users", format_number(latest.get("active_users"))),
            ("Product Views", format_number(latest.get("product_views"))),
            ("Payment Failures", format_number(latest.get("payment_failures"))),
        ]
    )

    ordered = realtime.sort_values("window_start")
    line_chart(ordered, "window_start", "revenue_per_window", "Revenue per window")
    line_chart(ordered, "window_start", "orders_per_window", "Orders per window")
    line_chart(ordered, "window_start", "active_users", "Active users")
    st.dataframe(ordered, use_container_width=True, hide_index=True)
