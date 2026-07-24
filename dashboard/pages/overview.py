"""Overview dashboard page."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from analytics.kpi_service import DashboardData
from dashboard.components.charts import bar_chart, line_chart
from dashboard.components.metric_cards import format_currency, format_number, render_metric_row


def render(data: DashboardData) -> None:
    """Render the executive overview page."""

    kpis = data.overview_kpis.iloc[0].to_dict() if not data.overview_kpis.empty else {}
    render_metric_row(
        [
            ("Revenue", format_currency(kpis.get("total_revenue"))),
            ("Orders", format_number(kpis.get("total_orders"))),
            ("AOV", format_currency(kpis.get("average_order_value"))),
            ("Customers", format_number(kpis.get("unique_customers"))),
            ("Units Sold", format_number(kpis.get("units_sold"))),
        ]
    )
    line_chart(data.daily_sales, "sales_date", "total_revenue", "Daily revenue")
    bar_chart(
        data.category_performance,
        "category_name",
        "revenue",
        "Revenue by category",
    )
