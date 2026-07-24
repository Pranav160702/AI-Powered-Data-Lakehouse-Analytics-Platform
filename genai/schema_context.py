"""Approved analytics schema context for safe SQL generation."""

APPROVED_TABLES: dict[str, tuple[str, ...]] = {
    "daily_sales_summary": (
        "sales_date",
        "total_orders",
        "total_revenue",
        "total_items_sold",
        "average_order_value",
        "unique_customers",
    ),
    "product_performance": (
        "product_id",
        "product_name",
        "category_name",
        "units_sold",
        "gross_revenue",
        "discount_amount",
        "net_revenue",
        "average_rating",
    ),
    "category_performance": (
        "category_id",
        "category_name",
        "total_orders",
        "units_sold",
        "revenue",
        "average_order_value",
    ),
    "customer_360": (
        "customer_id",
        "customer_name",
        "total_orders",
        "lifetime_value",
        "average_order_value",
        "last_order_date",
        "days_since_last_order",
        "customer_segment",
    ),
    "inventory_health": (
        "product_id",
        "product_name",
        "stock_quantity",
        "reorder_level",
        "inventory_status",
        "estimated_days_remaining",
    ),
    "realtime_metrics": (
        "window_start",
        "window_end",
        "revenue_per_window",
        "orders_per_window",
        "active_users",
        "product_views",
        "payment_failures",
        "add_to_cart_events",
        "checkout_events",
        "payment_failure_rate",
        "cart_abandonment_rate",
    ),
}


def render_schema_context() -> str:
    """Render approved table and column context for the LLM prompt."""

    lines = []
    for table_name, columns in APPROVED_TABLES.items():
        lines.append(f"- {table_name}: {', '.join(columns)}")
    return "\n".join(lines)
