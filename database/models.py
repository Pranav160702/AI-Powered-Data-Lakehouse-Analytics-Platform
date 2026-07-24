"""Serving-layer table metadata for Gold analytics outputs."""

from dataclasses import dataclass

from sqlalchemy import BigInteger, Date, DateTime, Float, String
from sqlalchemy.types import TypeEngine


@dataclass(frozen=True)
class GoldTableModel:
    """Column order and SQLAlchemy types for one serving table."""

    table_name: str
    columns: tuple[str, ...]
    dtype: dict[str, TypeEngine]


GOLD_TABLE_MODELS: dict[str, GoldTableModel] = {
    "daily_sales_summary": GoldTableModel(
        table_name="daily_sales_summary",
        columns=(
            "sales_date",
            "total_orders",
            "total_revenue",
            "total_items_sold",
            "average_order_value",
            "unique_customers",
        ),
        dtype={
            "sales_date": Date(),
            "total_orders": BigInteger(),
            "total_revenue": Float(),
            "total_items_sold": BigInteger(),
            "average_order_value": Float(),
            "unique_customers": BigInteger(),
        },
    ),
    "product_performance": GoldTableModel(
        table_name="product_performance",
        columns=(
            "product_id",
            "product_name",
            "category_name",
            "units_sold",
            "gross_revenue",
            "discount_amount",
            "net_revenue",
            "average_rating",
        ),
        dtype={
            "product_id": BigInteger(),
            "product_name": String(255),
            "category_name": String(255),
            "units_sold": BigInteger(),
            "gross_revenue": Float(),
            "discount_amount": Float(),
            "net_revenue": Float(),
            "average_rating": Float(),
        },
    ),
    "category_performance": GoldTableModel(
        table_name="category_performance",
        columns=(
            "category_id",
            "category_name",
            "total_orders",
            "units_sold",
            "revenue",
            "average_order_value",
        ),
        dtype={
            "category_id": BigInteger(),
            "category_name": String(255),
            "total_orders": BigInteger(),
            "units_sold": BigInteger(),
            "revenue": Float(),
            "average_order_value": Float(),
        },
    ),
    "customer_360": GoldTableModel(
        table_name="customer_360",
        columns=(
            "customer_id",
            "customer_name",
            "total_orders",
            "lifetime_value",
            "average_order_value",
            "last_order_date",
            "days_since_last_order",
            "customer_segment",
        ),
        dtype={
            "customer_id": BigInteger(),
            "customer_name": String(255),
            "total_orders": BigInteger(),
            "lifetime_value": Float(),
            "average_order_value": Float(),
            "last_order_date": Date(),
            "days_since_last_order": BigInteger(),
            "customer_segment": String(100),
        },
    ),
    "inventory_health": GoldTableModel(
        table_name="inventory_health",
        columns=(
            "product_id",
            "product_name",
            "stock_quantity",
            "reorder_level",
            "inventory_status",
            "estimated_days_remaining",
        ),
        dtype={
            "product_id": BigInteger(),
            "product_name": String(255),
            "stock_quantity": BigInteger(),
            "reorder_level": BigInteger(),
            "inventory_status": String(50),
            "estimated_days_remaining": Float(),
        },
    ),
    "realtime_metrics": GoldTableModel(
        table_name="realtime_metrics",
        columns=(
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
        dtype={
            "window_start": DateTime(),
            "window_end": DateTime(),
            "revenue_per_window": Float(),
            "orders_per_window": BigInteger(),
            "active_users": BigInteger(),
            "product_views": BigInteger(),
            "payment_failures": BigInteger(),
            "add_to_cart_events": BigInteger(),
            "checkout_events": BigInteger(),
            "payment_failure_rate": Float(),
            "cart_abandonment_rate": Float(),
        },
    ),
}


def get_gold_table_model(table_name: str) -> GoldTableModel:
    """Return serving metadata for a supported Gold table."""

    try:
        return GOLD_TABLE_MODELS[table_name]
    except KeyError as exc:
        available = ", ".join(sorted(GOLD_TABLE_MODELS))
        raise ValueError(f"Unknown Gold serving table '{table_name}'. Available: {available}") from exc
