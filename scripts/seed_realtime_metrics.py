"""Seed demo real-time metrics into PostgreSQL for the dashboard."""

from __future__ import annotations

import logging
import random
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.logging_config import configure_logging
from database.connection import execute_sql_file, get_engine_with_retry

logger = logging.getLogger(__name__)


def build_metric_rows(window_count: int = 18, seed: int = 42) -> list[dict]:
    """Build recent synthetic real-time metric windows."""

    rng = random.Random(seed)
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    first_window_start = now - timedelta(minutes=window_count * 5)
    rows = []

    for index in range(window_count):
        window_start = first_window_start + timedelta(minutes=index * 5)
        window_end = window_start + timedelta(minutes=5)
        orders = rng.randint(18, 95)
        payment_failures = rng.randint(0, max(1, orders // 8))
        add_to_cart = orders + rng.randint(25, 130)
        checkout = orders + payment_failures + rng.randint(5, 40)
        product_views = add_to_cart + rng.randint(120, 650)
        active_users = rng.randint(max(orders, 40), max(orders + 260, 300))
        revenue = round(orders * rng.uniform(1850.0, 4200.0), 2)

        rows.append(
            {
                "window_start": window_start,
                "window_end": window_end,
                "revenue_per_window": revenue,
                "orders_per_window": orders,
                "active_users": active_users,
                "product_views": product_views,
                "payment_failures": payment_failures,
                "add_to_cart_events": add_to_cart,
                "checkout_events": checkout,
                "payment_failure_rate": round(
                    payment_failures / max(orders + payment_failures, 1), 4
                ),
                "cart_abandonment_rate": round(1 - (orders / max(add_to_cart, 1)), 4),
            }
        )

    return rows


def seed_realtime_metrics(window_count: int = 18) -> int:
    """Replace real-time serving rows with recent demo metrics."""

    engine = get_engine_with_retry()
    sql_file = PROJECT_ROOT / "database" / "create_tables.sql"
    insert_sql = text(
        """
        INSERT INTO realtime_metrics (
            window_start,
            window_end,
            revenue_per_window,
            orders_per_window,
            active_users,
            product_views,
            payment_failures,
            add_to_cart_events,
            checkout_events,
            payment_failure_rate,
            cart_abandonment_rate
        )
        VALUES (
            :window_start,
            :window_end,
            :revenue_per_window,
            :orders_per_window,
            :active_users,
            :product_views,
            :payment_failures,
            :add_to_cart_events,
            :checkout_events,
            :payment_failure_rate,
            :cart_abandonment_rate
        )
        """
    )
    rows = build_metric_rows(window_count=window_count)

    try:
        execute_sql_file(engine, sql_file)
        with engine.begin() as connection:
            connection.execute(text("DELETE FROM realtime_metrics"))
            connection.execute(insert_sql, rows)
    finally:
        engine.dispose()

    return len(rows)


def main() -> None:
    """Run the real-time demo metric seeder."""

    configure_logging()
    rows_seeded = seed_realtime_metrics()
    logger.info("Seeded %s real-time metric windows into PostgreSQL", rows_seeded)


if __name__ == "__main__":
    main()
