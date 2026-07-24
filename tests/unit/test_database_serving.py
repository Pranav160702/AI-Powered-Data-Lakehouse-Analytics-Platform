"""Unit tests for PostgreSQL serving-layer metadata."""

from config.settings import Settings
from database.models import GOLD_TABLE_MODELS, get_gold_table_model


def test_postgres_url_uses_environment_settings_without_logging() -> None:
    """Settings should compose a usable SQLAlchemy PostgreSQL URL."""

    settings = Settings(
        postgres_host="db",
        postgres_port=5433,
        postgres_db="analytics",
        postgres_user="user",
        postgres_password="secret",
    )

    assert (
        settings.postgres_sqlalchemy_url
        == "postgresql+psycopg2://user:secret@db:5433/analytics"
    )


def test_gold_serving_models_cover_phase4_tables() -> None:
    """Every Phase 4 Gold table should have serving metadata."""

    assert set(GOLD_TABLE_MODELS) == {
        "daily_sales_summary",
        "product_performance",
        "category_performance",
        "customer_360",
        "inventory_health",
        "realtime_metrics",
    }
    assert get_gold_table_model("daily_sales_summary").columns[0] == "sales_date"
