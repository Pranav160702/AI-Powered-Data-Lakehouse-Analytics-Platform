"""Load Gold Delta tables into PostgreSQL serving tables."""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from sqlalchemy import Engine, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.logging_config import configure_logging
from config.settings import get_settings
from config.spark_config import spark_session
from database.connection import execute_sql_file, get_engine_with_retry
from database.models import GOLD_TABLE_MODELS, get_gold_table_model

logger = logging.getLogger(__name__)

BATCH_GOLD_TABLES = [
    "daily_sales_summary",
    "product_performance",
    "category_performance",
    "customer_360",
    "inventory_health",
]


@dataclass(frozen=True)
class PostgresLoadResult:
    """Summary of one Gold-to-PostgreSQL load."""

    table_name: str
    rows_loaded: int


def initialize_serving_schema(engine: Engine) -> None:
    """Create serving tables and indexes if they do not exist."""

    settings = get_settings()
    sql_file = settings.project_root / "database" / "create_tables.sql"
    execute_sql_file(engine, sql_file)


def load_gold_table_to_postgres(
    engine: Engine,
    spark,
    table_name: str,
    gold_root: Path,
    chunk_size: int = 1_000,
) -> PostgresLoadResult:
    """Replace one PostgreSQL serving table with the current Gold Delta snapshot."""

    table_model = get_gold_table_model(table_name)
    gold_path = gold_root / table_name
    if not gold_path.exists():
        raise FileNotFoundError(f"Gold table is missing: {gold_path}")

    logger.info("Reading Gold table %s from %s", table_name, gold_path)
    spark_df = spark.read.format("delta").load(str(gold_path)).select(*table_model.columns)
    pandas_df = pd.DataFrame([row.asDict() for row in spark_df.collect()])
    rows_loaded = len(pandas_df)

    with engine.begin() as connection:
        connection.execute(text(f"DELETE FROM {table_model.table_name}"))
        if rows_loaded > 0:
            pandas_df.to_sql(
                table_model.table_name,
                con=connection,
                if_exists="append",
                index=False,
                dtype=table_model.dtype,
                chunksize=chunk_size,
                method="multi",
            )

    logger.info("Loaded %s rows into PostgreSQL table %s", rows_loaded, table_name)
    return PostgresLoadResult(table_name=table_name, rows_loaded=rows_loaded)


def load_selected_gold_tables(
    table_names: list[str] | None = None,
    create_tables: bool = True,
) -> list[PostgresLoadResult]:
    """Load selected Gold Delta tables into PostgreSQL."""

    settings = get_settings()
    selected = table_names or BATCH_GOLD_TABLES
    invalid_names = set(selected).difference(GOLD_TABLE_MODELS)
    if invalid_names:
        raise ValueError(f"Unknown Gold table names: {', '.join(sorted(invalid_names))}")

    engine = get_engine_with_retry(settings=settings)
    if create_tables:
        initialize_serving_schema(engine)

    gold_root = settings.resolve_path(settings.warehouse_dir) / "gold"
    try:
        with spark_session(app_name="load-gold-to-postgres") as spark:
            return [
                load_gold_table_to_postgres(
                    engine=engine,
                    spark=spark,
                    table_name=table_name,
                    gold_root=gold_root,
                )
                for table_name in selected
            ]
    finally:
        engine.dispose()


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for Gold-to-PostgreSQL loading."""

    parser = argparse.ArgumentParser(description="Load Gold Delta tables to PostgreSQL.")
    parser.add_argument(
        "--tables",
        nargs="*",
        choices=sorted(GOLD_TABLE_MODELS),
        help="Optional list of Gold tables to load. Defaults to all.",
    )
    parser.add_argument(
        "--skip-create-tables",
        action="store_true",
        help="Skip CREATE TABLE/INDEX statements before loading.",
    )
    return parser.parse_args()


def main() -> None:
    """Run the serving-layer load from the command line."""

    args = parse_args()
    configure_logging()
    try:
        results = load_selected_gold_tables(
            table_names=args.tables,
            create_tables=not args.skip_create_tables,
        )
    except ConnectionError as exc:
        logger.error(
            "%s Check POSTGRES_HOST, POSTGRES_PORT, credentials, and whether PostgreSQL is running.",
            exc,
        )
        raise SystemExit(1) from exc
    for result in results:
        logger.info("Result: %s", result)


if __name__ == "__main__":
    main()
