"""Load real-time Gold metrics into PostgreSQL serving table."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.logging_config import get_logger
from database.load_gold_to_postgres import load_selected_gold_tables

logger = get_logger(__name__)


def main() -> None:
    """Load only the real-time metrics table."""

    try:
        load_selected_gold_tables(table_names=["realtime_metrics"], create_tables=True)
    except ConnectionError as exc:
        logger.error(
            "%s Check POSTGRES_HOST, POSTGRES_PORT, credentials, and whether PostgreSQL is running.",
            exc,
        )
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
