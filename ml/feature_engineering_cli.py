"""Generate and persist ML feature data for inspection."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.logging_config import configure_logging
from config.settings import get_settings
from config.spark_config import spark_session
from ml.feature_engineering import load_features_from_silver

logger = logging.getLogger(__name__)


def main() -> None:
    """Build feature data and save it under models/."""

    configure_logging()
    settings = get_settings()
    output_path = settings.resolve_path(settings.model_dir) / "demand_forecast_features.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with spark_session(app_name="demand-feature-engineering") as spark:
        features = load_features_from_silver(spark)
    features.to_csv(output_path, index=False)
    logger.info("Saved %s feature rows to %s", len(features), output_path)


if __name__ == "__main__":
    main()
