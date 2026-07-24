"""Shared logging setup for pipelines and local utilities."""

import logging
import sys
from typing import Final

from config.settings import get_settings

LOG_FORMAT: Final[str] = (
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)


def configure_logging(level: str | None = None) -> None:
    """Configure root logging once with a consistent console format."""

    settings = get_settings()
    resolved_level = (level or settings.log_level).upper()
    root_logger = logging.getLogger()
    root_logger.setLevel(resolved_level)

    if not root_logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(LOG_FORMAT))
        root_logger.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    """Return a named logger after ensuring shared logging is configured."""

    configure_logging()
    return logging.getLogger(name)
