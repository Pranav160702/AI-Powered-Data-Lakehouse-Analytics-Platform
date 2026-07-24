"""Command-line interface for the GenAI analytics assistant."""

from __future__ import annotations

import argparse
import logging

from config.logging_config import configure_logging
from genai.analytics_assistant import answer_question

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""

    parser = argparse.ArgumentParser(description="Ask a natural-language analytics question.")
    parser.add_argument("question", help="Business analytics question.")
    parser.add_argument("--limit", type=int, default=50)
    return parser.parse_args()


def main() -> None:
    """Run the assistant from the command line."""

    args = parse_args()
    configure_logging()
    result = answer_question(args.question, default_limit=args.limit)
    logger.info("SQL: %s", result.sql)
    logger.info("Answer: %s", result.answer)
    print(result.results.to_string(index=False))


if __name__ == "__main__":
    main()
