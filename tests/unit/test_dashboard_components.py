"""Unit tests for dashboard formatting helpers."""

from dashboard.app import PAGES
from dashboard.components.metric_cards import format_currency, format_number


def test_metric_formatters_handle_numeric_values() -> None:
    """Metric helpers should produce stable display strings."""

    assert format_currency(1234.56) == "Rs. 1,235"
    assert format_number(9876.5) == "9,876"


def test_metric_formatters_handle_missing_values() -> None:
    """Metric helpers should gracefully handle missing values."""

    assert format_currency(None) == "Rs. 0"
    assert format_number(None) == "0"


def test_dashboard_includes_realtime_page() -> None:
    """Phase 8 should expose the real-time monitoring page."""

    assert "Real-Time" in PAGES


def test_dashboard_includes_ai_assistant_page() -> None:
    """Phase 10 should expose the AI assistant page."""

    assert "AI Assistant" in PAGES
