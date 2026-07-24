"""Reusable metric-card rendering for Streamlit."""

from __future__ import annotations

from typing import Any

import streamlit as st


def format_currency(value: Any) -> str:
    """Format a metric value as Indian Rupees using ASCII text."""

    try:
        return f"Rs. {float(value):,.0f}"
    except (TypeError, ValueError):
        return "Rs. 0"


def format_number(value: Any) -> str:
    """Format a numeric metric without decimals."""

    try:
        return f"{float(value):,.0f}"
    except (TypeError, ValueError):
        return "0"


def render_metric_row(metrics: list[tuple[str, str]]) -> None:
    """Render a row of Streamlit metric cards."""

    columns = st.columns(len(metrics))
    for column, (label, value) in zip(columns, metrics, strict=True):
        column.metric(label, value)
