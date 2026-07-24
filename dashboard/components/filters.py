"""Reusable Streamlit filter controls."""

from __future__ import annotations

import pandas as pd
import streamlit as st


def date_range_filter(df: pd.DataFrame, date_column: str) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    """Render a date range control for a DataFrame date column."""

    if df.empty or date_column not in df:
        return None, None
    dates = pd.to_datetime(df[date_column]).dropna()
    if dates.empty:
        return None, None
    start, end = st.date_input(
        "Date range",
        value=(dates.min().date(), dates.max().date()),
        min_value=dates.min().date(),
        max_value=dates.max().date(),
    )
    return pd.Timestamp(start), pd.Timestamp(end)


def apply_date_range(
    df: pd.DataFrame,
    date_column: str,
    start: pd.Timestamp | None,
    end: pd.Timestamp | None,
) -> pd.DataFrame:
    """Filter a DataFrame by an optional inclusive date range."""

    if df.empty or start is None or end is None:
        return df
    dates = pd.to_datetime(df[date_column])
    return df[(dates >= start) & (dates <= end)]
