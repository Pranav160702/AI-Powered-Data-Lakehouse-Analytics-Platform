"""Reusable Plotly chart helpers for Streamlit pages."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st


def line_chart(df: pd.DataFrame, x: str, y: str, title: str) -> None:
    """Render a line chart or an empty state."""

    if df.empty:
        st.info("No data available.")
        return
    st.plotly_chart(px.line(df, x=x, y=y, markers=True, title=title), use_container_width=True)


def bar_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    title: str,
    color: str | None = None,
) -> None:
    """Render a bar chart or an empty state."""

    if df.empty:
        st.info("No data available.")
        return
    st.plotly_chart(px.bar(df, x=x, y=y, color=color, title=title), use_container_width=True)


def pie_chart(df: pd.DataFrame, names: str, values: str, title: str) -> None:
    """Render a pie chart or an empty state."""

    if df.empty:
        st.info("No data available.")
        return
    st.plotly_chart(px.pie(df, names=names, values=values, title=title), use_container_width=True)
