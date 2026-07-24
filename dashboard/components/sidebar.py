"""Reusable Streamlit sidebar controls."""

from __future__ import annotations

import streamlit as st


def render_sidebar(page_names: list[str]) -> tuple[str, int]:
    """Render dashboard navigation and shared controls."""

    with st.sidebar:
        page_name = st.radio("Page", page_names, label_visibility="collapsed")
        limit = st.slider("Rows", min_value=10, max_value=100, value=25, step=5)
        if st.button("Refresh", use_container_width=True):
            st.cache_data.clear()
    return page_name, limit
