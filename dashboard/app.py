"""Streamlit batch analytics dashboard."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st
from httpx import HTTPError
from sqlalchemy.exc import SQLAlchemyError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from analytics.api_client import load_dashboard_data_from_api
from analytics.kpi_service import create_dashboard_engine, load_dashboard_data
from config.logging_config import configure_logging
from config.settings import get_settings
from dashboard.components.sidebar import render_sidebar
from dashboard.views import (
    ai_assistant,
    customer_analytics,
    forecasting,
    inventory_analytics,
    overview,
    product_analytics,
    realtime_monitoring,
    sales_analytics,
)

PAGES = {
    "Overview": overview.render,
    "Sales": sales_analytics.render,
    "Products": product_analytics.render,
    "Customers": customer_analytics.render,
    "Inventory": inventory_analytics.render,
    "Real-Time": realtime_monitoring.render,
    "Forecasting": forecasting.render,
    "AI Assistant": ai_assistant.render,
}


@st.cache_resource(show_spinner=False)
def get_cached_engine():
    """Return a cached PostgreSQL engine."""

    return create_dashboard_engine()


@st.cache_data(ttl=300, show_spinner=False)
def get_cached_dashboard_data(limit: int):
    """Return cached dashboard data from API or PostgreSQL."""

    settings = get_settings()
    if settings.analytics_api_url:
        return load_dashboard_data_from_api(settings.analytics_api_url, limit=limit)

    engine = get_cached_engine()
    return load_dashboard_data(engine, limit=limit)


def main() -> None:
    """Render the Streamlit application."""

    configure_logging()
    st.set_page_config(
        page_title="Lakehouse Analytics",
        page_icon=":bar_chart:",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.title("E-Commerce Lakehouse Analytics")

    page_name, limit = render_sidebar(list(PAGES))

    try:
        data = get_cached_dashboard_data(limit)
    except (ConnectionError, SQLAlchemyError, HTTPError) as exc:
        st.error(
            "Analytics serving layer is not ready. Check PostgreSQL/API settings, "
            "start services, and load Gold tables into PostgreSQL."
        )
        st.caption(str(exc))
        return

    PAGES[page_name](data)


if __name__ == "__main__":
    main()
