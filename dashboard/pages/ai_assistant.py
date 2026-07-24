"""GenAI natural-language analytics assistant page."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st
from sqlalchemy.exc import SQLAlchemyError

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from genai.analytics_assistant import answer_question


def render(_data) -> None:
    """Render the AI analytics assistant."""

    st.caption("Ask business questions about approved Gold analytics tables.")
    question = st.text_area(
        "Question",
        value="Which products generate the highest revenue?",
        height=100,
    )
    default_limit = st.slider("Result limit", min_value=10, max_value=200, value=50, step=10)

    if not st.button("Ask", use_container_width=True):
        return

    try:
        result = answer_question(question, default_limit=default_limit)
    except ValueError as exc:
        st.error(str(exc))
        return
    except (ConnectionError, SQLAlchemyError) as exc:
        st.error("PostgreSQL is not ready or serving tables are not loaded.")
        st.caption(str(exc))
        return

    st.markdown(result.answer)
    st.code(result.sql, language="sql")
    st.dataframe(result.results, use_container_width=True, hide_index=True)
