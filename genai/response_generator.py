"""Natural-language response formatting for analytics results."""

from __future__ import annotations

import pandas as pd


def summarize_results(question: str, df: pd.DataFrame, explanation: str) -> str:
    """Return a concise human-readable explanation for query results."""

    if df.empty:
        return f"No rows matched the question: {question}"
    columns = ", ".join(df.columns[:5])
    return (
        f"{explanation or 'Here are the matching analytics results.'} "
        f"The query returned {len(df)} rows with columns: {columns}."
    )
