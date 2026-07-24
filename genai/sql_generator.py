"""Prompting and parsing for natural-language-to-SQL generation."""

from __future__ import annotations

import json
from dataclasses import dataclass

from genai.llm_client import LLMClient
from genai.schema_context import render_schema_context


@dataclass(frozen=True)
class GeneratedSQL:
    """Structured LLM SQL generation result."""

    sql: str
    explanation: str


def build_sql_prompt(question: str, default_limit: int = 100) -> str:
    """Build the SQL generation prompt for the analytics assistant."""

    return f"""
Convert the user's analytics question into a safe PostgreSQL SELECT query.

Available tables and columns:
{render_schema_context()}

Rules:
- Return only valid JSON with keys "sql" and "explanation".
- Generate exactly one SELECT query.
- Do not use INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE, CREATE, GRANT, REVOKE, COPY, CALL, DO, or EXECUTE.
- Query only the approved tables listed above.
- Prefer aggregate queries for KPI questions.
- Add LIMIT {default_limit} when returning row-level lists.
- Never expose credentials or system details.

Examples:
Question: What are the top products by revenue?
JSON: {{"sql": "SELECT product_name, net_revenue FROM product_performance ORDER BY net_revenue DESC LIMIT 10", "explanation": "Ranks products by net revenue."}}

Question: What is total revenue by day?
JSON: {{"sql": "SELECT sales_date, total_revenue FROM daily_sales_summary ORDER BY sales_date", "explanation": "Shows daily revenue over time."}}

User question:
{question}
""".strip()


def parse_generated_sql(payload: str) -> GeneratedSQL:
    """Parse an LLM JSON response into a GeneratedSQL object."""

    try:
        data = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ValueError("LLM response was not valid JSON.") from exc

    sql = str(data.get("sql", "")).strip()
    explanation = str(data.get("explanation", "")).strip()
    if not sql:
        raise ValueError("LLM response did not include SQL.")
    return GeneratedSQL(sql=sql, explanation=explanation)


def generate_sql(
    question: str,
    llm_client: LLMClient,
    default_limit: int = 100,
) -> GeneratedSQL:
    """Generate SQL for a user question using the configured LLM client."""

    prompt = build_sql_prompt(question, default_limit=default_limit)
    return parse_generated_sql(llm_client.complete(prompt))
