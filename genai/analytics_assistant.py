"""End-to-end GenAI analytics assistant flow."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd
from sqlalchemy.engine import Engine

from database.connection import get_engine_with_retry
from genai.llm_client import GroqLLMClient, LLMClient
from genai.query_executor import execute_read_only_query
from genai.response_generator import summarize_results
from genai.sql_generator import GeneratedSQL, generate_sql
from genai.sql_validator import validate_sql

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AssistantResult:
    """Analytics assistant response payload."""

    question: str
    sql: str
    explanation: str
    answer: str
    results: pd.DataFrame


def answer_question(
    question: str,
    llm_client: LLMClient | None = None,
    engine: Engine | None = None,
    default_limit: int = 100,
) -> AssistantResult:
    """Generate, validate, execute, and explain an analytics SQL query."""

    resolved_llm = llm_client or GroqLLMClient.from_settings()
    generated: GeneratedSQL = generate_sql(
        question,
        llm_client=resolved_llm,
        default_limit=default_limit,
    )
    validation = validate_sql(generated.sql, default_limit=default_limit)
    if not validation.is_valid:
        raise ValueError(validation.error or "Generated SQL failed validation.")

    logger.info("Validated generated analytics SQL: %s", validation.sql)
    resolved_engine = engine or get_engine_with_retry(retries=1)
    df = execute_read_only_query(resolved_engine, validation.sql)
    answer = summarize_results(question, df, generated.explanation)
    return AssistantResult(
        question=question,
        sql=validation.sql,
        explanation=generated.explanation,
        answer=answer,
        results=df,
    )
