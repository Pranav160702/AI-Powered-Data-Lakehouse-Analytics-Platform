"""Unit tests for LLM SQL generation parsing."""

from genai.sql_generator import generate_sql


class FakeLLM:
    """Simple fake LLM for deterministic tests."""

    def complete(self, _prompt: str) -> str:
        return '{"sql": "SELECT total_revenue FROM daily_sales_summary", "explanation": "Revenue query."}'


def test_generate_sql_parses_json_response() -> None:
    """Generated JSON should parse into SQL and explanation fields."""

    generated = generate_sql("What is revenue?", FakeLLM())

    assert generated.sql == "SELECT total_revenue FROM daily_sales_summary"
    assert generated.explanation == "Revenue query."
