"""Unit tests for GenAI SQL validation."""

from genai.sql_validator import validate_sql


def test_validate_sql_allows_approved_select_and_adds_limit() -> None:
    """Approved SELECT queries should pass and receive a default limit."""

    result = validate_sql("SELECT product_name FROM product_performance ORDER BY net_revenue DESC")

    assert result.is_valid
    assert result.sql.endswith("LIMIT 100")


def test_validate_sql_rejects_write_operations() -> None:
    """Unsafe SQL verbs should be rejected."""

    result = validate_sql("DROP TABLE customer_360")

    assert not result.is_valid
    assert "Only SELECT" in result.error


def test_validate_sql_rejects_unapproved_tables() -> None:
    """Queries may only reference approved Gold tables."""

    result = validate_sql("SELECT * FROM users")

    assert not result.is_valid
    assert "not approved" in result.error


def test_validate_sql_rejects_multiple_statements() -> None:
    """Multiple statements are not allowed."""

    result = validate_sql("SELECT * FROM customer_360; SELECT * FROM inventory_health")

    assert not result.is_valid
    assert "Multiple" in result.error
