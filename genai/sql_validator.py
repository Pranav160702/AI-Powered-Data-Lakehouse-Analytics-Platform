"""Deterministic SQL safety validation for generated analytics queries."""

from __future__ import annotations

import re
from dataclasses import dataclass

from genai.schema_context import APPROVED_TABLES

BLOCKED_KEYWORDS = {
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "truncate",
    "create",
    "grant",
    "revoke",
    "copy",
    "call",
    "execute",
    "do",
}

TABLE_PATTERN = re.compile(r"\b(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_\.]*)", re.IGNORECASE)


@dataclass(frozen=True)
class ValidationResult:
    """SQL validation result."""

    is_valid: bool
    sql: str
    error: str | None = None


def _strip_sql(sql: str) -> str:
    return sql.strip().rstrip(";").strip()


def _has_multiple_statements(sql: str) -> bool:
    return ";" in sql.strip().rstrip(";")


def _referenced_tables(sql: str) -> set[str]:
    tables = set()
    for match in TABLE_PATTERN.finditer(sql):
        table = match.group(1).split(".")[-1].lower()
        tables.add(table)
    return tables


def ensure_default_limit(sql: str, default_limit: int) -> str:
    """Append a default LIMIT if the query does not already include one."""

    if re.search(r"\blimit\s+\d+\b", sql, flags=re.IGNORECASE):
        return sql
    return f"{sql} LIMIT {default_limit}"


def validate_sql(sql: str, default_limit: int = 100) -> ValidationResult:
    """Validate generated SQL and return a safe query when valid."""

    cleaned = _strip_sql(sql)
    lowered = cleaned.lower()
    if not cleaned:
        return ValidationResult(False, cleaned, "SQL is empty.")
    if _has_multiple_statements(sql):
        return ValidationResult(False, cleaned, "Multiple SQL statements are not allowed.")
    if not lowered.startswith("select"):
        return ValidationResult(False, cleaned, "Only SELECT queries are allowed.")

    tokens = set(re.findall(r"\b[a-z_]+\b", lowered))
    blocked = sorted(tokens.intersection(BLOCKED_KEYWORDS))
    if blocked:
        return ValidationResult(False, cleaned, f"Blocked SQL keyword used: {blocked[0]}.")

    tables = _referenced_tables(cleaned)
    if not tables:
        return ValidationResult(False, cleaned, "Query must reference at least one approved table.")

    unknown = sorted(tables.difference(APPROVED_TABLES))
    if unknown:
        return ValidationResult(False, cleaned, f"Table is not approved: {unknown[0]}.")

    return ValidationResult(True, ensure_default_limit(cleaned, default_limit))
