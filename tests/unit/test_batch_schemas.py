"""Unit tests for explicit Phase 2 batch source schemas."""

from ingestion.schemas import CORRUPT_RECORD_COLUMN, SOURCE_DEFINITIONS


def test_all_batch_sources_include_corrupt_record_column() -> None:
    """Every raw schema should capture malformed CSV records."""

    for source in SOURCE_DEFINITIONS.values():
        assert source.schema.fieldNames()[-1] == CORRUPT_RECORD_COLUMN


def test_expected_source_files_are_configured() -> None:
    """The Phase 2 batch ingestion should cover all required CSV sources."""

    assert set(SOURCE_DEFINITIONS) == {
        "customers",
        "categories",
        "products",
        "orders",
        "order_items",
        "payments",
        "inventory",
    }
