"""Integration tests for Silver cleaning transformations."""

from datetime import datetime

from config.spark_config import spark_session
from lakehouse.silver.transformations import clean_customers


def test_clean_customers_deduplicates_valid_rows_and_quarantines_invalid() -> None:
    """Silver customers should keep latest valid IDs and quarantine bad business records."""

    rows = [
        (
            "1",
            "  alice  shah ",
            "ALICE@EXAMPLE.COM",
            "+917000000001",
            "pune",
            "maharashtra",
            "india",
            "2026-01-01",
            "premium",
            datetime(2026, 7, 23, 10, 0, 0),
            "batch-a",
        ),
        (
            "1",
            "Alice Shah",
            "alice.new@example.com",
            "+917000000001",
            "Pune",
            "Maharashtra",
            "India",
            "2026-01-02",
            "premium",
            datetime(2026, 7, 23, 11, 0, 0),
            "batch-b",
        ),
        (
            None,
            "Missing Id",
            "bad@example.com",
            "+917000000002",
            "Mumbai",
            "Maharashtra",
            "India",
            "2026-01-03",
            "regular",
            datetime(2026, 7, 23, 12, 0, 0),
            "batch-c",
        ),
    ]

    with spark_session(app_name="silver-customer-test") as spark:
        bronze_df = spark.createDataFrame(
            rows,
            [
                "customer_id",
                "customer_name",
                "email",
                "phone",
                "city",
                "state",
                "country",
                "registration_date",
                "customer_segment",
                "ingestion_timestamp",
                "batch_id",
            ],
        )

        valid_df, invalid_df = clean_customers(bronze_df)

        valid = valid_df.collect()
        invalid = invalid_df.collect()

    assert len(valid) == 1
    assert valid[0]["customer_id"] == 1
    assert valid[0]["email"] == "alice.new@example.com"
    assert valid[0]["city"] == "Pune"
    assert len(invalid) == 1
    assert "missing_customer_id" in invalid[0]["data_quality_flags"]
