"""Silver cleaning entry point for orders and required customer reference data."""

from lakehouse.silver.silver_pipeline import main


if __name__ == "__main__":
    main(default_tables=["customers", "orders"])
