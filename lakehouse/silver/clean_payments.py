"""Silver cleaning entry point for payments and required order references."""

from lakehouse.silver.silver_pipeline import main


if __name__ == "__main__":
    main(default_tables=["customers", "orders", "payments"])
