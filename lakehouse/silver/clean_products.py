"""Silver cleaning entry point for products and required category reference data."""

from lakehouse.silver.silver_pipeline import main


if __name__ == "__main__":
    main(default_tables=["categories", "products"])
