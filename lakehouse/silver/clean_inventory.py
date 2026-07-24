"""Silver cleaning entry point for inventory and required product references."""

from lakehouse.silver.silver_pipeline import main


if __name__ == "__main__":
    main(default_tables=["categories", "products", "inventory"])
