"""Gold entry point for product performance."""

from lakehouse.gold.gold_pipeline import main


if __name__ == "__main__":
    main(default_tables=["product_performance"])
