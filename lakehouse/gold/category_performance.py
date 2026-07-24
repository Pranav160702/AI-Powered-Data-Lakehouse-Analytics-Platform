"""Gold entry point for category performance."""

from lakehouse.gold.gold_pipeline import main


if __name__ == "__main__":
    main(default_tables=["category_performance"])
