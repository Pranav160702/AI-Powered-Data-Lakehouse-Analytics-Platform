"""Gold entry point for inventory health."""

from lakehouse.gold.gold_pipeline import main


if __name__ == "__main__":
    main(default_tables=["inventory_health"])
