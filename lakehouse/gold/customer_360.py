"""Gold entry point for customer 360."""

from lakehouse.gold.gold_pipeline import main


if __name__ == "__main__":
    main(default_tables=["customer_360"])
