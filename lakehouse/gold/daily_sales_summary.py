"""Gold entry point for daily sales summary."""

from lakehouse.gold.gold_pipeline import main


if __name__ == "__main__":
    main(default_tables=["daily_sales_summary"])
