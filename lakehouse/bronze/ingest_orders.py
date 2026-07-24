"""Bronze ingestion entry point for orders."""

from ingestion.batch_ingestion import main


if __name__ == "__main__":
    main(default_tables=["orders", "order_items", "payments"])
