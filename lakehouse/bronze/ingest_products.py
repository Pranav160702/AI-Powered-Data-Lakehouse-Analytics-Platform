"""Bronze ingestion entry point for products."""

from ingestion.batch_ingestion import main


if __name__ == "__main__":
    main(default_tables=["products"])
