"""Bronze ingestion entry point for customers."""

from ingestion.batch_ingestion import main


if __name__ == "__main__":
    main(default_tables=["customers"])
