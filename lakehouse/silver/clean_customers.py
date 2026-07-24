"""Silver cleaning entry point for customers."""

from lakehouse.silver.silver_pipeline import main


if __name__ == "__main__":
    main(default_tables=["customers"])
