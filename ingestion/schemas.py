"""Explicit raw source schemas for batch ingestion."""

from dataclasses import dataclass
from pathlib import Path

from pyspark.sql.types import StringType, StructField, StructType

CORRUPT_RECORD_COLUMN = "_corrupt_record"


@dataclass(frozen=True)
class SourceDefinition:
    """Description of a batch source file and target Bronze table."""

    table_name: str
    file_name: str
    columns: tuple[str, ...]
    source_system: str = "ecommerce_batch"

    @property
    def schema(self) -> StructType:
        """Return an all-string raw schema with a corrupt-record capture column."""

        fields = [StructField(column, StringType(), True) for column in self.columns]
        fields.append(StructField(CORRUPT_RECORD_COLUMN, StringType(), True))
        return StructType(fields)

    def source_path(self, raw_data_dir: Path) -> Path:
        """Return the expected source file path for this definition."""

        return raw_data_dir / self.file_name


SOURCE_DEFINITIONS: dict[str, SourceDefinition] = {
    "customers": SourceDefinition(
        table_name="customers",
        file_name="customers.csv",
        columns=(
            "customer_id",
            "customer_name",
            "email",
            "phone",
            "city",
            "state",
            "country",
            "registration_date",
            "customer_segment",
        ),
    ),
    "categories": SourceDefinition(
        table_name="categories",
        file_name="categories.csv",
        columns=("category_id", "category_name", "department"),
    ),
    "products": SourceDefinition(
        table_name="products",
        file_name="products.csv",
        columns=(
            "product_id",
            "product_name",
            "category_id",
            "brand",
            "price",
            "cost_price",
            "rating",
            "created_at",
        ),
    ),
    "orders": SourceDefinition(
        table_name="orders",
        file_name="orders.csv",
        columns=(
            "order_id",
            "customer_id",
            "order_date",
            "order_status",
            "shipping_city",
            "shipping_state",
            "payment_method",
            "order_total",
        ),
    ),
    "order_items": SourceDefinition(
        table_name="order_items",
        file_name="order_items.csv",
        columns=(
            "order_item_id",
            "order_id",
            "product_id",
            "quantity",
            "unit_price",
            "discount",
            "item_total",
        ),
    ),
    "payments": SourceDefinition(
        table_name="payments",
        file_name="payments.csv",
        columns=(
            "payment_id",
            "order_id",
            "payment_method",
            "payment_status",
            "payment_amount",
            "payment_timestamp",
        ),
    ),
    "inventory": SourceDefinition(
        table_name="inventory",
        file_name="inventory.csv",
        columns=(
            "product_id",
            "warehouse_id",
            "stock_quantity",
            "reorder_level",
            "last_updated",
        ),
    ),
}


def get_source_definition(table_name: str) -> SourceDefinition:
    """Return a source definition by table name."""

    try:
        return SOURCE_DEFINITIONS[table_name]
    except KeyError as exc:
        available = ", ".join(sorted(SOURCE_DEFINITIONS))
        raise ValueError(f"Unknown source table '{table_name}'. Available: {available}") from exc
