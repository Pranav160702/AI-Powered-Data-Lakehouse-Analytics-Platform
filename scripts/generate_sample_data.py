"""Generate reproducible synthetic e-commerce source data for Phase 1."""

from __future__ import annotations

import argparse
import csv
import logging
import random
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from config.logging_config import configure_logging
    from config.settings import get_settings
except ModuleNotFoundError:
    configure_logging = None
    get_settings = None

logger = logging.getLogger(__name__)

CITIES: list[tuple[str, str]] = [
    ("Mumbai", "Maharashtra"),
    ("Pune", "Maharashtra"),
    ("Bengaluru", "Karnataka"),
    ("Hyderabad", "Telangana"),
    ("Chennai", "Tamil Nadu"),
    ("Delhi", "Delhi"),
    ("Ahmedabad", "Gujarat"),
    ("Jaipur", "Rajasthan"),
    ("Kolkata", "West Bengal"),
    ("Kochi", "Kerala"),
]
SEGMENTS = ["new", "regular", "premium", "at_risk"]
DEPARTMENTS = ["Electronics", "Fashion", "Home", "Beauty", "Sports"]
BRANDS = [
    "Aster",
    "NorthPeak",
    "UrbanCart",
    "Zenova",
    "BlueNest",
    "Swiftly",
    "CasaCore",
    "VividWear",
]
PAYMENT_METHODS = ["card", "upi", "wallet", "net_banking", "cod"]
ORDER_STATUSES = ["delivered", "shipped", "processing", "cancelled", "returned"]


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for dataset volume and destination."""

    parser = argparse.ArgumentParser(
        description="Generate synthetic e-commerce CSV files for the lakehouse."
    )
    parser.add_argument("--customers", type=int, default=5_000)
    parser.add_argument("--products", type=int, default=1_000)
    parser.add_argument("--categories", type=int, default=20)
    parser.add_argument("--orders", type=int, default=50_000)
    parser.add_argument("--min-items-per-order", type=int, default=1)
    parser.add_argument("--max-items-per-order", type=int, default=4)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Destination directory. Defaults to RAW_DATA_DIR from settings.",
    )
    return parser.parse_args()


def _random_date(start: date, end: date) -> date:
    days = (end - start).days
    return start + timedelta(days=random.randint(0, days))


Row = dict[str, object]


def generate_customers(count: int) -> list[Row]:
    """Create synthetic customers."""

    rows = []
    for customer_id in range(1, count + 1):
        city, state = random.choice(CITIES)
        name = f"Customer {customer_id:05d}"
        rows.append(
            {
                "customer_id": customer_id,
                "customer_name": name,
                "email": f"customer{customer_id:05d}@example.com",
                "phone": f"+91{random.randint(7000000000, 9999999999)}",
                "city": city,
                "state": state,
                "country": "India",
                "registration_date": _random_date(date(2022, 1, 1), date(2026, 7, 1)),
                "customer_segment": random.choices(
                    SEGMENTS, weights=[0.25, 0.45, 0.2, 0.1], k=1
                )[0],
            }
        )
    return rows


def generate_categories(count: int) -> list[Row]:
    """Create synthetic product categories."""

    rows = []
    for category_id in range(1, count + 1):
        department = DEPARTMENTS[(category_id - 1) % len(DEPARTMENTS)]
        rows.append(
            {
                "category_id": category_id,
                "category_name": f"{department} Category {category_id:02d}",
                "department": department,
            }
        )
    return rows


def generate_products(count: int, category_count: int) -> list[Row]:
    """Create synthetic products with prices, costs, and ratings."""

    rows = []
    for product_id in range(1, count + 1):
        price = round(random.lognormvariate(mu=6.4, sigma=0.55), 2)
        cost_ratio = random.uniform(0.48, 0.78)
        rows.append(
            {
                "product_id": product_id,
                "product_name": f"Product {product_id:05d}",
                "category_id": random.randint(1, category_count),
                "brand": random.choice(BRANDS),
                "price": price,
                "cost_price": round(price * cost_ratio, 2),
                "rating": round(random.uniform(3.1, 5.0), 2),
                "created_at": _random_date(date(2021, 1, 1), date(2026, 1, 1)),
            }
        )
    return rows


def generate_orders_and_items(
    order_count: int,
    customer_count: int,
    products: list[Row],
    min_items: int,
    max_items: int,
) -> tuple[list[Row], list[Row], list[Row]]:
    """Create orders, order items, and payments with matching totals."""

    product_lookup = {int(product["product_id"]): float(product["price"]) for product in products}
    order_rows = []
    item_rows = []
    payment_rows = []
    order_item_id = 1

    for order_id in range(1, order_count + 1):
        order_date = _random_date(date(2025, 1, 1), date(2026, 7, 20))
        status = random.choices(
            ORDER_STATUSES, weights=[0.72, 0.1, 0.08, 0.06, 0.04], k=1
        )[0]
        city, state = random.choice(CITIES)
        payment_method = random.choice(PAYMENT_METHODS)
        selected_products = random.sample(
            list(product_lookup.keys()), random.randint(min_items, max_items)
        )

        order_total = 0.0
        for product_id in selected_products:
            quantity = random.choices([1, 2, 3, 4], weights=[0.58, 0.25, 0.12, 0.05], k=1)[0]
            unit_price = float(product_lookup[product_id])
            discount = round(unit_price * quantity * random.choice([0, 0.03, 0.05, 0.1]), 2)
            item_total = round(unit_price * quantity - discount, 2)
            order_total += item_total
            item_rows.append(
                {
                    "order_item_id": order_item_id,
                    "order_id": order_id,
                    "product_id": product_id,
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "discount": discount,
                    "item_total": item_total,
                }
            )
            order_item_id += 1

        order_total = round(order_total, 2)
        payment_status = "failed" if status == "cancelled" and random.random() < 0.35 else "paid"
        if status == "returned":
            payment_status = "refunded"

        order_rows.append(
            {
                "order_id": order_id,
                "customer_id": random.randint(1, customer_count),
                "order_date": order_date,
                "order_status": status,
                "shipping_city": city,
                "shipping_state": state,
                "payment_method": payment_method,
                "order_total": order_total,
            }
        )
        payment_rows.append(
            {
                "payment_id": order_id,
                "order_id": order_id,
                "payment_method": payment_method,
                "payment_status": payment_status,
                "payment_amount": 0.0 if payment_status == "failed" else order_total,
                "payment_timestamp": datetime.combine(
                    order_date, datetime.min.time()
                )
                + timedelta(minutes=random.randint(1, 120)),
            }
        )

    return order_rows, item_rows, payment_rows


def generate_inventory(products: list[Row]) -> list[Row]:
    """Create inventory records for all products."""

    rows = []
    for product in products:
        product_id = int(product["product_id"])
        reorder_level = random.randint(15, 80)
        stock_quantity = random.randint(0, 500)
        rows.append(
            {
                "product_id": product_id,
                "warehouse_id": f"WH-{random.randint(1, 8):02d}",
                "stock_quantity": stock_quantity,
                "reorder_level": reorder_level,
                "last_updated": datetime.now().replace(microsecond=0),
            }
        )
    return rows


def write_csv(rows: list[Row], output_dir: Path, file_name: str) -> None:
    """Write rows to CSV and log the resulting file."""

    path = output_dir / file_name
    if not rows:
        raise ValueError(f"No rows generated for {file_name}")
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    logger.info("Wrote %s rows to %s", len(rows), path)


def main() -> None:
    """Generate all Phase 1 source datasets."""

    args = parse_args()
    if configure_logging is not None:
        configure_logging()
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        )
    random.seed(args.seed)

    if args.output_dir is not None:
        output_dir = args.output_dir
    elif get_settings is not None:
        settings = get_settings()
        output_dir = settings.resolve_path(settings.raw_data_dir)
    else:
        output_dir = PROJECT_ROOT / "data" / "raw"
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.min_items_per_order < 1 or args.max_items_per_order < args.min_items_per_order:
        raise ValueError("Item count bounds must be positive and ordered correctly.")

    logger.info("Generating synthetic data with seed=%s", args.seed)
    customers = generate_customers(args.customers)
    categories = generate_categories(args.categories)
    products = generate_products(args.products, args.categories)
    orders, order_items, payments = generate_orders_and_items(
        args.orders,
        args.customers,
        products,
        args.min_items_per_order,
        args.max_items_per_order,
    )
    inventory = generate_inventory(products)

    write_csv(customers, output_dir, "customers.csv")
    write_csv(categories, output_dir, "categories.csv")
    write_csv(products, output_dir, "products.csv")
    write_csv(orders, output_dir, "orders.csv")
    write_csv(order_items, output_dir, "order_items.csv")
    write_csv(payments, output_dir, "payments.csv")
    write_csv(inventory, output_dir, "inventory.csv")
    logger.info("Synthetic data generation completed: %s", output_dir)


if __name__ == "__main__":
    main()
