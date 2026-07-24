"""Generate realistic e-commerce events and publish them to Kafka."""

from __future__ import annotations

import argparse
import json
import logging
import random
import signal
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.logging_config import configure_logging
from config.settings import get_settings

logger = logging.getLogger(__name__)

EVENT_TYPES = [
    "product_view",
    "product_search",
    "add_to_cart",
    "remove_from_cart",
    "checkout_started",
    "purchase_completed",
    "payment_failed",
]

EVENT_WEIGHTS = [0.46, 0.18, 0.16, 0.04, 0.07, 0.07, 0.02]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
CITIES = ["Mumbai", "Pune", "Bengaluru", "Hyderabad", "Chennai", "Delhi", "Jaipur"]


@dataclass(frozen=True)
class CustomerEvent:
    """Kafka event payload for simulated e-commerce activity."""

    event_id: str
    customer_id: int
    session_id: str
    event_type: str
    product_id: int
    category_id: int
    quantity: int
    price: float
    city: str
    device_type: str
    event_timestamp: str


class EventFactory:
    """Create correlated synthetic customer activity events."""

    def __init__(
        self,
        customer_count: int,
        product_count: int,
        category_count: int,
        seed: int,
    ) -> None:
        self.customer_count = customer_count
        self.product_count = product_count
        self.category_count = category_count
        self.random = random.Random(seed)
        self.sessions: dict[int, str] = {}

    def create_event(self) -> CustomerEvent:
        """Create one synthetic event."""

        customer_id = self.random.randint(1, self.customer_count)
        if customer_id not in self.sessions or self.random.random() < 0.08:
            self.sessions[customer_id] = str(uuid4())

        event_type = self.random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
        quantity = self.random.choices([1, 2, 3, 4], weights=[0.72, 0.18, 0.07, 0.03], k=1)[0]
        price = round(self.random.lognormvariate(mu=6.4, sigma=0.55), 2)
        return CustomerEvent(
            event_id=str(uuid4()),
            customer_id=customer_id,
            session_id=self.sessions[customer_id],
            event_type=event_type,
            product_id=self.random.randint(1, self.product_count),
            category_id=self.random.randint(1, self.category_count),
            quantity=quantity,
            price=price,
            city=self.random.choice(CITIES),
            device_type=self.random.choice(DEVICE_TYPES),
            event_timestamp=datetime.now(UTC).replace(microsecond=0).isoformat(),
        )


def topic_for_event(event_type: str) -> str:
    """Return the configured Kafka topic for an event type."""

    settings = get_settings()
    if event_type == "purchase_completed":
        return settings.kafka_order_events_topic
    if event_type == "payment_failed":
        return settings.kafka_payment_events_topic
    return settings.kafka_customer_events_topic


def parse_args() -> argparse.Namespace:
    """Parse command-line options for the producer."""

    parser = argparse.ArgumentParser(description="Simulate Kafka e-commerce events.")
    parser.add_argument("--rate", type=float, default=20.0, help="Events per second.")
    parser.add_argument("--customer-count", type=int, default=5_000)
    parser.add_argument("--product-count", type=int, default=1_000)
    parser.add_argument("--category-count", type=int, default=20)
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def main() -> None:
    """Run the Kafka producer until interrupted."""

    try:
        from kafka import KafkaProducer
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Kafka producer dependency is missing. Install requirements.txt; "
            "this project uses kafka-python-ng for Python 3.12 compatibility."
        ) from exc

    args = parse_args()
    configure_logging()
    settings = get_settings()
    interval = 1.0 / max(args.rate, 0.1)
    factory = EventFactory(
        customer_count=args.customer_count,
        product_count=args.product_count,
        category_count=args.category_count,
        seed=args.seed,
    )
    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: str(value).encode("utf-8"),
        acks="all",
        retries=3,
    )
    running = True

    def stop(_signum, _frame) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    logger.info(
        "Starting event producer at %.2f events/sec to %s",
        args.rate,
        settings.kafka_bootstrap_servers,
    )

    sent = 0
    while running:
        event = factory.create_event()
        producer.send(topic_for_event(event.event_type), key=event.event_id, value=asdict(event))
        sent += 1
        if sent % 100 == 0:
            producer.flush()
            logger.info("Published %s events", sent)
        time.sleep(interval)

    producer.flush()
    producer.close()
    logger.info("Producer stopped after publishing %s events", sent)


if __name__ == "__main__":
    main()
