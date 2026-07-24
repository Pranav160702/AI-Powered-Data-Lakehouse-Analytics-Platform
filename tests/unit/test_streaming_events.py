"""Unit tests for Kafka event generation and schemas."""

from ingestion.event_generator import EventFactory, topic_for_event
from streaming.stream_processor import EVENT_SCHEMA


def test_event_factory_creates_supported_event_payload() -> None:
    """Generated events should match the expected Kafka payload shape."""

    event = EventFactory(
        customer_count=10,
        product_count=20,
        category_count=5,
        seed=7,
    ).create_event()

    assert event.event_id
    assert 1 <= event.customer_id <= 10
    assert 1 <= event.product_id <= 20
    assert 1 <= event.category_id <= 5
    assert event.quantity >= 1
    assert event.price > 0


def test_streaming_schema_contains_required_fields() -> None:
    """Spark streaming schema should include the required event fields."""

    assert set(EVENT_SCHEMA.fieldNames()) == {
        "event_id",
        "customer_id",
        "session_id",
        "event_type",
        "product_id",
        "category_id",
        "quantity",
        "price",
        "city",
        "device_type",
        "event_timestamp",
    }


def test_topic_routing_uses_order_and_payment_topics() -> None:
    """Business-critical event types should route to their dedicated topics."""

    assert topic_for_event("purchase_completed") == "order-events"
    assert topic_for_event("payment_failed") == "payment-events"
