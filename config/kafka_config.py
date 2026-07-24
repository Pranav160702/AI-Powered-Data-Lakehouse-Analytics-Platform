"""Kafka topic configuration."""

from config.settings import get_settings


def event_topics() -> list[str]:
    """Return the Kafka topics used by the platform."""

    settings = get_settings()
    return [
        settings.kafka_customer_events_topic,
        settings.kafka_order_events_topic,
        settings.kafka_payment_events_topic,
        settings.kafka_inventory_events_topic,
    ]
