from __future__ import annotations

import json
import time
from collections.abc import Iterable
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable


def make_kafka_producer(bootstrap_servers: str, max_wait_seconds: int = 90) -> KafkaProducer:
    """Create a Kafka producer, retrying until the broker is ready.

    Docker Compose can start the producer container before Kafka has fully
    finished booting. Retrying here makes replay/live mode more reliable.
    """

    deadline = time.time() + max_wait_seconds
    last_error: Exception | None = None

    while time.time() < deadline:
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                key_serializer=lambda key: str(key).encode("utf-8"),
                value_serializer=lambda record: json.dumps(record, sort_keys=True).encode("utf-8"),
                acks="all",
                retries=10,
                linger_ms=50,
                request_timeout_ms=30000,
            )
        except NoBrokersAvailable as exc:
            last_error = exc
            print(f"Kafka is not ready yet at {bootstrap_servers}; retrying...", flush=True)
            time.sleep(3)

    raise RuntimeError(f"Could not connect to Kafka at {bootstrap_servers}") from last_error


def publish_events(producer: KafkaProducer, topic: str, records: Iterable[dict[str, Any]]) -> int:
    """Publish earthquake records to Kafka using event_id as the message key."""

    futures = []
    count = 0
    for record in records:
        event_id = record.get("event_id")
        if not event_id:
            continue
        futures.append(producer.send(topic, key=event_id, value=record))
        count += 1

    for future in futures:
        try:
            future.get(timeout=30)
        except KafkaError as exc:
            raise RuntimeError(f"Failed to publish event to Kafka topic {topic}") from exc

    producer.flush(timeout=30)
    return count
