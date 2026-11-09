from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Iterable

import requests

from src.common.geo import flatten_usgs_feature
from src.common.kafka_io import make_kafka_producer, publish_events

USGS_URL = os.getenv(
    "USGS_URL", "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "seismic-events")
STATE_FILE = Path(os.getenv("PRODUCER_STATE_FILE", "/opt/workspace/data/producer_seen_ids.json"))
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))


def load_seen_ids() -> set[str]:
    if not STATE_FILE.exists():
        return set()
    try:
        return set(json.loads(STATE_FILE.read_text()))
    except (json.JSONDecodeError, OSError):
        return set()


def save_seen_ids(seen_ids: Iterable[str]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(sorted(seen_ids)[-20000:], indent=2))


def fetch_events() -> list[dict]:
    response = requests.get(USGS_URL, timeout=30)
    response.raise_for_status()
    feed = response.json()
    records: list[dict] = []
    for feature in feed.get("features", []):
        record = flatten_usgs_feature(feature)
        if record is not None:
            records.append(record)
    return records


def main() -> int:
    seen_ids = load_seen_ids()
    producer = make_kafka_producer(KAFKA_BOOTSTRAP_SERVERS)
    print(
        f"Starting USGS live Kafka producer: url={USGS_URL}, poll={POLL_SECONDS}s, "
        f"topic={KAFKA_TOPIC}, bootstrap={KAFKA_BOOTSTRAP_SERVERS}",
        flush=True,
    )

    try:
        while True:
            try:
                records = fetch_events()
                new_records = [r for r in records if r["event_id"] not in seen_ids]
                published = publish_events(producer, KAFKA_TOPIC, new_records)
                for record in new_records:
                    seen_ids.add(record["event_id"])
                save_seen_ids(seen_ids)
                print(
                    f"Fetched={len(records)} new={len(new_records)} published_to_kafka={published}",
                    flush=True,
                )
            except Exception as exc:  # noqa: BLE001 - keep the long-running producer alive.
                print(f"Producer error: {exc}", flush=True)
            time.sleep(POLL_SECONDS)
    finally:
        producer.close(timeout=30)


if __name__ == "__main__":
    raise SystemExit(main())
