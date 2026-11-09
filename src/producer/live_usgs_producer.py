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
# Fetched once at start-up so the sliding windows, b-values and Omori fits have the recent
# past to work with instead of starting from an empty database. Set to "" to disable.
USGS_BACKFILL_URL = os.getenv(
    "USGS_BACKFILL_URL", "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_week.geojson"
)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "seismic-events")
STATE_FILE = Path(os.getenv("PRODUCER_STATE_FILE", "/opt/workspace/state/producer_seen_ids.json"))
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))
LIVE_SOURCE = "usgs-live"
USER_AGENT = "seismic-spark-dis-workshop/1.0 (student project; polls the public USGS GeoJSON feed)"


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


def parse_feed(feed: dict) -> list[dict]:
    records: list[dict] = []
    for feature in feed.get("features", []):
        record = flatten_usgs_feature(feature, source=LIVE_SOURCE)
        if record is not None:
            records.append(record)
    return records


def fetch_events(url: str = USGS_URL) -> list[dict]:
    response = requests.get(url, timeout=60, headers={"User-Agent": USER_AGENT})
    response.raise_for_status()
    return parse_feed(response.json())


def publish_new(producer, records: list[dict], seen_ids: set[str]) -> tuple[int, int]:
    new_records = [r for r in records if r["event_id"] not in seen_ids]
    # Oldest first, so Spark sees the feed in event-time order.
    new_records.sort(key=lambda r: r["event_time"])
    published = publish_events(producer, KAFKA_TOPIC, new_records)
    for record in new_records:
        seen_ids.add(record["event_id"])
    save_seen_ids(seen_ids)
    return len(new_records), published


def main() -> int:
    seen_ids = load_seen_ids()
    producer = make_kafka_producer(KAFKA_BOOTSTRAP_SERVERS)
    print(
        f"Starting USGS live Kafka producer: url={USGS_URL}, poll={POLL_SECONDS}s, "
        f"topic={KAFKA_TOPIC}, bootstrap={KAFKA_BOOTSTRAP_SERVERS}, backfill={USGS_BACKFILL_URL or 'off'}",
        flush=True,
    )

    try:
        if USGS_BACKFILL_URL:
            try:
                records = fetch_events(USGS_BACKFILL_URL)
                new, published = publish_new(producer, records, seen_ids)
                print(f"Backfill fetched={len(records)} new={new} published_to_kafka={published}", flush=True)
            except Exception as exc:  # noqa: BLE001 - the live loop still works without the backfill.
                print(f"Backfill error: {exc}", flush=True)
        while True:
            try:
                records = fetch_events()
                new, published = publish_new(producer, records, seen_ids)
                print(f"Fetched={len(records)} new={new} published_to_kafka={published}", flush=True)
            except Exception as exc:  # noqa: BLE001 - keep the long-running producer alive.
                print(f"Producer error: {exc}", flush=True)
            time.sleep(POLL_SECONDS)
    finally:
        producer.close(timeout=30)


if __name__ == "__main__":
    raise SystemExit(main())
