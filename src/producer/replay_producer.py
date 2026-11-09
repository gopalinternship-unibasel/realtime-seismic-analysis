from __future__ import annotations

import json
import os
import time
from pathlib import Path

from src.common.kafka_io import make_kafka_producer, publish_events

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "seismic-events")
REPLAY_FILE = Path(os.getenv("REPLAY_FILE", "/opt/workspace/data/replay_catalog/sample_replay_events.jsonl"))
EVENTS_PER_BATCH = int(os.getenv("EVENTS_PER_BATCH", os.getenv("EVENTS_PER_FILE", "25")))
REPLAY_SLEEP_SECONDS = float(os.getenv("REPLAY_SLEEP_SECONDS", "3"))
LOOP_REPLAY = os.getenv("LOOP_REPLAY", "false").lower() == "true"


def read_jsonl(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def replay_once(producer, records: list[dict]) -> int:
    batches = 0
    for start in range(0, len(records), EVENTS_PER_BATCH):
        chunk = records[start : start + EVENTS_PER_BATCH]
        published = publish_events(producer, KAFKA_TOPIC, chunk)
        print(
            f"Replay published {published} events to Kafka topic '{KAFKA_TOPIC}' "
            f"via {KAFKA_BOOTSTRAP_SERVERS}",
            flush=True,
        )
        batches += 1
        time.sleep(REPLAY_SLEEP_SECONDS)
    return batches


def main() -> int:
    if not REPLAY_FILE.exists():
        raise FileNotFoundError(f"Replay file not found: {REPLAY_FILE}")

    records = read_jsonl(REPLAY_FILE)
    producer = make_kafka_producer(KAFKA_BOOTSTRAP_SERVERS)
    print(
        f"Starting Kafka replay producer with {len(records)} records from {REPLAY_FILE}; "
        f"topic={KAFKA_TOPIC}, bootstrap={KAFKA_BOOTSTRAP_SERVERS}",
        flush=True,
    )

    total_batches = 0
    try:
        while True:
            total_batches += replay_once(producer, records)
            if not LOOP_REPLAY:
                break
    finally:
        producer.close(timeout=30)

    print(f"Replay complete. Kafka batches published: {total_batches}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
