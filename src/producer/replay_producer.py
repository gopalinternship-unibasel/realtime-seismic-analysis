from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from src.common.kafka_io import make_kafka_producer, publish_events

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "seismic-events")
REPLAY_FILE = Path(os.getenv("REPLAY_FILE", "/opt/workspace/data/replay_catalog/sample_replay_events.jsonl"))
EVENTS_PER_BATCH = int(os.getenv("EVENTS_PER_BATCH", os.getenv("EVENTS_PER_FILE", "25")))
REPLAY_SLEEP_SECONDS = float(os.getenv("REPLAY_SLEEP_SECONDS", "3"))
LOOP_REPLAY = os.getenv("LOOP_REPLAY", "false").lower() == "true"
# Shift the catalogue in time so that its last event happens "now". The demo then behaves
# like live data: windows, the 30-day active-mainshock cutoff and the dashboard time filters
# all work on the replayed events.
REPLAY_REBASE_TO_NOW = os.getenv("REPLAY_REBASE_TO_NOW", "true").lower() == "true"
REPLAY_SOURCE = "replay"


def read_jsonl(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def prepare_records(records: list[dict], now: datetime | None = None, rebase: bool = True) -> list[dict]:
    """Tag records as replay data and (optionally) move the catalogue so it ends at ``now``.

    Relative spacing between events is kept, so the mainshock-aftershock sequence is unchanged.
    """
    now = now or datetime.now(tz=timezone.utc)
    shift = None
    if rebase and records:
        latest = max(parse_time(r["event_time"]) for r in records)
        shift = now - latest
    prepared = []
    for record in records:
        out = dict(record)
        if shift is not None:
            out["event_time"] = (parse_time(record["event_time"]) + shift).isoformat()
        out["source"] = REPLAY_SOURCE
        prepared.append(out)
    return sorted(prepared, key=lambda r: parse_time(r["event_time"]))


def replay_once(producer, records: list[dict]) -> int:
    batches = 0
    for start in range(0, len(records), EVENTS_PER_BATCH):
        chunk = records[start : start + EVENTS_PER_BATCH]
        ingestion_time = datetime.now(tz=timezone.utc).isoformat()
        published = publish_events(producer, KAFKA_TOPIC, [{**r, "ingestion_time": ingestion_time} for r in chunk])
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

    raw_records = read_jsonl(REPLAY_FILE)
    producer = make_kafka_producer(KAFKA_BOOTSTRAP_SERVERS)
    print(
        f"Starting Kafka replay producer with {len(raw_records)} records from {REPLAY_FILE}; "
        f"topic={KAFKA_TOPIC}, bootstrap={KAFKA_BOOTSTRAP_SERVERS}, rebase_to_now={REPLAY_REBASE_TO_NOW}",
        flush=True,
    )

    total_batches = 0
    try:
        while True:
            records = prepare_records(raw_records, rebase=REPLAY_REBASE_TO_NOW)
            print(f"Replay timeline: {records[0]['event_time']} .. {records[-1]['event_time']}", flush=True)
            total_batches += replay_once(producer, records)
            if not LOOP_REPLAY:
                break
    finally:
        producer.close(timeout=30)

    print(f"Replay complete. Kafka batches published: {total_batches}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
