"""The streaming job's ingest path: parse, clean, enrich and de-duplicate across micro-batches."""

import json
import os
import time
from datetime import datetime, timedelta, timezone

from src.streaming import streaming_job
from src.streaming.streaming_job import (
    MESSAGE_SCHEMA,
    LateRecordsListener,
    clean_events,
    load_long_term_bvalues,
    load_long_term_mc,
    merge_time_ranges,
    prepare_stream,
    rows_dropped_by_watermark,
)

T = datetime(2026, 11, 9, 1, 0, tzinfo=timezone.utc)


def record(event_id, event_time, lon=-122.35, lat=37.62, **extra):
    base = {"event_id": event_id, "event_time": event_time, "magnitude": 2.4, "longitude": lon, "latitude": lat,
            "event_type": "earthquake", "source": "usgs-live"}
    base.update(extra)
    return base


class Topic:
    """Kafka messages as the job sees them: message text, partition, offset and arrival time."""

    def __init__(self):
        self.offset = 0

    def message(self, value, arrived):
        text = value if isinstance(value, str) else json.dumps(value)
        self.offset += 1
        return {"value": text, "kafka_partition": 0, "kafka_offset": self.offset,
                "kafka_ts": arrived.isoformat().replace("+00:00", "Z")}


def write_batch(folder, name, messages):
    """One file = one micro-batch (maxFilesPerTrigger=1); modification times keep the file order."""
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / name
    path.write_text("\n".join(json.dumps(m) for m in messages) + "\n", encoding="utf-8")
    stamp = time.time() - 600 + 10 * len(list(folder.iterdir()))
    os.utime(path, (stamp, stamp))


def static_messages(spark, topic, values, arrived=T):
    rows = [topic.message(v, arrived) for v in values]
    return spark.createDataFrame(
        [(r["value"], r["kafka_partition"], r["kafka_offset"], arrived) for r in rows], MESSAGE_SCHEMA
    )


def test_cleaning_rules_give_every_rejected_record_a_reason(spark):
    values = [
        record("ok", "2026-11-09T01:00:00+00:00"),
        record("text-mag", "2026-11-09T01:00:00+00:00", magnitude="3.0", depth_km=" 7.5 ", tsunami="0"),
        record("no-mag", "2026-11-09T01:00:00+00:00", magnitude=None),
        {**record("x", "2026-11-09T01:00:00+00:00"), "event_id": None},
        record("  ", "2026-11-09T01:00:00+00:00"),
        record("no-time", None),
        record("bad-time", "yesterday at noon"),
        record("no-lat", "2026-11-09T01:00:00+00:00", lat=None),
        record("lon200", "2026-11-09T01:00:00+00:00", lon=-200.0),
        record("lat95", "2026-11-09T01:00:00+00:00", lat=95.0),
        record("word-mag", "2026-11-09T01:00:00+00:00", magnitude="strong"),
        record("nan-mag", "2026-11-09T01:00:00+00:00", magnitude="NaN"),
        record("text-lon", "2026-11-09T01:00:00+00:00", lon="west"),
        record("bool-depth", "2026-11-09T01:00:00+00:00", depth_km=True),
        '{"event_id": "broken", "event_time": ',
        "not json at all",
        "[1, 2, 3]",
        "",
    ]
    rows = clean_events(static_messages(spark, Topic(), values)).collect()
    by_offset = {r["kafka_offset"]: r for r in rows}
    reasons = [by_offset[i + 1]["reject_reason"] for i in range(len(values))]
    assert reasons == [
        None,
        None,
        None,
        "missing event_id",
        "missing event_id",
        "missing event_time",
        "unparseable event_time",
        "missing longitude or latitude",
        "longitude out of range",
        "latitude out of range",
        "magnitude is not a number",
        "magnitude is not a number",
        "longitude is not a number",
        "depth_km is not a number",
        "not a JSON object",
        "not a JSON object",
        "not a JSON object",
        "not a JSON object",
    ]
    # A number sent as text is a number: before, the double schema turned "3.0" into NULL silently.
    text_mag = by_offset[2]
    assert (text_mag["magnitude"], text_mag["depth_km"], text_mag["tsunami"]) == (3.0, 7.5, 0)
    assert by_offset[3]["magnitude"] is None  # JSON null stays a missing magnitude, not an error
    assert by_offset[1]["cell_id"] == "-123:37" and by_offset[1]["region_id"] == "R-130_+30"
    # Rejected messages are keyed by their place in the topic, so they never hide each other.
    assert by_offset[15]["dedup_key"] == "rejected:0:15" and by_offset[16]["dedup_key"] == "rejected:0:16"


def test_prepare_stream_deduplicates_by_arrival_time_and_keeps_old_events(spark, tmp_path):
    incoming = tmp_path / "incoming"
    topic = Topic()
    write_batch(incoming, "1.json", [
        topic.message(record("a", "2026-11-09T01:00:00+00:00"), T),
        topic.message(record("a", "2026-11-09T01:00:00+00:00"), T),  # repeated inside one micro-batch
        topic.message(record("b", "2026-11-09T02:00:00+00:00", lon=140.6, lat=35.2, source=None), T),
        topic.message(record("bad-lon", "2026-11-09T02:00:00+00:00", lon=200.0), T),
        topic.message(record("no-time", None), T),
        topic.message("not json at all", T),
    ])
    later = T + timedelta(hours=1)
    write_batch(incoming, "2.json", [
        # A USGS revision of "a" (same id, new magnitude): a duplicate.
        topic.message(record("a", "2026-11-09T01:00:05+00:00", magnitude=2.6), later),
        topic.message(record("c", "2026-11-09T03:00:00+00:00", lon=-72.5, lat=-35.1), later),
        # Event times far behind the newest event (10 days; the replay without rebasing, May 2026).
        # The old event-time watermark dropped both without a trace (QA round 2).
        topic.message(record("old-10d", "2026-10-30T03:00:00+00:00"), later),
        topic.message(record("may-replay", "2026-05-17T09:12:30+00:00", source="replay"), later),
        topic.message("not json at all", later),  # the same bad text again: rejected again
    ])
    # A message whose Kafka timestamp is 8 days older than the newest one already processed.
    write_batch(incoming, "3.json", [topic.message(record("late-arrival", "2026-11-09T04:00:00+00:00"), T - timedelta(days=8))])

    listener = LateRecordsListener()
    spark.streams.addListener(listener)
    raw = spark.readStream.schema(MESSAGE_SCHEMA).option("maxFilesPerTrigger", 1).json(str(incoming))
    query = (
        prepare_stream(raw)
        .writeStream.format("memory")
        .queryName("prepared_events")
        .outputMode("append")
        .option("checkpointLocation", str(tmp_path / "checkpoint"))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination(180)
    progress = [p for p in query.recentProgress if p["numInputRows"] > 0]
    assert len(progress) == 3  # three micro-batches

    rows = spark.sql("SELECT * FROM prepared_events").collect()
    valid = {r["event_id"]: r for r in rows if r["reject_reason"] is None}
    assert sorted(valid) == ["a", "b", "c", "may-replay", "old-10d"]
    assert valid["a"]["magnitude"] == 2.4  # the later duplicate did not replace or repeat the first
    assert (valid["b"]["grid_lon"], valid["b"]["grid_lat"], valid["b"]["cell_id"]) == (140, 35, "140:35")
    assert valid["b"]["region_id"] == "R+140_+30" and valid["b"]["source"] == "unknown"
    assert valid["c"]["region_id"] == "R-080_-40"
    assert valid["c"]["event_ts"] is not None and valid["c"]["ingestion_ts"] is not None
    assert valid["old-10d"]["event_ts"] == datetime(2026, 10, 30, 3, 0)
    rejected = sorted((r["kafka_offset"], r["reject_reason"]) for r in rows if r["reject_reason"] is not None)
    assert rejected == [
        (4, "longitude out of range"), (5, "missing event_time"), (6, "not a JSON object"), (11, "not a JSON object"),
    ]

    # The late arrival is the only record Spark dropped, and the listener said so.
    assert [rows_dropped_by_watermark(p) for p in progress] == [0, 0, 1]
    deadline = time.time() + 30
    while not listener.warnings and time.time() < deadline:
        time.sleep(0.5)
    spark.streams.removeListener(listener)
    assert len(listener.warnings) == 1 and "dropped 1 record(s)" in listener.warnings[0]


def test_rows_dropped_by_watermark_reads_every_state_operator():
    assert rows_dropped_by_watermark({}) == 0
    assert rows_dropped_by_watermark({"stateOperators": [{"numRowsDroppedByWatermark": 2}, {}]}) == 2


def test_read_back_ranges_cover_only_the_hours_around_the_batch_events():
    pad = timedelta(hours=24)
    times = [T, T + timedelta(hours=5), datetime(2026, 5, 17, 9, tzinfo=timezone.utc), T - timedelta(days=10)]
    assert merge_time_ranges(times, pad) == [
        (datetime(2026, 5, 16, 9, tzinfo=timezone.utc), datetime(2026, 5, 18, 9, tzinfo=timezone.utc)),
        (T - timedelta(days=11), T - timedelta(days=9)),
        (T - pad, T + timedelta(hours=29)),
    ]
    assert merge_time_ranges([], pad) == []


def test_regional_baselines_are_read_by_column(tmp_path):
    csv = tmp_path / "region_bvalues.csv"
    csv.write_text("region_id,long_term_b,event_count,completeness_mag\nR-160_+50,0.7896,1232,1.7\nR+000_+00,x,1,2.0\n")
    assert load_long_term_bvalues(str(csv)) == {"R-160_+50": 0.7896}
    assert load_long_term_mc(str(csv)) == {"R-160_+50": 1.7, "R+000_+00": 2.0}
    assert load_long_term_mc(str(tmp_path / "missing.csv")) == {}
    assert streaming_job.load_region_values(str(csv), "no_such_column") == {}
