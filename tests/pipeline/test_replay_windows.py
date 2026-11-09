"""The replay demo gives the same sliding windows on every start.

QA saw 34 b-value windows (b 0.653-1.027) and 61 anomaly windows on one fresh start, and 33
windows (b 0.672-1.028) and 63 anomaly windows on another: the replay was shifted by exactly
"now - last event", a different fraction of an hour each time, while the windows start on whole
UTC hours. The shift is now a whole number of hours.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.common.geo import coarse_region, grid_cell
from src.producer import replay_producer
from src.streaming.analytics import anomaly_windows, bvalue_rows, bvalue_window_magnitudes
from src.streaming.streaming_job import load_long_term_bvalues, load_long_term_mc

DATA = Path(__file__).resolve().parents[2] / "data"
REPLAY_FILE = DATA / "replay_catalog/sample_replay_events.jsonl"
BASELINE_CSV = DATA / "baseline/cell_baselines.csv"
REGION_CSV = DATA / "baseline/region_bvalues.csv"
SCHEMA = (
    "event_id string, event_ts timestamp, magnitude double, cell_id string, grid_lon int, grid_lat int, "
    "region_id string, source string"
)


def replay_windows(spark, records):
    """(anomaly windows, b-value windows) the streaming job computes for the whole replay, times relative to its last event."""
    rows = []
    for r in records:
        if (r.get("event_type") or "earthquake").lower() != "earthquake":
            continue
        lon, lat = float(r["longitude"]), float(r["latitude"])
        glon, glat, cell = grid_cell(lon, lat)
        rows.append((r["event_id"], replay_producer.parse_time(r["event_time"]), r.get("magnitude"), cell, glon, glat,
                     coarse_region(lon, lat), "replay"))
    last = max(row[1] for row in rows)
    events = spark.createDataFrame(rows, SCHEMA).cache()
    baselines = spark.read.option("header", True).schema(
        "cell_id string, baseline_mean double, baseline_std double, historical_events int"
    ).csv(str(BASELINE_CSV))
    anomalies = sorted(
        (r["cell_id"], (r["window_end"].replace(tzinfo=timezone.utc) - last).total_seconds(), r["event_count"])
        for r in anomaly_windows(events, events, baselines, 2.0, 3).collect()
    )
    bvalues = sorted(
        (row[4], (row[3].replace(tzinfo=timezone.utc) - last).total_seconds(), row[5], round(row[7], 4))
        for row in bvalue_rows(
            bvalue_window_magnitudes(events, events).collect(),
            load_long_term_bvalues(str(REGION_CSV)),
            0,
            20,
            mc_floor=load_long_term_mc(str(REGION_CSV)),
        )
    )
    events.unpersist()
    return anomalies, bvalues


def test_rebase_shift_is_whole_hours_and_ends_in_the_last_hour():
    latest = datetime(2026, 5, 17, 9, 12, 30, tzinfo=timezone.utc)
    now = datetime(2026, 11, 9, 12, 34, 56, tzinfo=timezone.utc)
    shift = replay_producer.rebase_shift(latest, now)
    assert shift.total_seconds() % 3600 == 0
    assert timedelta(0) <= now - (latest + shift) < timedelta(hours=1)
    assert replay_producer.rebase_shift(latest, latest + timedelta(hours=5)) == timedelta(hours=5)


def test_replay_windows_do_not_depend_on_the_start_minute(spark):
    records = replay_producer.read_jsonl(REPLAY_FILE)
    starts = [datetime(2026, 11, 9, 10, minute, 7, tzinfo=timezone.utc) for minute in (2, 21, 40, 58)]
    results = [replay_windows(spark, replay_producer.prepare_records(records, now=now)) for now in starts]
    anomalies, bvalues = results[0]
    assert anomalies and bvalues
    for other in results[1:]:
        assert other == results[0]

    # The old shift (exactly now - last event) moved the events across the hourly window grid.
    def exact_shift(now):
        latest = max(replay_producer.parse_time(r["event_time"]) for r in records)
        return [{**r, "event_time": (replay_producer.parse_time(r["event_time"]) + (now - latest)).isoformat()}
                for r in records]

    old = [replay_windows(spark, exact_shift(now)) for now in starts]
    assert len({(len(a), len(b)) for a, b in old}) > 1, [(len(a), len(b)) for a, b in old]
