import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.common.geo import coarse_region, flatten_usgs_feature, grid_cell, haversine_km
from src.producer import live_usgs_producer, replay_producer

FEATURE = {
    "type": "Feature",
    "id": "nc75012345",
    "properties": {
        "mag": 2.4,
        "place": "5 km N of Somewhere, CA",
        "time": 1794200000000,
        "type": "earthquake",
        "status": "automatic",
        "tsunami": 0,
        "sig": 89,
        "nst": 21,
        "rms": 0.05,
        "gap": 61,
        "felt": None,
        "url": "https://earthquake.usgs.gov/earthquakes/eventpage/nc75012345",
    },
    "geometry": {"type": "Point", "coordinates": [-122.35, 37.62, 8.1]},
}


def test_flatten_usgs_feature_cleans_and_enriches():
    record = flatten_usgs_feature(FEATURE)
    assert record["event_id"] == "nc75012345"
    assert record["event_time"].startswith("2026-11-09T")
    assert (record["grid_lon"], record["grid_lat"], record["cell_id"]) == (-123, 37, "-123:37")
    assert record["region_id"] == "R-130_+30"
    assert record["source"] == "usgs-live"
    assert record["depth_km"] == 8.1


def test_flatten_rejects_records_without_id_time_or_valid_coordinates():
    assert flatten_usgs_feature({**FEATURE, "geometry": {"coordinates": [-122.0]}}) is None
    assert flatten_usgs_feature({**FEATURE, "geometry": {"coordinates": [200.0, 10.0, 1.0]}}) is None
    assert flatten_usgs_feature({**FEATURE, "id": None, "properties": {**FEATURE["properties"], "time": None}}) is None


def test_region_and_cell_formats():
    assert coarse_region(-122.35, 37.62) == "R-130_+30"
    assert coarse_region(140.6, 35.2) == "R+140_+30"
    assert coarse_region(-72.5, -35.1) == "R-080_-40"
    assert coarse_region(5.0, 0.5) == "R+000_+00"
    assert grid_cell(-0.5, -0.5) == (-1, -1, "-1:-1")


def test_haversine():
    assert abs(haversine_km(0, 0, 0, 1) - 111.19) < 0.1


def test_replay_rebase_moves_catalogue_to_now_and_keeps_spacing():
    records = [
        {"event_id": "b", "event_time": "2026-05-17T09:00:00+00:00"},
        {"event_id": "a", "event_time": "2026-05-17T07:00:00+00:00"},
    ]
    now = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)
    out = replay_producer.prepare_records(records, now=now)
    assert [r["event_id"] for r in out] == ["a", "b"]
    times = [datetime.fromisoformat(r["event_time"]) for r in out]
    assert times[-1] == now
    assert times[1] - times[0] == timedelta(hours=2)
    assert {r["source"] for r in out} == {"replay"}
    # The input is not modified and rebase can be switched off.
    assert records[0]["event_time"] == "2026-05-17T09:00:00+00:00"
    kept = replay_producer.prepare_records(records, now=now, rebase=False)
    assert kept[-1]["event_time"] == "2026-05-17T09:00:00+00:00"


REPLAY_FILE = Path(__file__).resolve().parents[2] / "data/replay_catalog/sample_replay_events.jsonl"


def test_replay_catalogue_has_a_mainshock_sequence():
    records = replay_producer.read_jsonl(REPLAY_FILE)
    assert len(records) == 184  # 84 hand-made records + 100 generated aftershocks
    assert max(r["magnitude"] for r in records) >= 5.0
    # Enough events in the mainshock's region for 24-hour b-value windows (20 above Mc).
    assert sum(r["region_id"] == "R-130_+30" for r in records) > 100


def test_generated_aftershocks_are_reproducible_and_follow_the_mainshock(tmp_path):
    from src.producer import extend_replay_catalog as gen

    copy = tmp_path / "catalogue.jsonl"
    copy.write_text(REPLAY_FILE.read_text(encoding="utf-8"), encoding="utf-8")
    gen.main(copy)
    gen.main(copy)  # running twice replaces the generated events instead of adding more
    assert copy.read_text(encoding="utf-8") == REPLAY_FILE.read_text(encoding="utf-8")

    records = replay_producer.read_jsonl(copy)
    original = [r for r in records if not r["event_id"].startswith(gen.GENERATED_PREFIX)]
    generated = [r for r in records if r["event_id"].startswith(gen.GENERATED_PREFIX)]
    assert (len(original), len(generated)) == (84, gen.N_AFTERSHOCKS)
    main = next(r for r in original if r["event_id"] == gen.MAINSHOCK_ID)
    t0 = replay_producer.parse_time(main["event_time"])
    for r in generated:
        hours = (replay_producer.parse_time(r["event_time"]) - t0).total_seconds() / 3600
        assert 0 < hours <= gen.SPAN_HOURS
        assert gen.M_MIN <= r["magnitude"] <= gen.M_MAX
        assert haversine_km(main["longitude"], main["latitude"], r["longitude"], r["latitude"]) <= gen.MAX_DISTANCE_KM + 0.1
        assert (r["cell_id"], r["region_id"]) == ("-123:37", "R-130_+30")
    # Omori-Utsu decay: more aftershocks in the first two hours than in the last ten.
    hours = [(replay_producer.parse_time(r["event_time"]) - t0).total_seconds() / 3600 for r in generated]
    assert sum(h <= 2 for h in hours) > sum(h > 12 for h in hours)


class FakeFuture:
    def get(self, timeout=None):
        return None


class FakeProducer:
    def __init__(self):
        self.sent = []

    def send(self, topic, key=None, value=None):
        self.sent.append((topic, key, value))
        return FakeFuture()

    def flush(self, timeout=None):
        return None


def test_live_producer_publishes_only_unseen_events_oldest_first(tmp_path, monkeypatch):
    monkeypatch.setattr(live_usgs_producer, "STATE_FILE", tmp_path / "seen.json")
    older = {**FEATURE, "id": "nc1", "properties": {**FEATURE["properties"], "time": 1794100000000}}
    feed = {"features": [FEATURE, older, {**FEATURE, "id": "already"}]}
    records = live_usgs_producer.parse_feed(feed)
    producer = FakeProducer()
    seen = {"already"}
    new, published = live_usgs_producer.publish_new(producer, records, seen)
    assert (new, published) == (2, 2)
    assert [key for _topic, key, _value in producer.sent] == ["nc1", "nc75012345"]
    assert set(json.loads((tmp_path / "seen.json").read_text())) == {"already", "nc1", "nc75012345"}
    # A second poll of the same feed publishes nothing (producer-side de-duplication).
    assert live_usgs_producer.publish_new(producer, records, seen) == (0, 0)
