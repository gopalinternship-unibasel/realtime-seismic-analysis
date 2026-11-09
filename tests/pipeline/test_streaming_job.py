"""The streaming job's ingest path: parse, clean, enrich and de-duplicate across micro-batches."""

import json

from src.streaming import streaming_job
from src.streaming.streaming_job import EVENT_SCHEMA, load_long_term_bvalues, load_long_term_mc, prepare_stream


def record(event_id, event_time, lon=-122.35, lat=37.62, **extra):
    base = {"event_id": event_id, "event_time": event_time, "magnitude": 2.4, "longitude": lon, "latitude": lat,
            "event_type": "earthquake", "source": "usgs-live"}
    base.update(extra)
    return base


def write_batch(folder, name, records):
    folder.mkdir(parents=True, exist_ok=True)
    (folder / name).write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")


def test_prepare_stream_cleans_enriches_and_deduplicates_across_micro_batches(spark, tmp_path):
    incoming = tmp_path / "incoming"
    write_batch(incoming, "1.json", [
        record("a", "2026-11-09T01:00:00+00:00"),
        record("a", "2026-11-09T01:00:00+00:00"),  # repeated inside one micro-batch
        record("b", "2026-11-09T02:00:00+00:00", lon=140.6, lat=35.2, source=None),
        record("bad-lon", "2026-11-09T02:00:00+00:00", lon=200.0),
        record("no-time", None),
        {**record("x", "2026-11-09T02:00:00+00:00"), "event_id": None},
    ])
    # A second micro-batch: a USGS revision of "a" (same id, new magnitude) and a new event.
    write_batch(incoming, "2.json", [
        record("a", "2026-11-09T01:00:05+00:00", magnitude=2.6),
        record("c", "2026-11-09T03:00:00+00:00", lon=-72.5, lat=-35.1),
    ])

    raw = spark.readStream.schema(EVENT_SCHEMA).option("maxFilesPerTrigger", 1).json(str(incoming))
    query = (
        prepare_stream(raw)
        .writeStream.format("memory")
        .queryName("prepared_events")
        .outputMode("append")
        .option("checkpointLocation", str(tmp_path / "checkpoint"))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination(120)
    assert len([p for p in query.recentProgress if p["numInputRows"] > 0]) == 2  # two micro-batches

    rows = {r["event_id"]: r for r in spark.sql("SELECT * FROM prepared_events").collect()}
    assert sorted(rows) == ["a", "b", "c"]  # invalid records dropped, "a" kept once across both batches
    assert rows["a"]["magnitude"] == 2.4  # the later duplicate did not replace or repeat the first
    assert (rows["b"]["grid_lon"], rows["b"]["grid_lat"], rows["b"]["cell_id"]) == (140, 35, "140:35")
    assert rows["b"]["region_id"] == "R+140_+30" and rows["b"]["source"] == "unknown"
    assert rows["c"]["region_id"] == "R-080_-40"
    assert rows["c"]["event_ts"] is not None and rows["c"]["ingestion_ts"] is not None


def test_regional_baselines_are_read_by_column(tmp_path):
    csv = tmp_path / "region_bvalues.csv"
    csv.write_text("region_id,long_term_b,event_count,completeness_mag\nR-160_+50,0.7896,1232,1.7\nR+000_+00,x,1,2.0\n")
    assert load_long_term_bvalues(str(csv)) == {"R-160_+50": 0.7896}
    assert load_long_term_mc(str(csv)) == {"R-160_+50": 1.7, "R+000_+00": 2.0}
    assert load_long_term_mc(str(tmp_path / "missing.csv")) == {}
    assert streaming_job.load_region_values(str(csv), "no_such_column") == {}
