"""process_batch and the active regions against a real PostgreSQL (the test-postgres service of scripts/test.ps1).

Regression tests for QA round 2: records that broke a cleaning rule, records far behind the newest
event and a number sent as text all vanished without a trace, and more than 5 separate anomalous
areas were merged into 5 active regions thousands of km wide.
"""
from __future__ import annotations

import json
import os
from contextlib import closing
from datetime import datetime, timedelta, timezone

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from src.common.config import PostgresConfig, StreamConfig  # noqa: E402
from src.streaming import streaming_job  # noqa: E402
from src.streaming.streaming_job import MESSAGE_SCHEMA, clean_events  # noqa: E402

HOST = os.getenv("TEST_POSTGRES_HOST", "")
REQUIRED = os.getenv("REQUIRE_TEST_DB", "0") == "1"
PG = PostgresConfig(host=HOST or "localhost", port=5432, db="seismic_test", user="seismic", password="seismic")
CFG = StreamConfig()
NOW = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)
STORED_SCHEMA = (
    "event_id string, event_ts timestamp, magnitude double, cell_id string, grid_lon int, grid_lat int, "
    "region_id string, source string"
)


@pytest.fixture
def db():
    try:
        conn = psycopg2.connect(PG.dsn, connect_timeout=5)
    except psycopg2.OperationalError as exc:
        if REQUIRED:
            raise
        pytest.skip(f"no test database at {HOST or 'localhost'}:5432 ({exc.__class__.__name__}); run scripts/test.ps1")
    with closing(conn), conn, conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    streaming_job.ensure_schema(PG, wait_seconds=30)
    streaming_job._LAST_CLUSTERING.clear()
    return PG


def query(sql, params=()):
    with closing(psycopg2.connect(PG.dsn)) as conn, conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def quake(event_id, t, mag=2.5, lon=-122.35, lat=37.62, **extra):
    return {"event_id": event_id, "event_time": t.isoformat(), "magnitude": mag, "longitude": lon, "latitude": lat,
            "event_type": "earthquake", "source": "usgs-live", **extra}


def messages(spark, values):
    """A micro-batch as clean_events() produces it from Kafka (partition 0, offsets 1, 2, ...)."""
    rows = [(v if isinstance(v, str) else json.dumps(v), 0, i + 1, NOW) for i, v in enumerate(values)]
    return clean_events(spark.createDataFrame(rows, MESSAGE_SCHEMA))


@pytest.fixture
def read_back(spark, monkeypatch):
    """load_stored_events without Spark's JDBC driver (not in the test image): same SQL ranges, via psycopg2."""
    calls = []

    def load(_spark, _pg, ranges):
        calls.append(ranges)
        where = " OR ".join("(event_time >= %s AND event_time < %s)" for _ in ranges)
        rows = query(
            "SELECT event_id, event_time, magnitude, cell_id, grid_lon, grid_lat, region_id, source "
            f"FROM seismic_events WHERE ({where}) AND COALESCE(LOWER(event_type), 'earthquake') = 'earthquake'",
            [t for r in ranges for t in r],
        )
        return spark.createDataFrame(rows, STORED_SCHEMA)

    monkeypatch.setattr(streaming_job, "load_stored_events", load)
    return calls


def run_batch(spark, batch, batch_id=0):
    baselines = spark.createDataFrame([], "cell_id string, baseline_mean double, baseline_std double")
    streaming_job.process_batch(batch, batch_id, spark, baselines, {}, {}, CFG, PG)


def test_rejected_records_are_stored_with_their_reason_and_old_events_are_analysed(db, spark, read_back, capsys):
    values = [quake(f"burst-{i}", NOW - timedelta(minutes=30 - i), lon=-122.35 + 0.01 * i) for i in range(4)]
    values += [
        quake("text-mag", NOW - timedelta(minutes=20), mag="3.0", lon=140.6, lat=35.2),
        quake("old-10d", NOW - timedelta(days=10), lon=20.7, lat=40.7),
        "not json at all",
        {**quake("no-time", NOW), "event_time": None},
        quake("lat95", NOW, lat=95.0),
    ]
    run_batch(spark, messages(spark, values))

    stored = dict(query("SELECT event_id, magnitude FROM seismic_events"))
    assert sorted(stored) == ["burst-0", "burst-1", "burst-2", "burst-3", "old-10d", "text-mag"]
    assert stored["text-mag"] == 3.0  # was NULL: the double schema turned "3.0" into NULL silently
    rejected = query("SELECT kafka_offset, reason, event_id, raw_value FROM rejected_records ORDER BY kafka_offset")
    assert [r[:3] for r in rejected] == [
        (7, "not a JSON object", None), (8, "missing event_time", "no-time"), (9, "latitude out of range", "lat95"),
    ]
    assert rejected[0][3] == "not json at all"
    out = capsys.readouterr().out
    assert "Batch 0: received=6, rejected=3 (1 latitude out of range, 1 missing event_time, 1 not a JSON object; " \
           "see rejected_records)" in out
    # The read-back covers the hours around the batch's events, not the 10 days in between.
    assert len(read_back[0]) == 2
    # The burst of 4 events in one cell is an anomaly and an active region.
    assert query("SELECT COUNT(DISTINCT cell_id) FROM anomaly_windows")[0][0] == 1
    assert query("SELECT cells, within_radius, radius_limit_km, max_k FROM active_regions") == [
        ("-123:37", True, 300.0, CFG.max_kmeans_clusters)
    ]

    # The same micro-batch run again after a restart (same batch id) stores nothing twice.
    run_batch(spark, messages(spark, values))
    assert query("SELECT COUNT(*) FROM rejected_records")[0][0] == 3
    assert query("SELECT COUNT(*) FROM seismic_events")[0][0] == 6


def test_a_batch_of_rejected_records_only_says_so(db, spark, read_back, capsys):
    run_batch(spark, messages(spark, ["", quake("lon200", NOW, lon=-200.0)]), batch_id=4)
    assert "Batch 4: received=0, rejected=2 (1 longitude out of range, 1 not a JSON object; see rejected_records)" \
        in capsys.readouterr().out
    assert query("SELECT COUNT(*) FROM seismic_events")[0][0] == 0


def store_hot_spots(n, source="qa-many"):
    """n anomalous cells 15 degrees of longitude apart, each with one flagged 6-hour window ending now."""
    with closing(psycopg2.connect(PG.dsn)) as conn, conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO seismic_events (event_id, event_time, longitude, latitude, source) VALUES (%s, %s, 0, 0, %s)",
            (f"{source}-clock", NOW, source),
        )
        for i in range(n):
            lon, lat = -170 + 15 * i, 10 if i % 2 else -20
            cur.execute(
                """
                INSERT INTO anomaly_windows (batch_id, source, window_start, window_end, cell_id, grid_lon, grid_lat,
                                             event_count, baseline_mean, baseline_std, z_score)
                VALUES (1, %s, %s, %s, %s, %s, %s, 4, 0.5, 1.0, 3.5)
                """,
                (source, NOW - timedelta(hours=5), NOW + timedelta(hours=1), f"{lon}:{lat}", lon, lat),
            )


def test_many_separate_hot_spots_get_their_own_regions(db, spark):
    store_hot_spots(22)
    assert streaming_job.refresh_active_regions(spark, 1, CFG, PG) == 22
    rows = query("SELECT radius_km, within_radius, radius_limit_km, k FROM active_regions WHERE source = 'qa-many'")
    assert len(rows) == 22
    assert all(radius <= 300 and within and limit == 300 and k == 22 for radius, within, limit, k in rows)
    # Every anomaly window is labelled with its own region.
    assert query("SELECT COUNT(DISTINCT cluster_id) FROM anomaly_windows WHERE source = 'qa-many'")[0][0] == 22


def test_regions_over_the_radius_at_the_k_cap_are_flagged_and_logged(db, spark, capsys):
    store_hot_spots(22)
    capped = StreamConfig(max_kmeans_clusters=5)
    assert streaming_job.refresh_active_regions(spark, 1, capped, PG) == 5
    rows = query("SELECT radius_km, within_radius, max_k FROM active_regions WHERE source = 'qa-many'")
    assert len(rows) == 5 and {r[2] for r in rows} == {5}
    assert all((radius > 300) == (not within) for radius, within, _k in rows)
    assert any(not within for _r, within, _k in rows)
    out = capsys.readouterr().out
    assert "active regions of source qa-many are wider than REGION_RADIUS_KM=300 km" in out
    assert "need more than MAX_KMEANS_CLUSTERS=5 regions" in out
