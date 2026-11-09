"""compute_aftershock_forecasts against a real PostgreSQL (the test-postgres service of scripts/test.ps1).

Regression tests for the QA finding that forecasts covered only the 20 newest active M >= 5
events (LIMIT 20): older mainshocks kept a stale forecast or never got one, and the M5
aftershocks of a large event pushed the large event itself out of the list.
"""
from __future__ import annotations

import math
import os
from contextlib import closing
from datetime import datetime, timedelta, timezone

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from src.common.config import PostgresConfig, StreamConfig  # noqa: E402
from src.streaming import streaming_job  # noqa: E402
from src.streaming.streaming_job import EVENT_COLUMNS  # noqa: E402

HOST = os.getenv("TEST_POSTGRES_HOST", "")
REQUIRED = os.getenv("REQUIRE_TEST_DB", "0") == "1"
PG = PostgresConfig(host=HOST or "localhost", port=5432, db="seismic_test", user="seismic", password="seismic")
CFG = StreamConfig()
NOW = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)


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
    return PG


def event(event_id, t, mag, lon, lat, source="replay", event_type="earthquake", place=None):
    row = {c: None for c in EVENT_COLUMNS}
    row.update(
        event_id=event_id, event_ts=t, ingestion_ts=t, magnitude=mag, longitude=lon, latitude=lat,
        event_type=event_type, source=source, place=place or event_id,
    )
    return row


def store(rows):
    streaming_job.write_event_rows(rows, PG)
    streaming_job.write_mainshock_rows(rows, PG, CFG.mainshock_magnitude)


def query(sql, params=()):
    with closing(psycopg2.connect(PG.dsn)) as conn, conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def latest_forecasts():
    """mainshock id -> (aftershock_count, forecast_origin_hours, batch_id) of its latest forecast."""
    rows = query(
        """
        SELECT DISTINCT ON (mainshock_id) mainshock_id, aftershock_count, forecast_origin_hours, batch_id
        FROM aftershock_forecasts ORDER BY mainshock_id, fitted_at DESC, id DESC
        """
    )
    return {r[0]: (r[1], r[2], r[3]) for r in rows}


def isolated_sequences(n, t_last, source="usgs-live"):
    """n M5.2 mainshocks 15 degrees of longitude apart (1,600 km), one per hour, each with 3 aftershocks."""
    rows = []
    for i in range(n):
        t = t_last - timedelta(hours=n - 1 - i)
        lon = -170.0 + 15.0 * (i % 23)
        lat = -40.0 + 30.0 * (i // 23)
        rows.append(event(f"{source}-main-{i:02d}", t, 5.2, lon, lat, source))
        rows += [event(f"{source}-a{i:02d}-{j}", t + timedelta(minutes=10 * (j + 1)), 3.0, lon + 0.1, lat, source)
                 for j in range(3)]
    return rows


def test_every_active_mainshock_gets_a_current_forecast_not_only_the_newest_20(db):
    store(isolated_sequences(25, NOW - timedelta(hours=2)))
    fitted, stored = streaming_job.compute_aftershock_forecasts(1, CFG, PG)
    assert (fitted, stored) == (25, 25)
    first = latest_forecasts()
    assert len(first) == 25  # the old LIMIT 20 fitted only the 20 newest

    # The clock moves (one new event far away): every forecast is refitted from the new "now",
    # including the oldest mainshocks that used to keep their old forecast.
    store([event("usgs-live-later", NOW + timedelta(hours=3), 2.0, 60.0, 60.0, "usgs-live")])
    assert streaming_job.compute_aftershock_forecasts(2, CFG, PG) == (25, 25)
    second = latest_forecasts()
    clock_rows = query("SELECT MAX(event_time) FROM seismic_events WHERE source = 'usgs-live'")
    for main_id, main_time in query("SELECT event_id, event_time FROM mainshocks"):
        count, origin, batch_id = second[main_id]
        assert batch_id == 2 and count == 3
        assert math.isclose(origin, (clock_rows[0][0] - main_time).total_seconds() / 3600.0, abs_tol=1e-3)


def test_unchanged_forecasts_are_not_stored_again(db):
    store(isolated_sequences(3, NOW))
    assert streaming_job.compute_aftershock_forecasts(1, CFG, PG) == (3, 3)
    # Nothing new for this source: same fits, nothing stored, the latest rows stay current.
    assert streaming_job.compute_aftershock_forecasts(2, CFG, PG) == (3, 0)
    # Another source's events do not change this source's forecasts either.
    store([event("replay-x", NOW + timedelta(hours=5), 2.0, 0.0, 0.0, "replay")])
    assert streaming_job.compute_aftershock_forecasts(3, CFG, PG) == (3, 0)
    assert query("SELECT COUNT(*) FROM aftershock_forecasts")[0][0] == 3


def test_a_large_sequence_is_forecast_once_for_its_largest_event(db):
    """The QA repro: an M7.0 with 25 M5.0-5.4 and 60 M3-4.5 aftershocks in 6 hours, plus a foreshock."""
    t0 = NOW - timedelta(hours=6)
    rows = [event("m7", t0, 7.0, 100.0, -5.0, place="Test M7.0"),
            event("fore", t0 - timedelta(hours=3), 5.2, 100.2, -5.1)]
    for i in range(25):
        rows.append(event(f"m5-{i:02d}", t0 + timedelta(minutes=5 + 13 * i), 5.0 + (i % 5) / 10.0,
                          100.0 + 0.02 * (i % 7), -5.0 - 0.02 * (i % 3)))
    for i in range(60):
        rows.append(event(f"small-{i:02d}", t0 + timedelta(minutes=3 + 6 * i), 3.0 + (i % 16) / 10.0,
                          99.9 + 0.01 * (i % 9), -5.1 + 0.01 * (i % 4)))
    # 30 newer, unrelated M5 events far away (no aftershocks): they used to fill the LIMIT 20.
    rows += [event(f"other-{i:02d}", NOW - timedelta(minutes=i), 5.0, -60.0 + 3.0 * i, 40.0) for i in range(30)]
    store(rows)

    fitted, stored = streaming_job.compute_aftershock_forecasts(1, CFG, PG)
    forecasts = latest_forecasts()
    assert "m7" in forecasts and forecasts["m7"][0] == 85  # all 85 aftershocks, the M5s included
    assert set(forecasts) == {"m7"} and fitted == stored == 1

    larger = dict(query("SELECT event_id, larger_event_id FROM mainshocks"))
    assert larger["m7"] is None
    assert larger["fore"] == "m7"  # a foreshock is part of the sequence too
    assert all(larger[f"m5-{i:02d}"] == "m7" for i in range(25))
    assert sum(1 for k, v in larger.items() if k.startswith("other-") and v is None) == 30


def test_aftershocks_across_the_antimeridian_and_far_east_at_high_latitude_are_counted(db):
    rows = [event("fiji", NOW - timedelta(hours=10), 6.1, 179.8, -17.5, "usgs-live")]
    # West of the date line (positive longitude) and east of it (negative longitude), within 150 km.
    rows += [event(f"fiji-a{j}", NOW - timedelta(hours=9 - j), 4.4, 179.6 if j % 2 else -179.4, -17.6, "usgs-live")
             for j in range(6)]
    rows.append(event("alaska", NOW - timedelta(hours=10), 5.6, -150.0, 62.0, "usgs-live"))
    # 280 km east of the Alaskan mainshock: 5.4 degrees of longitude, outside the old +-4 degree box.
    rows += [event(f"alaska-a{j}", NOW - timedelta(hours=8 - j), 3.5, -144.6, 62.0, "usgs-live") for j in range(4)]
    store(rows)
    streaming_job.compute_aftershock_forecasts(1, CFG, PG)
    forecasts = latest_forecasts()
    assert forecasts["fiji"][0] == 6
    assert forecasts["alaska"][0] == 4
