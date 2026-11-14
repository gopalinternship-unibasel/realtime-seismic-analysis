"""The exporter of the static page (src/export/export_site.py) against the throwaway test database.

scripts/test.ps1 and the CI "tests" job start test-postgres and pass TEST_POSTGRES_HOST and
REQUIRE_TEST_DB=1; without them these tests are skipped.
"""
from __future__ import annotations

import json
import os
import sys
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

psycopg2 = pytest.importorskip("psycopg2")
from sqlalchemy import create_engine  # noqa: E402

from src.export import export_site  # noqa: E402

HOST = os.getenv("TEST_POSTGRES_HOST", "")
REQUIRED = os.getenv("REQUIRE_TEST_DB", "0") == "1"
SCHEMA_SQL = Path(os.getenv("SCHEMA_SQL", str(ROOT / "sql" / "init.sql")))
DSN = f"host={HOST or 'localhost'} port=5432 dbname=seismic_test user=seismic password=seismic options='-c TimeZone=UTC'"
T0 = datetime(2026, 11, 9, 2, 0, tzinfo=timezone.utc)  # the mainshock
NOW = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)  # latest replay event: 10 hours after it


@pytest.fixture
def engine():
    try:
        conn = psycopg2.connect(DSN, connect_timeout=5)
    except psycopg2.OperationalError as exc:
        if REQUIRED:
            raise
        pytest.skip(f"no test database at {HOST or 'localhost'}:5432 ({exc.__class__.__name__}); run scripts/test.ps1")
    with closing(conn), conn, conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        cur.execute(SCHEMA_SQL.read_text(encoding="utf-8"))
        seed(cur)
    eng = create_engine(f"postgresql+psycopg2://seismic:seismic@{HOST or 'localhost'}:5432/seismic_test")
    yield eng
    eng.dispose()


def insert_event(cur, event_id, t, mag, lon, lat, source="replay", depth=10.0, place=None, event_type="earthquake"):
    cur.execute(
        "INSERT INTO seismic_events (event_id, event_time, magnitude, depth_km, longitude, latitude, place, "
        "event_type, source, grid_lon, grid_lat, cell_id, region_id) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (event_id, t, mag, depth, lon, lat, place or event_id, event_type, source, int(lon // 1), int(lat // 1),
         f"{int(lon // 1)}_{int(lat // 1)}", "R-130_+30"),
    )


def seed(cur):
    insert_event(cur, "main", T0, 5.6, -122.0, 38.0, place="Demo M5.6 - Northern California", depth=8.34567)
    # Aftershocks: 5 within 300 km (hours 0.5, 1.5, 2.5, 4.5, 9.5) and 1 far away that must not count.
    for i, hours in enumerate([0.5, 1.5, 2.5, 4.5, 9.5]):
        insert_event(cur, f"after{i}", T0 + timedelta(hours=hours), 2.0 + i * 0.1, -122.0 + i * 0.05, 38.0)
    insert_event(cur, "far", T0 + timedelta(hours=3), 3.0, 140.0, 35.0)
    insert_event(cur, "last", NOW, None, -121.9, 38.1)  # no magnitude: stored as NULL
    insert_event(cur, "live1", NOW - timedelta(hours=1), 4.4, 10.0, 46.0, source="usgs-live", place="Alps")
    cur.execute(
        "INSERT INTO mainshocks (event_id, event_time, magnitude, longitude, latitude, place, source, active) "
        "VALUES ('main', %s, 5.6, -122.0, 38.0, 'Demo M5.6 - Northern California', 'replay', TRUE)", (T0,)
    )
    cur.execute(
        "INSERT INTO aftershock_forecasts (batch_id, mainshock_id, fitted_at, aftershock_count, k_value, c_value, "
        "p_value, next_hour_rate, next_six_hour_expected, r_squared, forecast_origin_hours, p_estimated) "
        "VALUES (1, 'main', %s, 5, 20.0, 0.05, 1.1, 1.5, 8.5, 0.87, 10.0, TRUE)", (NOW,)
    )
    # The replay clock is three hours after the forecast start: the forecast is stale.
    cur.execute(
        "INSERT INTO forecast_runs (source, clock, batch_id, mainshocks, forecasts) VALUES ('replay', %s, 2, 1, 1)",
        (NOW + timedelta(hours=3),),
    )
    for end, count, z, cluster in [(NOW - timedelta(hours=2), 5, 3.456, 0), (NOW - timedelta(hours=1), 7, 4.0, 0)]:
        cur.execute(
            "INSERT INTO anomaly_windows (batch_id, source, window_start, window_end, cell_id, grid_lon, grid_lat, "
            "event_count, z_score, cluster_id) VALUES (1,'replay',%s,%s,'-122_38',-122,38,%s,%s,%s)",
            (end - timedelta(hours=6), end, count, z, cluster),
        )
    cur.execute(
        "INSERT INTO active_regions (source, cluster_id, centroid_lon, centroid_lat, cell_count, cells, "
        "max_z_score, max_event_count, radius_km, k, within_radius, radius_limit_km, max_k) "
        "VALUES ('replay', 0, -122.123456, 38.0, 1, '-122_38', 4.0, 7, 12.34, 1, FALSE, 300, 30)"
    )
    for i, (b, alert) in enumerate([(0.9, False), (1.4, True)]):
        end = NOW - timedelta(hours=1 - i)
        cur.execute(
            "INSERT INTO bvalue_windows (batch_id, source, window_start, window_end, region_id, event_count, "
            "completeness_mag, b_value, b_sigma, long_term_b, deviation, is_alert) "
            "VALUES (1,'replay',%s,%s,'R-130_+30',25,2.0,%s,0.123456,0.88,0.1,%s)",
            (end - timedelta(hours=24), end, b, alert),
        )
    for offset, reason in [(0, "not a JSON object"), (1, "not a JSON object"), (2, "latitude out of range")]:
        cur.execute(
            "INSERT INTO rejected_records (kafka_partition, kafka_offset, batch_id, reason) VALUES (0, %s, 1, %s)",
            (offset, reason),
        )


def run_export(engine, tmp_path, monkeypatch=None, **kwargs):
    if monkeypatch is not None:
        monkeypatch.setenv("GITHUB_SHA", "abc123")
    sizes = export_site.export_site(engine, tmp_path, **kwargs)
    return sizes, {p.name: json.loads(p.read_text(encoding="utf-8")) for p in tmp_path.glob("*.json")}


def records(tbl):
    return [dict(zip(tbl["columns"], row)) for row in tbl["rows"]]


def test_writes_every_file_as_valid_json(engine, tmp_path):
    sizes, data = run_export(engine, tmp_path)
    assert set(data) == {"meta.json", "events.json", "anomaly_windows.json", "active_regions.json",
                         "bvalue_windows.json", "mainshocks.json", "status.json"}
    assert all(size < 20_000 for size in sizes.values())


def test_events_are_newest_first_and_rounded(engine, tmp_path):
    _, data = run_export(engine, tmp_path)
    events = records(data["events.json"])
    assert len(events) == 9
    assert [e["time"] for e in events] == sorted((e["time"] for e in events), reverse=True)
    main = next(e for e in events if e["id"] == "main")
    assert main == {"id": "main", "time": "2026-11-09T02:00:00Z", "lat": 38.0, "lon": -122.0, "depth_km": 8.3,
                    "mag": 5.6, "place": "Demo M5.6 - Northern California", "source": "replay",
                    "cell_id": "-122_38", "region_id": "R-130_+30"}
    assert next(e for e in events if e["id"] == "last")["mag"] is None
    assert {e["source"] for e in events} == {"replay", "usgs-live"}


def test_event_limit_is_reported_in_meta(engine, tmp_path):
    _, data = run_export(engine, tmp_path, max_events=3)
    assert len(data["events.json"]["rows"]) == 3
    assert data["meta.json"]["events_total"] == 9
    assert data["meta.json"]["events_exported"] == 3


def test_meta_has_time_commit_and_newest_event_per_source(engine, tmp_path, monkeypatch):
    _, data = run_export(engine, tmp_path, monkeypatch)
    meta = data["meta.json"]
    assert meta["commit"] == "abc123"
    generated = datetime.strptime(meta["generated_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    assert abs((datetime.now(tz=timezone.utc) - generated).total_seconds()) < 300
    assert meta["newest_event"] == {"replay": "2026-11-09T12:00:00Z", "usgs-live": "2026-11-09T11:00:00Z"}
    assert "events.json" in meta["files"]


def test_commit_is_null_without_github_sha(engine, tmp_path, monkeypatch):
    monkeypatch.delenv("GITHUB_SHA", raising=False)
    _, data = run_export(engine, tmp_path)
    assert data["meta.json"]["commit"] is None


def test_anomaly_windows_regions_and_bvalues(engine, tmp_path):
    _, data = run_export(engine, tmp_path)
    windows = records(data["anomaly_windows.json"])
    assert [w["event_count"] for w in windows] == [5, 7]
    assert windows[0]["z"] == 3.46 and windows[0]["window_end"] == "2026-11-09T10:00:00Z"
    assert windows[0]["cell_id"] == "-122_38" and windows[0]["cluster_id"] == 0
    region = records(data["active_regions.json"])[0]
    assert region["centroid_lon"] == -122.123 and region["radius_km"] == 12.3
    assert region["within_radius"] is False and region["radius_limit_km"] == 300 and region["max_k"] == 30
    bvalues = records(data["bvalue_windows.json"])
    assert [b["is_alert"] for b in bvalues] == [False, True]
    assert bvalues[0]["b_sigma"] == 0.123 and bvalues[0]["long_term_b"] == 0.88 and bvalues[1]["b_value"] == 1.4


def test_mainshock_forecast_and_curve_points(engine, tmp_path):
    _, data = run_export(engine, tmp_path)
    shocks = data["mainshocks.json"]
    assert shocks["active_counts"] == {"replay": 1}
    assert shocks["stale_seconds"] == 1
    assert len(shocks["items"]) == 1
    item = shocks["items"][0]
    assert item["id"] == "main" and item["active"] is True and item["mag"] == 5.6
    assert item["clock"] == "2026-11-09T15:00:00Z"
    f = item["forecast"]
    assert (f["aftershocks"], f["k"], f["c"], f["p"], f["p_estimated"]) == (5, 20.0, 0.05, 1.1, True)
    assert (f["next_hour"], f["next_six_hours"], f["r_squared"], f["origin_hours"]) == (1.5, 8.5, 0.87, 10.0)
    # The forecast starts at the mainshock plus 10 hours; the clock is 3 hours later, so it is stale.
    assert f["start"] == "2026-11-09T12:00:00Z"
    # Observed aftershocks per hour: the far event is outside 300 km; the five near ones fill hours 0, 1, 2, 4, 9.
    assert f["hist"] == [1, 1, 1, 0, 1, 0, 0, 0, 0, 1, 0]
    assert len(f["curve"]["x"]) == len(f["curve"]["y"]) == 200
    assert f["curve"]["x"][0] == 0.25 and f["curve"]["x"][-1] == 16.0
    assert len(f["points"]["x"]) == len(f["points"]["y"]) == 6
    assert f["points"]["x"][0] == 10.5
    assert sum(f["points"]["y"]) == pytest.approx(8.5, abs=2.0)  # K = 20: about 8 expected in 6 hours
    assert all(y > 0 for y in f["curve"]["y"])


def test_status_rows_rejects_and_clocks(engine, tmp_path):
    _, data = run_export(engine, tmp_path)
    status = data["status.json"]
    assert status["rows"]["seismic_events"] == {"replay": 8, "usgs-live": 1}
    assert status["rows"]["anomaly_windows"] == {"replay": 2}
    assert status["rows"]["aftershock_forecasts"] == {"replay": 1}
    assert status["rejected"] == {"total": 3, "by_reason": {"not a JSON object": 2, "latitude out of range": 1}}
    assert status["forecast_runs"]["replay"] == {"clock": "2026-11-09T15:00:00Z", "batch_id": 2, "mainshocks": 1,
                                                 "forecasts": 1}


def test_empty_database_exports_empty_tables(engine, tmp_path):
    with closing(psycopg2.connect(DSN)) as conn, conn, conn.cursor() as cur:
        cur.execute("TRUNCATE seismic_events CASCADE; TRUNCATE anomaly_windows, bvalue_windows, active_regions, "
                    "rejected_records, forecast_runs")
    _, data = run_export(engine, tmp_path)
    assert data["events.json"]["rows"] == [] and data["bvalue_windows.json"]["rows"] == []
    assert data["mainshocks.json"]["items"] == [] and data["mainshocks.json"]["active_counts"] == {}
    assert data["meta.json"]["newest_event"] == {} and data["status.json"]["rejected"]["total"] == 0


def test_command_line_writes_into_the_folder(engine, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(export_site.dash, "ENGINE", engine)
    assert export_site.main(["--out", str(tmp_path / "data"), "--max-events", "5"]) == 0
    assert (tmp_path / "data" / "meta.json").exists()
    assert "events.json" in capsys.readouterr().out
