import json
import math
from datetime import datetime, timezone

from pyspark.sql import functions as F

from src.batch import compute_baselines
from src.batch.fetch_usgs_history import fdsn_rows, history_period

FDSN_CSV = """time,latitude,longitude,depth,mag,magType,nst,gap,dmin,rms,net,id,updated,place,type,horizontalError,depthError,magError,magNst,status,locationSource,magSource
2026-10-01T00:10:00.000Z,37.62,-122.35,8.1,2.4,md,21,61,0.01,0.05,nc,nc1,2026-10-01T01:00:00.000Z,"5 km N of A, CA",earthquake,0.2,0.4,0.1,10,reviewed,nc,nc
2026-10-01T03:00:00.000Z,95.0,-122.35,8.1,2.4,md,21,61,0.01,0.05,nc,bad,2026-10-01T01:00:00.000Z,"bad latitude",earthquake,0.2,0.4,0.1,10,reviewed,nc,nc
2026-10-01T04:00:00.000Z,35.2,140.6,30,4.6,mb,,80,1.2,0.7,us,us1,2026-10-01T05:00:00.000Z,"Japan",earthquake,5,2,0.1,30,reviewed,us,us
"""


def test_fdsn_csv_is_converted_and_cleaned():
    rows = fdsn_rows(FDSN_CSV)
    assert [r["event_id"] for r in rows] == ["nc1", "us1"]
    assert rows[0]["event_time"] == "2026-10-01T00:10:00.000Z"
    assert rows[0]["longitude"] == -122.35 and rows[0]["magnitude"] == "2.4"


def test_history_period_ends_at_midnight_utc():
    start, end = history_period(datetime(2026, 11, 9).date(), days=90)
    assert end == datetime(2026, 11, 9, tzinfo=timezone.utc)
    assert (end - start).days == 90


def historical(spark, rows):
    schema = compute_baselines.HISTORICAL_SCHEMA
    data = [(eid, datetime.fromisoformat(t), mag, lon, lat, 10.0, typ) for eid, t, mag, lon, lat, typ in rows]
    return spark.createDataFrame(data, schema)


def test_cell_baselines_count_empty_windows_as_zero(spark):
    # 4 earthquakes in one 6-hour window, then nothing for the other 3 windows of a 24-hour period.
    rows = [(f"e{i}", f"2026-10-01T0{i}:00:00+00:00", 2.0, -122.35, 37.62, "earthquake") for i in range(4)]
    rows.append(("blast", "2026-10-01T02:30:00+00:00", 1.5, -122.35, 37.62, "quarry blast"))
    enriched = compute_baselines.enrich(historical(spark, rows))
    n_windows = compute_baselines.window_count(
        datetime(2026, 10, 1, tzinfo=timezone.utc), datetime(2026, 10, 2, tzinfo=timezone.utc)
    )
    assert n_windows == 4
    (row,) = compute_baselines.cell_baselines(enriched, n_windows).collect()
    assert row["cell_id"] == "-123:37"
    assert row["historical_events"] == 4  # the quarry blast is not an earthquake
    assert row["baseline_mean"] == 1.0  # 4 events / 4 windows, not 4 events / 1 non-empty window
    assert abs(row["baseline_std"] - math.sqrt(3.0)) < 1e-6


def test_region_bvalues_use_streaming_region_ids(spark):
    rows = [
        (f"e{i}", f"2026-10-0{1 + i % 9}T00:00:00+00:00", 2.0 + (i % 13) / 10, -122.35, 37.62, "earthquake")
        for i in range(120)
    ]
    enriched = compute_baselines.enrich(historical(spark, rows))
    assert enriched.select(F.first("region_id")).first()[0] == "R-130_+30"
    regions = compute_baselines.region_bvalues(enriched, min_events=50)
    assert [r[0] for r in regions] == ["R-130_+30"]
    assert regions[0][1] > 0


def test_shipped_baselines_come_from_usgs_history():
    from pathlib import Path

    root = Path(__file__).resolve().parents[2] / "data"
    meta = json.loads((root / "baseline/baseline_metadata.json").read_text())
    assert meta["historical_csv"] == "usgs_historical_events.csv"
    assert meta["events"] > 1000 and meta["cells"] > 100
    header = (root / "baseline/cell_baselines.csv").read_text().splitlines()[0]
    assert header == "cell_id,baseline_mean,baseline_std,historical_events"
