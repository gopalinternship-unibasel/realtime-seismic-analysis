from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F

from src.common.geo import coarse_region
from src.streaming.analytics import (
    anomaly_windows,
    bvalue_rows,
    bvalue_window_magnitudes,
    cluster_active_cells,
    region_id_expr,
)

T0 = datetime(2026, 11, 9, 0, 0, tzinfo=timezone.utc)
EVENT_SCHEMA = (
    "event_id string, event_ts timestamp, magnitude double, cell_id string, grid_lon int, grid_lat int, "
    "region_id string, source string"
)


def events(spark, rows):
    """rows: (event_id, minutes after T0, magnitude[, cell_id, source])"""
    data = []
    for row in rows:
        event_id, minutes, mag = row[:3]
        cell = row[3] if len(row) > 3 else "-123:37"
        source = row[4] if len(row) > 4 else "replay"
        lon, lat = (int(x) for x in cell.split(":"))
        data.append(
            (event_id, T0 + timedelta(minutes=minutes), mag, cell, lon, lat, coarse_region(lon + 0.5, lat + 0.5), source)
        )
    return spark.createDataFrame(data, EVENT_SCHEMA)


def baselines(spark, rows=(("-123:37", 0.2, 0.5),)):
    return spark.createDataFrame(list(rows), "cell_id string, baseline_mean double, baseline_std double")


def test_spark_region_id_matches_python_region_id(spark):
    points = [(-122.35, 37.62), (140.6, 35.2), (-72.5, -35.1), (5.0, 0.5), (-0.1, -0.1), (179.9, -89.0), (180.0, -16.5)]
    df = spark.createDataFrame(points, "lon double, lat double")
    got = [r["rid"] for r in df.select(region_id_expr(F.col("lon"), F.col("lat")).alias("rid")).collect()]
    assert got == [coarse_region(lon, lat) for lon, lat in points]


def test_anomaly_windows_count_events_from_earlier_micro_batches(spark):
    batch1 = events(spark, [("e1", 10, 2.0), ("e2", 20, 2.1)])
    batch2 = events(spark, [("e3", 30, 2.2), ("e4", 40, 2.3)])
    stored = batch1.union(batch2)  # what PostgreSQL holds after batch 2

    # Old behaviour: windows from the micro-batch alone see only 2 events (< MIN_EVENTS 3).
    assert anomaly_windows(batch2, batch2, baselines(spark), 2.0, 3).count() == 0

    flagged = anomaly_windows(stored, batch2, baselines(spark), 2.0, 3).collect()
    assert flagged, "windows must span micro-batches"
    assert {r["event_count"] for r in flagged} == {4}
    # 6-hour windows sliding by 1 hour: an event at 00:10-00:40 belongs to 6 windows.
    assert len(flagged) == 6
    assert all(abs(r["z_score"] - (4 - 0.2) / 0.5) < 1e-9 for r in flagged)


def test_anomaly_windows_only_touch_windows_of_the_current_batch(spark):
    old = events(spark, [(f"o{i}", 10 + i, 2.0) for i in range(5)])  # 00:10-00:14
    late_batch = events(spark, [("n1", 20 * 60, 2.0)])  # 20:00, far from the old burst
    flagged = anomaly_windows(old.union(late_batch), late_batch, baselines(spark), 2.0, 3).collect()
    assert flagged == []  # the old windows are not rewritten by an unrelated batch


def test_unknown_cells_use_default_baseline(spark):
    batch = events(spark, [(f"x{i}", 5 * i, 3.0, "10:10") for i in range(3)])
    flagged = anomaly_windows(batch, batch, baselines(spark), 2.0, 3).collect()
    assert flagged and all(r["baseline_mean"] == 0.5 and r["baseline_std"] == 1.0 for r in flagged)


def test_bvalue_windows_accumulate_small_batches(spark):
    mags = [1.6, 1.7, 1.8, 1.9, 2.0, 2.2, 2.4, 2.7, 3.1]
    batches = [events(spark, [(f"b{i * 3 + j}", 60 * (i * 3 + j), mags[i * 3 + j]) for j in range(3)]) for i in range(3)]
    last = batches[-1]

    # Old behaviour: a 3-event micro-batch never reaches the 5 events a b-value needs.
    assert bvalue_rows(bvalue_window_magnitudes(last, last).collect(), {}, 1, min_events=5) == []

    stored = batches[0].union(batches[1]).union(batches[2])
    rows = bvalue_rows(bvalue_window_magnitudes(stored, last).collect(), {"R-130_+30": 0.95}, 2, min_events=5)
    assert rows, "b-value windows must use events from earlier micro-batches"
    full = [r for r in rows if r[5] >= 5]
    assert full
    batch_id, source, _start, _end, region, n_above, mc, b, sigma, long_b, deviation, _alert = full[0]
    assert (batch_id, source, region, long_b) == (2, "replay", "R-130_+30", 0.95)
    assert b > 0 and sigma > 0 and abs(deviation - (b - 0.95)) < 1e-9


def test_bvalue_windows_need_min_events_above_mc(spark):
    small = events(spark, [(f"s{i}", 10 * i, 2.0 + i / 10) for i in range(12)])
    grouped = bvalue_window_magnitudes(small, small).collect()
    assert bvalue_rows(grouped, {}, 1, min_events=20) == []
    assert bvalue_rows(grouped, {}, 1, min_events=5)


def test_sources_never_share_a_window(spark):
    live = events(spark, [(f"l{i}", i, 2.0, "-123:37", "usgs-live") for i in range(2)])
    replay = events(spark, [(f"r{i}", i, 2.0, "-123:37", "replay") for i in range(2)])
    both = live.union(replay)
    assert anomaly_windows(both, both, baselines(spark), 2.0, 3).count() == 0


def cell(cell_id, z=5.0, count=5):
    lon, lat = (int(x) for x in cell_id.split(":"))
    return {"cell_id": cell_id, "center_lon": lon + 0.5, "center_lat": lat + 0.5, "max_z_score": z, "max_event_count": count}


def test_kmeans_groups_nearby_cells_into_one_region(spark):
    assignment, regions = cluster_active_cells(spark, [cell("-123:37"), cell("-124:38"), cell("-123:38")])
    assert len(regions) == 1
    assert set(assignment.values()) == {0}
    assert regions[0].cells == ["-123:37", "-123:38", "-124:38"]
    assert regions[0].radius_km < 300


def test_kmeans_separates_distant_regions(spark):
    cells = [
        cell("-123:37", z=9.0), cell("-124:38", z=6.0), cell("-123:38", z=4.0),  # California
        cell("139:35", z=7.0), cell("140:35", z=3.0),  # Japan
        cell("-73:-35", z=3.5), cell("-72:-35", z=3.0),  # Chile
    ]
    assignment, regions = cluster_active_cells(spark, cells, max_k=5, radius_km=300)
    assert len(regions) == 3
    assert assignment["-123:37"] == assignment["-124:38"] == assignment["-123:38"] == 0  # strongest region first
    assert assignment["139:35"] == assignment["140:35"]
    assert assignment["-73:-35"] == assignment["-72:-35"]
    assert all(r.radius_km <= 300 for r in regions)
    california = regions[0]
    assert -124.5 < california.centroid_lon < -122.0 and 37.0 < california.centroid_lat < 39.0


def test_kmeans_handles_the_dateline(spark):
    _assignment, regions = cluster_active_cells(spark, [cell("179:-17"), cell("-180:-17")])
    assert len(regions) == 1
    assert abs(abs(regions[0].centroid_lon) - 180) < 1.0


def test_kmeans_respects_max_k_and_flags_the_regions_it_could_not_fit(spark):
    cells = [cell(f"{lon}:0") for lon in (-150, -90, -30, 30, 90, 150)]
    _assignment, regions = cluster_active_cells(spark, cells, max_k=3, radius_km=300)
    assert len(regions) == 3
    # Three regions cannot hold six spots 60 degrees apart within 300 km: the wide ones say so
    # (K-Means may still leave a single spot alone in a region, which is within the radius).
    assert all(r.within_radius == (r.radius_km <= 300) for r in regions)
    assert sum(not r.within_radius for r in regions) >= 1
    assert sum(len(r.cells) for r in regions if not r.within_radius) >= 4


def test_more_than_five_separate_hot_spots_each_get_a_region_within_the_radius(spark):
    # QA round 2: 22 M5 sequences 15 degrees of longitude apart. With k capped at 5 they were merged
    # into 5 "regions" with radii of 3,000 to 4,500 km and no warning.
    cells = [cell(f"{-170 + 15 * i}:{10 if i % 2 else -20}", z=3.5, count=4) for i in range(22)]
    assignment, regions = cluster_active_cells(spark, cells, radius_km=300)
    assert len(regions) == 22 and len(set(assignment.values())) == 22
    assert all(r.within_radius and r.radius_km <= 300 for r in regions)
    # A single region of nearby cells is still within the radius, also with the cap.
    _a, near = cluster_active_cells(spark, [cell("-123:37"), cell("-124:38")], max_k=1, radius_km=300)
    assert len(near) == 1 and near[0].within_radius


def test_bvalue_mc_is_never_below_the_regional_long_term_mc(spark):
    # One day with a burst of tiny events (M0.0-0.3) and a normal tail (M1.7-3.4), as in an
    # Alaska window on 9 Nov 2026: maximum curvature puts Mc at 0.0, far below the regional 1.7.
    tiny = [0.0] * 8 + [0.1] * 5 + [0.2] * 4 + [0.3] * 3
    tail = [round(1.7 + 0.1 * (i % 18), 1) for i in range(30)]
    batch = events(spark, [(f"t{i}", i, m, "-152:57") for i, m in enumerate(tiny + tail)])
    grouped = bvalue_window_magnitudes(batch, batch).collect()

    unfloored = bvalue_rows(grouped, {"R-160_+50": 0.79}, 1, min_events=20)
    assert unfloored and {r[6] for r in unfloored} == {0.0}

    floored = bvalue_rows(grouped, {"R-160_+50": 0.79}, 1, min_events=20, mc_floor={"R-160_+50": 1.7})
    assert floored and {r[6] for r in floored} == {1.7}
    assert {r[5] for r in floored} == {30}  # only the magnitudes at or above 1.7 are used
    # A floor below the window's own Mc changes nothing.
    assert bvalue_rows(grouped, {}, 1, min_events=20, mc_floor={"R-160_+50": -1.0}) == bvalue_rows(
        grouped, {}, 1, min_events=20
    )
