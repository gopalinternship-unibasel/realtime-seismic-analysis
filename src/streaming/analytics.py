"""Window and clustering logic used by the streaming job.

The functions here take plain DataFrames so they can be unit-tested with a
local SparkSession.  The streaming job feeds them two inputs per micro-batch:

* ``batch_events``  - the de-duplicated events of the current micro-batch;
* ``recent_events`` - every stored event in the time span the batch touches,
  read back from PostgreSQL.

Sliding windows are recomputed from ``recent_events`` and then restricted to
the windows that ``batch_events`` falls into.  A window therefore counts every
event that belongs to it, no matter in which micro-batch the event arrived,
and late events update the window they belong to.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.geo import haversine_km, lonlat_to_unit_vector, unit_vector_to_lonlat
from src.common.seismo_models import estimate_b_value, estimate_completeness_magnitude

ANOMALY_WINDOW = "6 hours"
ANOMALY_SLIDE = "1 hour"
BVALUE_WINDOW = "24 hours"
BVALUE_SLIDE = "1 hour"
# Longest window above; events this far before/after a batch can share a window with it.
MAX_WINDOW_HOURS = 24

DEFAULT_BASELINE_MEAN = 0.5
DEFAULT_BASELINE_STD = 1.0
MIN_BASELINE_STD = 0.1


def region_id_expr(lon: Column, lat: Column) -> Column:
    """Spark version of ``src.common.geo.coarse_region`` (same text, e.g. ``R-130_+30``)."""
    return F.format_string(
        "R%+04d_%+03d",
        (F.floor(lon / 10) * 10).cast("int"),
        (F.floor(lat / 10) * 10).cast("int"),
    )


def cell_id_expr(grid_lon: Column, grid_lat: Column) -> Column:
    return F.concat_ws(":", grid_lon, grid_lat)


def is_earthquake(event_type: Column) -> Column:
    """Analytics use earthquakes only; quarry blasts, explosions and ice quakes are stored but not scored."""
    return F.coalesce(F.lower(event_type), F.lit("earthquake")) == F.lit("earthquake")


def _windowed(df: DataFrame, window: str, slide: str) -> DataFrame:
    return df.withColumn("w", F.window("event_ts", window, slide)).withColumn(
        "window_start", F.col("w.start")
    ).withColumn("window_end", F.col("w.end")).drop("w")


def touched_windows(batch_events: DataFrame, key: str, window: str, slide: str) -> DataFrame:
    """Distinct (source, window, key) combinations that the current micro-batch contributes to."""
    return (
        _windowed(batch_events.filter(F.col("event_ts").isNotNull() & F.col(key).isNotNull()), window, slide)
        .select("source", "window_start", "window_end", key)
        .distinct()
    )


def sliding_cell_counts(recent_events: DataFrame, batch_events: DataFrame) -> DataFrame:
    counts = (
        _windowed(recent_events.filter(F.col("event_ts").isNotNull() & F.col("cell_id").isNotNull()), ANOMALY_WINDOW, ANOMALY_SLIDE)
        .groupBy("source", "window_start", "window_end", "cell_id", "grid_lon", "grid_lat")
        .agg(F.count("*").alias("event_count"))
    )
    touched = touched_windows(batch_events, "cell_id", ANOMALY_WINDOW, ANOMALY_SLIDE)
    return counts.join(touched, ["source", "window_start", "window_end", "cell_id"], "left_semi")


def score_anomalies(counts: DataFrame, baseline_df: DataFrame, z_threshold: float, min_events: int) -> DataFrame:
    """z = (count - historical mean) / historical std for each 6-hour cell window; keep the flagged ones."""
    return (
        counts.join(F.broadcast(baseline_df.select("cell_id", "baseline_mean", "baseline_std")), on="cell_id", how="left")
        .withColumn("baseline_mean", F.coalesce(F.col("baseline_mean"), F.lit(DEFAULT_BASELINE_MEAN)))
        .withColumn("baseline_std", F.coalesce(F.col("baseline_std"), F.lit(DEFAULT_BASELINE_STD)))
        .withColumn(
            "safe_std",
            F.when(F.col("baseline_std") < MIN_BASELINE_STD, F.lit(MIN_BASELINE_STD)).otherwise(F.col("baseline_std")),
        )
        .withColumn("z_score", (F.col("event_count") - F.col("baseline_mean")) / F.col("safe_std"))
        .filter((F.col("event_count") >= min_events) & (F.col("z_score") >= z_threshold))
        .drop("safe_std")
    )


def anomaly_windows(
    recent_events: DataFrame, batch_events: DataFrame, baseline_df: DataFrame, z_threshold: float, min_events: int
) -> DataFrame:
    return score_anomalies(sliding_cell_counts(recent_events, batch_events), baseline_df, z_threshold, min_events)


def bvalue_window_magnitudes(recent_events: DataFrame, batch_events: DataFrame) -> DataFrame:
    grouped = (
        _windowed(
            recent_events.filter(
                F.col("magnitude").isNotNull() & F.col("event_ts").isNotNull() & F.col("region_id").isNotNull()
            ),
            BVALUE_WINDOW,
            BVALUE_SLIDE,
        )
        .groupBy("source", "window_start", "window_end", "region_id")
        .agg(F.collect_list("magnitude").alias("magnitudes"), F.count("*").alias("event_count"))
    )
    touched = touched_windows(
        batch_events.filter(F.col("magnitude").isNotNull()), "region_id", BVALUE_WINDOW, BVALUE_SLIDE
    )
    return grouped.join(touched, ["source", "window_start", "window_end", "region_id"], "left_semi")


def bvalue_rows(
    grouped_rows: Iterable, long_term: dict[str, float], batch_id: int, min_events: int = 20
) -> list[tuple]:
    """Gutenberg-Richter b-value (Mc by maximum curvature, b by maximum likelihood) per window.

    Windows with fewer than ``min_events`` magnitudes at or above Mc are skipped: with
    sigma = b / sqrt(n), five events give sigma of about half of b and b-values of 3 to 8
    that would raise false alerts.
    """
    rows = []
    for row in grouped_rows:
        magnitudes = [float(m) for m in row["magnitudes"] if m is not None]
        if len(magnitudes) < max(5, min_events):
            continue
        mc = estimate_completeness_magnitude(magnitudes)
        if mc is None:
            continue
        b_value, b_sigma, n_above = estimate_b_value(magnitudes, mc)
        if b_value is None or b_sigma is None or n_above < min_events:
            continue
        long_b = long_term.get(row["region_id"], 1.0)
        deviation = b_value - long_b
        is_alert = bool(abs(deviation) > max(0.25, 2.0 * b_sigma) and n_above >= 5)
        rows.append(
            (
                batch_id,
                row["source"],
                row["window_start"],
                row["window_end"],
                row["region_id"],
                int(n_above),
                float(mc),
                float(b_value),
                float(b_sigma),
                float(long_b),
                float(deviation),
                is_alert,
            )
        )
    return rows


@dataclass
class ActiveRegion:
    cluster_id: int
    centroid_lon: float
    centroid_lat: float
    cells: list[str]
    max_z_score: float
    max_event_count: int
    radius_km: float


def _centroid(vectors: list[tuple[float, float, float]]) -> tuple[float, float]:
    x = sum(v[0] for v in vectors) / len(vectors)
    y = sum(v[1] for v in vectors) / len(vectors)
    z = sum(v[2] for v in vectors) / len(vectors)
    if math.sqrt(x * x + y * y + z * z) == 0.0:
        x, y, z = vectors[0]
    return unit_vector_to_lonlat(x, y, z)


def greedy_region_count(points: list[tuple], radius_km: float) -> int:
    """Leader clustering: how many centres of radius ``radius_km`` cover the cells (strongest first).

    Used only to pick the K-Means k to try first, so most micro-batches need one K-Means fit.
    """
    leaders: list[tuple[float, float]] = []
    for _cell_id, lon, lat, _vec in points:
        if not any(haversine_km(lon, lat, l_lon, l_lat) <= radius_km for l_lon, l_lat in leaders):
            leaders.append((lon, lat))
    return max(1, len(leaders))


def cluster_active_cells(
    spark: SparkSession,
    cells: list[dict],
    max_k: int = 5,
    radius_km: float = 300.0,
    seed: int = 42,
) -> tuple[dict[str, int], list[ActiveRegion]]:
    """Group anomalous cells into active regions with Spark MLlib K-Means.

    Cells are clustered on the unit sphere (so the dateline does not split a
    region).  k starts at a greedy estimate of how many regions of ``radius_km``
    cover the cells and grows (up to ``max_k``) until every cell lies within
    ``radius_km`` of its cluster centroid.  Returns (cell_id -> cluster_id,
    regions ordered by their strongest z-score).
    """
    if not cells:
        return {}, []

    from pyspark.ml.clustering import KMeans
    from pyspark.ml.linalg import Vectors, VectorUDT
    from pyspark.sql.types import StringType, StructField, StructType

    info = {c["cell_id"]: c for c in cells}
    ordered = sorted(cells, key=lambda c: (-float(c["max_z_score"]), c["cell_id"]))
    points = []
    for cell in ordered:
        lon, lat = float(cell["center_lon"]), float(cell["center_lat"])
        points.append((cell["cell_id"], lon, lat, lonlat_to_unit_vector(lon, lat)))
    distinct_points = len({p[3] for p in points})
    k_cap = max(1, min(int(max_k), distinct_points))

    def build(assignment: dict[str, int]) -> list[ActiveRegion]:
        by_cluster: dict[int, list] = {}
        for cell_id, lon, lat, vec in points:
            by_cluster.setdefault(assignment[cell_id], []).append((cell_id, lon, lat, vec))
        regions = []
        for members in by_cluster.values():
            c_lon, c_lat = _centroid([m[3] for m in members])
            regions.append(
                ActiveRegion(
                    cluster_id=-1,
                    centroid_lon=c_lon,
                    centroid_lat=c_lat,
                    cells=sorted(m[0] for m in members),
                    max_z_score=max(float(info[m[0]]["max_z_score"]) for m in members),
                    max_event_count=max(int(info[m[0]]["max_event_count"]) for m in members),
                    radius_km=max(haversine_km(c_lon, c_lat, m[1], m[2]) for m in members),
                )
            )
        regions.sort(key=lambda r: (-r.max_z_score, r.centroid_lon, r.centroid_lat))
        for idx, region in enumerate(regions):
            region.cluster_id = idx
        return regions

    def fits(regions: list[ActiveRegion]) -> bool:
        return max(r.radius_km for r in regions) <= radius_km

    k = min(k_cap, greedy_region_count(points, radius_km))
    best = build({p[0]: 0 for p in points})
    if k == 1 and fits(best) or k_cap == 1:
        return {cell: 0 for region in best for cell in region.cells}, best

    feature_schema = StructType([StructField("cell_id", StringType(), False), StructField("features", VectorUDT(), False)])
    feature_df = spark.createDataFrame([(p[0], Vectors.dense(list(p[3]))) for p in points], feature_schema).cache()
    for k in range(max(2, k), k_cap + 1):
        model = KMeans(k=k, seed=seed, featuresCol="features", predictionCol="cluster").fit(feature_df)
        centers = [tuple(float(x) for x in c) for c in model.clusterCenters()]
        # Same rule as model.transform (nearest centre), without another Spark job.
        assignment = {
            cell_id: min(range(len(centers)), key=lambda i: sum((a - b) ** 2 for a, b in zip(vec, centers[i])))
            for cell_id, _lon, _lat, vec in points
        }
        best = build(assignment)
        if fits(best):
            break
    feature_df.unpersist()
    return {cell: region.cluster_id for region in best for cell in region.cells}, best
