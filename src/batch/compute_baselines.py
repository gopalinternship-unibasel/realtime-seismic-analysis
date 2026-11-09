"""Spark batch job: historical baselines for the streaming anomaly and b-value monitors.

Input: a historical catalogue (``fetch_usgs_history.py`` writes one from the USGS
FDSN service) with columns event_id,event_time,magnitude,longitude,latitude,depth_km[,event_type].

Outputs:
* ``cell_baselines.csv``  - per 1 x 1 degree cell, mean and standard deviation of the
  number of earthquakes per 6-hour window over the whole period. Windows with no
  events count as zero, so the mean is (events in cell) / (6-hour windows in period).
* ``region_bvalues.csv``  - per 10 x 10 degree region, the long-term Gutenberg-Richter
  b-value (Mc by maximum curvature, b by maximum likelihood).
"""

from __future__ import annotations

import csv
import json
import math
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from src.common.seismo_models import estimate_b_value, estimate_completeness_magnitude
from src.streaming.analytics import cell_id_expr, grid_lon_expr, region_id_expr

HISTORICAL_CSV = os.getenv("HISTORICAL_CSV", "/opt/workspace/data/historical/usgs_historical_events.csv")
OUT_BASELINE = os.getenv("OUT_BASELINE", "/opt/workspace/data/baseline/cell_baselines.csv")
OUT_REGION_B = os.getenv("OUT_REGION_B", "/opt/workspace/data/baseline/region_bvalues.csv")
OUT_META = os.getenv("OUT_BASELINE_META", "/opt/workspace/data/baseline/baseline_metadata.json")
WINDOW_HOURS = 6
MIN_REGION_EVENTS = int(os.getenv("MIN_REGION_EVENTS", "50"))

HISTORICAL_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("magnitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("depth_km", DoubleType(), True),
        StructField("event_type", StringType(), True),
    ]
)


def build_spark() -> SparkSession:
    master = os.getenv("SPARK_MASTER_URL", "local[*]")
    return (
        SparkSession.builder.appName("SeismicHistoricalBaselines")
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_period(csv_path: str, df: DataFrame) -> tuple[datetime, datetime]:
    """Catalogue period from the download's metadata file, else from the first/last event."""
    meta = Path(csv_path).with_name(Path(csv_path).stem + ".meta.json")
    if meta.exists():
        info = json.loads(meta.read_text(encoding="utf-8"))
        return datetime.fromisoformat(info["period_start"]), datetime.fromisoformat(info["period_end"])
    row = df.agg(F.min("event_time").alias("lo"), F.max("event_time").alias("hi")).first()
    return row["lo"].replace(tzinfo=timezone.utc), row["hi"].replace(tzinfo=timezone.utc)


def window_count(start: datetime, end: datetime, window_hours: int = WINDOW_HOURS) -> int:
    return max(1, math.ceil((end - start) / timedelta(hours=window_hours)))


def enrich(df: DataFrame) -> DataFrame:
    return (
        df.filter(
            F.col("event_id").isNotNull()
            & F.col("event_time").isNotNull()
            & F.col("longitude").between(-180.0, 180.0)
            & F.col("latitude").between(-90.0, 90.0)
            & (F.coalesce(F.lower(F.col("event_type")), F.lit("earthquake")) == "earthquake")
        )
        .dropDuplicates(["event_id"])
        .withColumn("grid_lon", grid_lon_expr(F.col("longitude")))
        .withColumn("grid_lat", F.floor("latitude").cast("int"))
        .withColumn("cell_id", cell_id_expr(F.col("grid_lon"), F.col("grid_lat")))
        .withColumn("region_id", region_id_expr(F.col("longitude"), F.col("latitude")))
    )


def cell_baselines(enriched: DataFrame, n_windows: int) -> DataFrame:
    """Mean/std of 6-hour counts per cell, counting the windows without events as zeros."""
    per_window = enriched.groupBy(F.window("event_time", f"{WINDOW_HOURS} hours"), "cell_id").agg(
        F.count("*").alias("c")
    )
    return (
        per_window.groupBy("cell_id")
        .agg(F.sum("c").alias("s"), F.sum(F.col("c") * F.col("c")).alias("q"))
        .withColumn("baseline_mean", F.col("s") / F.lit(float(n_windows)))
        .withColumn(
            "baseline_std",
            F.sqrt(F.greatest(F.col("q") / F.lit(float(n_windows)) - F.col("baseline_mean") ** 2, F.lit(0.0))),
        )
        .select(
            "cell_id",
            F.round("baseline_mean", 6).alias("baseline_mean"),
            F.round("baseline_std", 6).alias("baseline_std"),
            F.col("s").cast("int").alias("historical_events"),
        )
        .orderBy("cell_id")
    )


def region_bvalues(enriched: DataFrame, min_events: int = MIN_REGION_EVENTS) -> list[tuple]:
    rows = []
    for row in enriched.groupBy("region_id").agg(F.collect_list("magnitude").alias("magnitudes")).collect():
        magnitudes = [float(m) for m in row["magnitudes"] if m is not None]
        mc = estimate_completeness_magnitude(magnitudes)
        if mc is None:
            continue
        b_value, _sigma, n = estimate_b_value(magnitudes, mc)
        if b_value is not None and n >= min_events:
            rows.append((row["region_id"], float(b_value), int(n), float(mc)))
    return sorted(rows)


def write_csv(path: str, header: list[str], rows: list) -> None:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)
    tmp.replace(out)


def main() -> int:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    df = spark.read.option("header", True).schema(HISTORICAL_SCHEMA).csv(HISTORICAL_CSV)
    enriched = enrich(df).cache()
    start, end = read_period(HISTORICAL_CSV, enriched)
    n_windows = window_count(start, end)

    baselines = [tuple(r) for r in cell_baselines(enriched, n_windows).collect()]
    write_csv(OUT_BASELINE, ["cell_id", "baseline_mean", "baseline_std", "historical_events"], baselines)

    regions = region_bvalues(enriched)
    write_csv(OUT_REGION_B, ["region_id", "long_term_b", "event_count", "completeness_mag"], regions)

    Path(OUT_META).write_text(
        json.dumps(
            {
                "historical_csv": os.path.basename(HISTORICAL_CSV),
                "period_start": start.isoformat(),
                "period_end": end.isoformat(),
                "window_hours": WINDOW_HOURS,
                "windows_in_period": n_windows,
                "events": enriched.count(),
                "cells": len(baselines),
                "regions_with_b_value": len(regions),
                "min_region_events": MIN_REGION_EVENTS,
                "computed_at": datetime.now(tz=timezone.utc).isoformat(timespec="seconds"),
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"Wrote {len(baselines)} cell baselines to {OUT_BASELINE} ({n_windows} six-hour windows)")
    print(f"Wrote {len(regions)} regional b-values to {OUT_REGION_B}")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
