from __future__ import annotations

import math
import os
from datetime import datetime, timezone
from typing import Iterable

import psycopg2
from psycopg2.extras import execute_values
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.common.config import KafkaConfig, PostgresConfig, StreamConfig
from src.common.seismo_models import estimate_b_value, estimate_completeness_magnitude, fit_omori_utsu

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("ingestion_time", StringType(), True),
        StructField("magnitude", DoubleType(), True),
        StructField("depth_km", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("place", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("status", StringType(), True),
        StructField("tsunami", IntegerType(), True),
        StructField("significance", IntegerType(), True),
        StructField("station_count", IntegerType(), True),
        StructField("rms", DoubleType(), True),
        StructField("azimuthal_gap", DoubleType(), True),
        StructField("felt_count", IntegerType(), True),
        StructField("url", StringType(), True),
        StructField("grid_lon", IntegerType(), True),
        StructField("grid_lat", IntegerType(), True),
        StructField("cell_id", StringType(), True),
        StructField("region_id", StringType(), True),
    ]
)

EVENT_COLUMNS = [
    "event_id",
    "event_ts",
    "ingestion_ts",
    "magnitude",
    "depth_km",
    "longitude",
    "latitude",
    "place",
    "event_type",
    "status",
    "tsunami",
    "significance",
    "station_count",
    "rms",
    "azimuthal_gap",
    "felt_count",
    "url",
    "grid_lon",
    "grid_lat",
    "cell_id",
    "region_id",
]


def pg_connect(config: PostgresConfig):
    return psycopg2.connect(config.dsn)


def rows_or_empty(df: DataFrame, columns: list[str]) -> list[tuple]:
    return [tuple(row[c] for c in columns) for row in df.select(*columns).collect()]


def write_events_to_postgres(df: DataFrame, pg_config: PostgresConfig) -> int:
    rows = rows_or_empty(df, EVENT_COLUMNS)
    if not rows:
        return 0
    sql = """
        INSERT INTO seismic_events (
            event_id, event_time, ingestion_time, magnitude, depth_km, longitude, latitude,
            place, event_type, status, tsunami, significance, station_count, rms,
            azimuthal_gap, felt_count, url, grid_lon, grid_lat, cell_id, region_id
        ) VALUES %s
        ON CONFLICT (event_id) DO NOTHING
    """
    with pg_connect(pg_config) as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=1000)
    return len(rows)


def write_mainshocks(df: DataFrame, pg_config: PostgresConfig) -> int:
    mainshock_rows = rows_or_empty(
        df.filter(F.col("magnitude") >= F.lit(5.0)),
        ["event_id", "event_ts", "magnitude", "longitude", "latitude", "place"],
    )
    if not mainshock_rows:
        return 0
    sql = """
        INSERT INTO mainshocks (event_id, event_time, magnitude, longitude, latitude, place)
        VALUES %s
        ON CONFLICT (event_id) DO NOTHING
    """
    with pg_connect(pg_config) as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, mainshock_rows, page_size=500)
    return len(mainshock_rows)


def compute_anomalies(df: DataFrame, baseline_df: DataFrame, batch_id: int, cfg: StreamConfig, pg_config: PostgresConfig) -> int:
    agg = (
        df.filter(F.col("event_ts").isNotNull() & F.col("cell_id").isNotNull())
        .groupBy(F.window("event_ts", "6 hours", "1 hour"), "cell_id", "grid_lon", "grid_lat")
        .agg(F.count("*").alias("event_count"))
    )

    scored = (
        agg.join(baseline_df, on="cell_id", how="left")
        .withColumn("baseline_mean", F.coalesce(F.col("baseline_mean"), F.lit(0.5)))
        .withColumn("baseline_std", F.coalesce(F.col("baseline_std"), F.lit(1.0)))
        .withColumn("safe_std", F.when(F.col("baseline_std") < 0.1, F.lit(0.1)).otherwise(F.col("baseline_std")))
        .withColumn("z_score", (F.col("event_count") - F.col("baseline_mean")) / F.col("safe_std"))
        .filter((F.col("event_count") >= cfg.min_events_for_anomaly) & (F.col("z_score") >= cfg.z_threshold))
        .withColumn("center_lon", F.col("grid_lon") + F.lit(0.5))
        .withColumn("center_lat", F.col("grid_lat") + F.lit(0.5))
    )

    flagged_count = scored.count()
    if flagged_count == 0:
        return 0

    if flagged_count >= 2:
        k = min(cfg.max_kmeans_clusters, int(flagged_count))
        assembler = VectorAssembler(inputCols=["center_lon", "center_lat"], outputCol="features")
        feature_df = assembler.transform(scored)
        model = KMeans(k=k, seed=42, featuresCol="features", predictionCol="cluster_id").fit(feature_df)
        clustered = model.transform(feature_df)
        centers = {idx: (float(c[0]), float(c[1])) for idx, c in enumerate(model.clusterCenters())}
        anomaly_rows = []
        for row in clustered.select(
            "window",
            "cell_id",
            "grid_lon",
            "grid_lat",
            "event_count",
            "baseline_mean",
            "baseline_std",
            "z_score",
            "cluster_id",
        ).collect():
            centroid_lon, centroid_lat = centers.get(int(row["cluster_id"]), (None, None))
            anomaly_rows.append(
                (
                    batch_id,
                    row["window"].start,
                    row["window"].end,
                    row["cell_id"],
                    row["grid_lon"],
                    row["grid_lat"],
                    int(row["event_count"]),
                    float(row["baseline_mean"]),
                    float(row["baseline_std"]),
                    float(row["z_score"]),
                    int(row["cluster_id"]),
                    centroid_lon,
                    centroid_lat,
                )
            )
    else:
        row = scored.collect()[0]
        anomaly_rows = [
            (
                batch_id,
                row["window"].start,
                row["window"].end,
                row["cell_id"],
                row["grid_lon"],
                row["grid_lat"],
                int(row["event_count"]),
                float(row["baseline_mean"]),
                float(row["baseline_std"]),
                float(row["z_score"]),
                0,
                float(row["center_lon"]),
                float(row["center_lat"]),
            )
        ]

    sql = """
        INSERT INTO anomaly_windows (
            batch_id, window_start, window_end, cell_id, grid_lon, grid_lat, event_count,
            baseline_mean, baseline_std, z_score, cluster_id, centroid_lon, centroid_lat
        ) VALUES %s
        ON CONFLICT (batch_id, window_start, window_end, cell_id) DO NOTHING
    """
    with pg_connect(pg_config) as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, anomaly_rows, page_size=500)
    return len(anomaly_rows)


def load_long_term_bvalues(path: str) -> dict[str, float]:
    if not os.path.exists(path):
        return {}
    result: dict[str, float] = {}
    with open(path, "r", encoding="utf-8") as f:
        header = f.readline().strip().split(",")
        try:
            region_idx = header.index("region_id")
            b_idx = header.index("long_term_b")
        except ValueError:
            return {}
        for line in f:
            parts = line.strip().split(",")
            if len(parts) <= max(region_idx, b_idx):
                continue
            try:
                result[parts[region_idx]] = float(parts[b_idx])
            except ValueError:
                continue
    return result


def compute_bvalues(df: DataFrame, batch_id: int, pg_config: PostgresConfig) -> int:
    region_b_path = os.getenv("REGION_BVALUE_CSV", "/opt/workspace/data/baseline/region_bvalues.csv")
    long_term = load_long_term_bvalues(region_b_path)
    grouped = (
        df.filter(F.col("magnitude").isNotNull() & F.col("event_ts").isNotNull() & F.col("region_id").isNotNull())
        .groupBy(F.window("event_ts", "24 hours", "1 hour"), "region_id")
        .agg(F.collect_list("magnitude").alias("magnitudes"), F.count("*").alias("event_count"))
    )

    rows = []
    for row in grouped.collect():
        magnitudes = [float(m) for m in row["magnitudes"] if m is not None]
        if len(magnitudes) < 5:
            continue
        mc = estimate_completeness_magnitude(magnitudes)
        if mc is None:
            continue
        b_value, b_sigma, n_above = estimate_b_value(magnitudes, mc)
        if b_value is None or b_sigma is None:
            continue
        long_b = long_term.get(row["region_id"], 1.0)
        deviation = b_value - long_b
        is_alert = bool(abs(deviation) > max(0.25, 2.0 * b_sigma) and n_above >= 5)
        rows.append(
            (
                batch_id,
                row["window"].start,
                row["window"].end,
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

    if not rows:
        return 0
    sql = """
        INSERT INTO bvalue_windows (
            batch_id, window_start, window_end, region_id, event_count, completeness_mag,
            b_value, b_sigma, long_term_b, deviation, is_alert
        ) VALUES %s
        ON CONFLICT (batch_id, window_start, window_end, region_id) DO NOTHING
    """
    with pg_connect(pg_config) as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=500)
    return len(rows)


def haversine_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    radius = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    return 2 * radius * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def compute_aftershock_forecasts(batch_id: int, pg_config: PostgresConfig) -> int:
    inserted = 0
    with pg_connect(pg_config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE mainshocks
                SET active = FALSE
                WHERE active = TRUE AND event_time < now() - interval '30 days'
                """
            )
            cur.execute(
                """
                SELECT event_id, event_time, magnitude, longitude, latitude, place
                FROM mainshocks
                WHERE active = TRUE
                ORDER BY event_time DESC
                LIMIT 20
                """
            )
            mainshocks = cur.fetchall()

            forecast_rows = []
            for main_id, main_time, main_mag, main_lon, main_lat, _place in mainshocks:
                cur.execute(
                    """
                    SELECT event_time, magnitude, longitude, latitude
                    FROM seismic_events
                    WHERE event_time > %s
                      AND event_time <= %s + interval '7 days'
                      AND magnitude < %s
                      AND longitude BETWEEN %s AND %s
                      AND latitude BETWEEN %s AND %s
                    ORDER BY event_time ASC
                    """,
                    (main_time, main_time, main_mag, main_lon - 4.0, main_lon + 4.0, main_lat - 4.0, main_lat + 4.0),
                )
                hours_after = []
                for event_time, _mag, lon, lat in cur.fetchall():
                    if haversine_km(float(main_lon), float(main_lat), float(lon), float(lat)) <= 300.0:
                        hours = (event_time - main_time).total_seconds() / 3600.0
                        if hours > 0:
                            hours_after.append(hours)
                fit = fit_omori_utsu(hours_after)
                if fit is None:
                    continue
                forecast_rows.append(
                    (
                        batch_id,
                        main_id,
                        fit.aftershock_count,
                        fit.k_value,
                        fit.c_value,
                        fit.p_value,
                        fit.next_hour_rate,
                        fit.next_six_hour_expected,
                        fit.r_squared,
                    )
                )

            if forecast_rows:
                execute_values(
                    cur,
                    """
                    INSERT INTO aftershock_forecasts (
                        batch_id, mainshock_id, aftershock_count, k_value, c_value, p_value,
                        next_hour_rate, next_six_hour_expected, r_squared
                    ) VALUES %s
                    ON CONFLICT (batch_id, mainshock_id) DO NOTHING
                    """,
                    forecast_rows,
                    page_size=100,
                )
                inserted = len(forecast_rows)
    return inserted


def process_batch(batch_df: DataFrame, batch_id: int, baseline_df: DataFrame, cfg: StreamConfig, pg_config: PostgresConfig) -> None:
    if batch_df.rdd.isEmpty():
        print(f"Batch {batch_id}: empty")
        return

    df = batch_df.dropDuplicates(["event_id"]).cache()
    event_count = write_events_to_postgres(df, pg_config)
    mainshock_count = write_mainshocks(df, pg_config)
    anomaly_count = compute_anomalies(df, baseline_df, batch_id, cfg, pg_config)
    bvalue_count = compute_bvalues(df, batch_id, pg_config)
    forecast_count = compute_aftershock_forecasts(batch_id, pg_config)
    df.unpersist()
    print(
        f"Batch {batch_id}: events={event_count}, mainshocks={mainshock_count}, "
        f"anomalies={anomaly_count}, bvalues={bvalue_count}, forecasts={forecast_count}",
        flush=True,
    )


def build_spark() -> SparkSession:
    master = os.getenv("SPARK_MASTER_URL", "local[*]")
    return (
        SparkSession.builder.appName("RealTimeSeismicActivityAnalysis")
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8"))
        .getOrCreate()
    )


def main() -> int:
    cfg = StreamConfig()
    kafka_config = KafkaConfig()
    pg_config = PostgresConfig()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    baseline_schema = StructType(
        [
            StructField("cell_id", StringType(), False),
            StructField("baseline_mean", DoubleType(), True),
            StructField("baseline_std", DoubleType(), True),
            StructField("historical_events", IntegerType(), True),
        ]
    )
    baseline_df = spark.read.schema(baseline_schema).option("header", True).csv(cfg.baseline_csv).cache()

    kafka_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_config.bootstrap_servers)
        .option("subscribe", kafka_config.topic)
        .option("startingOffsets", kafka_config.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    raw = kafka_raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event")).select("event.*")
    stream_df = (
        raw.withColumn("event_ts", F.to_timestamp("event_time"))
        .withColumn("ingestion_ts", F.coalesce(F.to_timestamp("ingestion_time"), F.current_timestamp()))
        .withColumn("grid_lon", F.coalesce(F.col("grid_lon"), F.floor(F.col("longitude")).cast("int")))
        .withColumn("grid_lat", F.coalesce(F.col("grid_lat"), F.floor(F.col("latitude")).cast("int")))
        .withColumn("cell_id", F.coalesce(F.col("cell_id"), F.concat_ws(":", F.col("grid_lon"), F.col("grid_lat"))))
        .withColumn(
            "region_id",
            F.coalesce(
                F.col("region_id"),
                F.concat(
                    F.lit("R"),
                    (F.floor(F.col("longitude") / 10) * 10).cast("int"),
                    F.lit("_"),
                    (F.floor(F.col("latitude") / 10) * 10).cast("int"),
                ),
            ),
        )
        .filter(F.col("event_id").isNotNull() & F.col("event_ts").isNotNull())
        .withWatermark("event_ts", "7 days")
        .dropDuplicates(["event_id"])
    )

    query = (
        stream_df.writeStream.foreachBatch(lambda df, bid: process_batch(df, bid, baseline_df, cfg, pg_config))
        .outputMode("append")
        .option("checkpointLocation", cfg.checkpoint_dir)
        .trigger(processingTime=os.getenv("TRIGGER_INTERVAL", "10 seconds"))
        .start()
    )

    print(
        f"Streaming from Kafka topic={kafka_config.topic} at {kafka_config.bootstrap_servers}; "
        f"checkpoint={cfg.checkpoint_dir}",
        flush=True,
    )
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
