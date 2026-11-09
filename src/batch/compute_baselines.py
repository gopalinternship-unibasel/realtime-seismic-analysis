from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from src.common.seismo_models import estimate_b_value, estimate_completeness_magnitude

HISTORICAL_CSV = os.getenv("HISTORICAL_CSV", "/opt/workspace/data/historical/sample_historical_events.csv")
OUT_BASELINE = os.getenv("OUT_BASELINE", "/opt/workspace/data/baseline/cell_baselines.csv")
OUT_REGION_B = os.getenv("OUT_REGION_B", "/opt/workspace/data/baseline/region_bvalues.csv")


def build_spark() -> SparkSession:
    master = os.getenv("SPARK_MASTER_URL", "local[*]")
    return (
        SparkSession.builder.appName("SeismicHistoricalBaselines")
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def main() -> int:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    schema = StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("event_time", TimestampType(), False),
            StructField("magnitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("depth_km", DoubleType(), True),
        ]
    )
    df = spark.read.option("header", True).schema(schema).csv(HISTORICAL_CSV)
    enriched = (
        df.filter(F.col("event_time").isNotNull() & F.col("longitude").isNotNull() & F.col("latitude").isNotNull())
        .withColumn("grid_lon", F.floor("longitude").cast("int"))
        .withColumn("grid_lat", F.floor("latitude").cast("int"))
        .withColumn("cell_id", F.concat_ws(":", F.col("grid_lon"), F.col("grid_lat")))
        .withColumn(
            "region_id",
            F.concat(
                F.lit("R"),
                (F.floor(F.col("longitude") / 10) * 10).cast("int"),
                F.lit("_"),
                (F.floor(F.col("latitude") / 10) * 10).cast("int"),
            ),
        )
    )

    six_hour_counts = (
        enriched.groupBy(F.window("event_time", "6 hours"), "cell_id")
        .agg(F.count("*").alias("window_count"))
    )
    baselines = (
        six_hour_counts.groupBy("cell_id")
        .agg(
            F.avg("window_count").alias("baseline_mean"),
            F.stddev_pop("window_count").alias("baseline_std"),
            F.sum("window_count").cast("int").alias("historical_events"),
        )
        .withColumn("baseline_std", F.coalesce(F.col("baseline_std"), F.lit(1.0)))
    )
    baselines.coalesce(1).write.mode("overwrite").option("header", True).csv(OUT_BASELINE + ".tmp")

    # Save a single CSV file instead of Spark's part-* directory for simpler Docker mounting.
    tmp_path = OUT_BASELINE + ".tmp"
    local_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    tmp = spark._jvm.org.apache.hadoop.fs.Path(tmp_path)
    out = spark._jvm.org.apache.hadoop.fs.Path(OUT_BASELINE)
    for status in local_fs.listStatus(tmp):
        name = status.getPath().getName()
        if name.startswith("part-") and name.endswith(".csv"):
            if local_fs.exists(out):
                local_fs.delete(out, False)
            local_fs.rename(status.getPath(), out)
            break
    local_fs.delete(tmp, True)

    region_rows = []
    for row in enriched.groupBy("region_id").agg(F.collect_list("magnitude").alias("magnitudes")).collect():
        magnitudes = [float(m) for m in row["magnitudes"] if m is not None]
        mc = estimate_completeness_magnitude(magnitudes)
        if mc is None:
            continue
        b_value, _sigma, n = estimate_b_value(magnitudes, mc)
        if b_value is not None:
            region_rows.append((row["region_id"], float(b_value), int(n), float(mc)))

    region_schema = "region_id string, long_term_b double, event_count int, completeness_mag double"
    spark.createDataFrame(region_rows, region_schema).coalesce(1).write.mode("overwrite").option("header", True).csv(
        OUT_REGION_B + ".tmp"
    )
    tmp_path = OUT_REGION_B + ".tmp"
    tmp = spark._jvm.org.apache.hadoop.fs.Path(tmp_path)
    out = spark._jvm.org.apache.hadoop.fs.Path(OUT_REGION_B)
    for status in local_fs.listStatus(tmp):
        name = status.getPath().getName()
        if name.startswith("part-") and name.endswith(".csv"):
            if local_fs.exists(out):
                local_fs.delete(out, False)
            local_fs.rename(status.getPath(), out)
            break
    local_fs.delete(tmp, True)

    print(f"Wrote cell baselines to {OUT_BASELINE}")
    print(f"Wrote regional b-values to {OUT_REGION_B}")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
