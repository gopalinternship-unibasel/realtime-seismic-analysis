from __future__ import annotations

import json
import math
import os
import time
from collections import Counter
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.common.config import KafkaConfig, PostgresConfig, StreamConfig
from src.common.seismo_models import (
    AFTERSHOCK_RADIUS_KM,
    AFTERSHOCK_WINDOW_DAYS,
    aftershock_hours,
    find_larger_events,
    fit_omori_utsu,
    search_box_params,
)
from src.streaming.analytics import (
    MAX_WINDOW_HOURS,
    anomaly_windows,
    bvalue_rows,
    bvalue_window_magnitudes,
    cell_id_expr,
    cluster_active_cells,
    is_earthquake,
    region_id_expr,
)

# Fields of a producer record. Every field is read as text and converted in clean_events(): a
# numeric field declared as a double in from_json() turned a number sent as text ("3.0") into
# NULL without a trace, and a value that is no number at all was indistinguishable from a
# missing one. Now numbers sent as text are accepted, and anything else is rejected with a reason.
DOUBLE_FIELDS = ("magnitude", "depth_km", "longitude", "latitude", "rms", "azimuthal_gap")
INT_FIELDS = ("tsunami", "significance", "station_count", "felt_count", "grid_lon", "grid_lat")
RECORD_FIELDS = (
    "event_id",
    "event_time",
    "ingestion_time",
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
    "source",
)
EVENT_SCHEMA = StructType([StructField(name, StringType(), True) for name in RECORD_FIELDS])

# Columns of a Kafka message the pipeline keeps (see kafka_messages()).
MESSAGE_SCHEMA = "value string, kafka_partition int, kafka_offset long, kafka_ts timestamp"

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
    "source",
]

SCHEMA_SQL = Path(os.getenv("SCHEMA_SQL", "/opt/workspace/sql/init.sql"))


def pg_connect(config: PostgresConfig):
    return psycopg2.connect(config.dsn)


def ensure_schema(pg_config: PostgresConfig, wait_seconds: int = 90) -> None:
    """Apply sql/init.sql (idempotent) so older volumes get the new columns and keys."""
    if not SCHEMA_SQL.exists():
        print(f"Schema file {SCHEMA_SQL} not found; assuming the database is initialised", flush=True)
        return
    deadline = time.time() + wait_seconds
    while True:
        try:
            with closing(pg_connect(pg_config)) as conn, conn, conn.cursor() as cur:
                cur.execute(SCHEMA_SQL.read_text(encoding="utf-8"))
            return
        except psycopg2.OperationalError as exc:
            if time.time() > deadline:
                raise
            print(f"PostgreSQL not ready yet ({exc.__class__.__name__}); retrying...", flush=True)
            time.sleep(3)


def is_earthquake_type(event_type: str | None) -> bool:
    return (event_type or "earthquake").lower() == "earthquake"


def write_event_rows(rows: list, pg_config: PostgresConfig) -> int:
    """Insert new events; returns how many were not already stored (idempotent on event_id)."""
    if not rows:
        return 0
    sql = """
        INSERT INTO seismic_events (
            event_id, event_time, ingestion_time, magnitude, depth_km, longitude, latitude,
            place, event_type, status, tsunami, significance, station_count, rms,
            azimuthal_gap, felt_count, url, grid_lon, grid_lat, cell_id, region_id, source
        ) VALUES %s
        ON CONFLICT (event_id) DO NOTHING
        RETURNING event_id
    """
    values = [tuple(row[c] for c in EVENT_COLUMNS) for row in rows]
    with closing(pg_connect(pg_config)) as conn, conn, conn.cursor() as cur:
        inserted = execute_values(cur, sql, values, page_size=1000, fetch=True)
    return len(inserted)


def write_mainshock_rows(rows: list, pg_config: PostgresConfig, min_magnitude: float) -> int:
    mainshock_rows = [
        (r["event_id"], r["event_ts"], r["magnitude"], r["longitude"], r["latitude"], r["place"], r["source"])
        for r in rows
        if r["magnitude"] is not None and r["magnitude"] >= min_magnitude and is_earthquake_type(r["event_type"])
    ]
    if not mainshock_rows:
        return 0
    sql = """
        INSERT INTO mainshocks (event_id, event_time, magnitude, longitude, latitude, place, source)
        VALUES %s
        ON CONFLICT (event_id) DO NOTHING
    """
    with closing(pg_connect(pg_config)) as conn, conn, conn.cursor() as cur:
        execute_values(cur, sql, mainshock_rows, page_size=500)
    return len(mainshock_rows)


REJECTED_COLUMNS = ["kafka_partition", "kafka_offset", "kafka_ts", "reject_reason", "event_id", "source", "value"]
# Longest message text kept in rejected_records.
REJECTED_VALUE_CHARS = 2000


def write_rejected_rows(rows: list, batch_id: int, pg_config: PostgresConfig) -> int:
    """Store the records the cleaning rules rejected, one row per Kafka message (idempotent on its offset)."""
    if not rows:
        return 0
    values = [
        (
            r["kafka_partition"],
            r["kafka_offset"],
            r["kafka_ts"],
            batch_id,
            r["reject_reason"],
            r["event_id"],
            r["source"],
            None if r["value"] is None else r["value"][:REJECTED_VALUE_CHARS],
        )
        for r in rows
    ]
    sql = """
        INSERT INTO rejected_records (
            kafka_partition, kafka_offset, received_at, batch_id, reason, event_id, source, raw_value
        ) VALUES %s
        ON CONFLICT (kafka_partition, kafka_offset) DO UPDATE SET
            received_at = EXCLUDED.received_at, batch_id = EXCLUDED.batch_id, reason = EXCLUDED.reason,
            event_id = EXCLUDED.event_id, source = EXCLUDED.source, raw_value = EXCLUDED.raw_value,
            rejected_at = now()
    """
    with closing(pg_connect(pg_config)) as conn, conn, conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=500)
    return len(values)


def reason_summary(reasons) -> str:
    """'2 missing event_time, 1 not a JSON object' (most frequent first)."""
    counts = Counter(reasons)
    return ", ".join(f"{n} {reason}" for reason, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])))


def utc_iso(value: datetime) -> str:
    """ISO text with an explicit +00:00 offset for SQL.

    PySpark's collect() returns naive datetimes in the driver's local time zone, which is UTC
    in the containers (no TZ is set). Without the offset, PostgreSQL would read the literal in
    the JDBC session's time zone (the JVM default), and psycopg2 writes go through the
    TimeZone=UTC session option in PostgresConfig.dsn.
    """
    value = value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
    return value.isoformat()


def merge_time_ranges(times: list[datetime], pad: timedelta) -> list[tuple[datetime, datetime]]:
    """The union of [t - pad, t + pad] over ``times``, as sorted, non-overlapping ranges.

    A micro-batch can hold events days or months apart (a late event next to live ones, or the
    replay without REPLAY_REBASE_TO_NOW beside the live feed). One range from the oldest to the
    newest event would read back every stored event in between; these ranges read only the
    events that can share a window with the batch.
    """
    ranges: list[list[datetime]] = []
    for t in sorted(times):
        lo, hi = t - pad, t + pad
        if ranges and lo <= ranges[-1][1]:
            ranges[-1][1] = max(ranges[-1][1], hi)
        else:
            ranges.append([lo, hi])
    return [(lo, hi) for lo, hi in ranges]


def load_stored_events(spark: SparkSession, pg_config: PostgresConfig, ranges: list[tuple[datetime, datetime]]) -> DataFrame:
    """Read the stored earthquakes in the given [lo, hi) ranges back from PostgreSQL with Spark's JDBC source."""
    in_ranges = " OR ".join(
        f"(event_time >= TIMESTAMPTZ '{utc_iso(lo)}' AND event_time < TIMESTAMPTZ '{utc_iso(hi)}')" for lo, hi in ranges
    )
    query = (
        "SELECT event_id, event_time AS event_ts, magnitude, cell_id, grid_lon, grid_lat, region_id, source "
        "FROM seismic_events "
        f"WHERE ({in_ranges}) "
        "AND COALESCE(LOWER(event_type), 'earthquake') = 'earthquake'"
    )
    return (
        spark.read.format("jdbc")
        .option("url", pg_config.jdbc_url)
        .option("driver", "org.postgresql.Driver")
        .option("user", pg_config.user)
        .option("password", pg_config.password)
        .option("query", query)
        .load()
    )


def upsert_anomalies(
    recent: DataFrame, quakes: DataFrame, baseline_df: DataFrame, batch_id: int, cfg: StreamConfig, pg_config: PostgresConfig
) -> int:
    scored = anomaly_windows(recent, quakes, baseline_df, cfg.z_threshold, cfg.min_events_for_anomaly)
    rows = [
        (
            batch_id,
            r["source"],
            r["window_start"],
            r["window_end"],
            r["cell_id"],
            r["grid_lon"],
            r["grid_lat"],
            int(r["event_count"]),
            float(r["baseline_mean"]),
            float(r["baseline_std"]),
            float(r["z_score"]),
        )
        for r in scored.collect()
    ]
    if not rows:
        return 0
    sql = """
        INSERT INTO anomaly_windows (
            batch_id, source, window_start, window_end, cell_id, grid_lon, grid_lat, event_count,
            baseline_mean, baseline_std, z_score
        ) VALUES %s
        ON CONFLICT (source, window_start, window_end, cell_id) DO UPDATE SET
            batch_id = EXCLUDED.batch_id,
            event_count = EXCLUDED.event_count,
            baseline_mean = EXCLUDED.baseline_mean,
            baseline_std = EXCLUDED.baseline_std,
            z_score = EXCLUDED.z_score,
            updated_at = now()
    """
    with closing(pg_connect(pg_config)) as conn, conn, conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=500)
    return len(rows)


def load_region_values(path: str, column: str) -> dict[str, float]:
    """region_id -> value of ``column`` from region_bvalues.csv (empty when the file or column is missing)."""
    if not os.path.exists(path):
        return {}
    result: dict[str, float] = {}
    with open(path, "r", encoding="utf-8") as f:
        header = f.readline().strip().split(",")
        try:
            region_idx = header.index("region_id")
            b_idx = header.index(column)
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


def load_long_term_bvalues(path: str) -> dict[str, float]:
    return load_region_values(path, "long_term_b")


def load_long_term_mc(path: str) -> dict[str, float]:
    return load_region_values(path, "completeness_mag")


def upsert_bvalues(
    recent: DataFrame,
    quakes: DataFrame,
    long_term: dict[str, float],
    long_term_mc: dict[str, float],
    batch_id: int,
    cfg: StreamConfig,
    pg_config: PostgresConfig,
) -> int:
    rows = bvalue_rows(
        bvalue_window_magnitudes(recent, quakes).collect(),
        long_term,
        batch_id,
        cfg.bvalue_min_events,
        mc_floor=long_term_mc,
    )
    if not rows:
        return 0
    sql = """
        INSERT INTO bvalue_windows (
            batch_id, source, window_start, window_end, region_id, event_count, completeness_mag,
            b_value, b_sigma, long_term_b, deviation, is_alert
        ) VALUES %s
        ON CONFLICT (source, window_start, window_end, region_id) DO UPDATE SET
            batch_id = EXCLUDED.batch_id,
            event_count = EXCLUDED.event_count,
            completeness_mag = EXCLUDED.completeness_mag,
            b_value = EXCLUDED.b_value,
            b_sigma = EXCLUDED.b_sigma,
            long_term_b = EXCLUDED.long_term_b,
            deviation = EXCLUDED.deviation,
            is_alert = EXCLUDED.is_alert,
            updated_at = now()
    """
    with closing(pg_connect(pg_config)) as conn, conn, conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=500)
    return len(rows)


# Per source: (anomalous-cell signature, cell -> region id, region id -> centroid) of the last clustering.
_LAST_CLUSTERING: dict[str, tuple] = {}


def refresh_active_regions(spark: SparkSession, batch_id: int, cfg: StreamConfig, pg_config: PostgresConfig) -> int:
    """Cluster the cells flagged in the last ``active_region_hours`` (per source) into active regions.

    K-Means only reruns when the set of anomalous cells (or their scores) changed since the last
    micro-batch; otherwise the stored regions are still current and only new windows get labelled.
    """
    with closing(pg_connect(pg_config)) as conn, conn, conn.cursor() as cur:
        cur.execute(
            """
            WITH clocks AS (
                SELECT source, MAX(event_time) AS clock FROM seismic_events GROUP BY source
            )
            SELECT a.source, a.cell_id, a.grid_lon, a.grid_lat, MAX(a.z_score), MAX(a.event_count)
            FROM anomaly_windows a JOIN clocks c ON c.source = a.source
            WHERE a.window_end > c.clock - make_interval(hours => %s)
            GROUP BY a.source, a.cell_id, a.grid_lon, a.grid_lat
            """,
            (cfg.active_region_hours,),
        )
        by_source: dict[str, list[dict]] = {}
        for source, cell_id, grid_lon, grid_lat, max_z, max_count in cur.fetchall():
            by_source.setdefault(source, []).append(
                {
                    "cell_id": cell_id,
                    "center_lon": float(grid_lon) + 0.5,
                    "center_lat": float(grid_lat) + 0.5,
                    "max_z_score": float(max_z),
                    "max_event_count": int(max_count),
                }
            )
        cur.execute("SELECT DISTINCT source FROM active_regions")
        stale_sources = {r[0] for r in cur.fetchall()} - set(by_source)

    total = 0
    with closing(pg_connect(pg_config)) as conn, conn, conn.cursor() as cur:
        for source in stale_sources:
            cur.execute("DELETE FROM active_regions WHERE source = %s", (source,))
            _LAST_CLUSTERING.pop(source, None)
        for source, cells in by_source.items():
            signature = tuple(sorted((c["cell_id"], round(c["max_z_score"], 3), c["max_event_count"]) for c in cells))
            cached = _LAST_CLUSTERING.get(source)
            if cached is not None and cached[0] == signature:
                _signature, assignment, centroids = cached
            else:
                assignment, regions = cluster_active_cells(
                    spark, cells, max_k=cfg.max_kmeans_clusters, radius_km=cfg.region_radius_km
                )
                centroids = {r.cluster_id: (r.centroid_lon, r.centroid_lat) for r in regions}
                wide = [r for r in regions if not r.within_radius]
                if wide:
                    print(
                        f"WARNING batch {batch_id}: {len(wide)} of {len(regions)} active regions of source {source} are "
                        f"wider than REGION_RADIUS_KM={cfg.region_radius_km:g} km (up to {max(r.radius_km for r in wide):,.0f} km): "
                        f"its {len(cells)} anomalous cells need more than MAX_KMEANS_CLUSTERS={cfg.max_kmeans_clusters} regions",
                        flush=True,
                    )
                cur.execute("DELETE FROM active_regions WHERE source = %s", (source,))
                execute_values(
                    cur,
                    """
                    INSERT INTO active_regions (
                        source, cluster_id, centroid_lon, centroid_lat, cell_count, cells,
                        max_z_score, max_event_count, radius_km, k, batch_id,
                        radius_limit_km, max_k, within_radius
                    ) VALUES %s
                    """,
                    [
                        (
                            source,
                            r.cluster_id,
                            r.centroid_lon,
                            r.centroid_lat,
                            len(r.cells),
                            " ".join(r.cells),
                            r.max_z_score,
                            r.max_event_count,
                            r.radius_km,
                            len(regions),
                            batch_id,
                            float(cfg.region_radius_km),
                            int(cfg.max_kmeans_clusters),
                            bool(r.within_radius),
                        )
                        for r in regions
                    ],
                )
                _LAST_CLUSTERING[source] = (signature, assignment, centroids)
            execute_values(
                cur,
                f"""
                UPDATE anomaly_windows AS a
                SET cluster_id = v.cluster_id, centroid_lon = v.lon, centroid_lat = v.lat
                FROM (VALUES %s) AS v(source, cell_id, cluster_id, lon, lat)
                WHERE a.source = v.source AND a.cell_id = v.cell_id
                  AND a.cluster_id IS DISTINCT FROM v.cluster_id
                  AND a.window_end > (SELECT MAX(event_time) FROM seismic_events s WHERE s.source = v.source)
                                     - make_interval(hours => {int(cfg.active_region_hours)})
                """,
                [
                    (source, cell, cluster, centroids[cluster][0], centroids[cluster][1])
                    for cell, cluster in assignment.items()
                ],
            )
            total += len(centroids)
    return total


AFTERSHOCK_CANDIDATES_SQL = """
    SELECT event_time, longitude, latitude
    FROM seismic_events
    WHERE source = %s
      AND event_time > %s
      AND event_time <= %s + make_interval(days => %s)
      AND magnitude < %s
      AND COALESCE(LOWER(event_type), 'earthquake') = 'earthquake'
      AND latitude BETWEEN %s AND %s
      AND (longitude BETWEEN %s AND %s OR longitude BETWEEN %s AND %s)
    ORDER BY event_time ASC
"""

# The stored forecast values, in the order of the aftershock_forecasts columns after mainshock_id.
FORECAST_FIELDS = (
    "aftershock_count",
    "k_value",
    "c_value",
    "p_value",
    "next_hour_rate",
    "next_six_hour_expected",
    "r_squared",
    "forecast_origin_hours",
    "p_estimated",
)


def decluster_mainshocks(cur, window_days: float = AFTERSHOCK_WINDOW_DAYS) -> None:
    """Store, for every active M >= 5 event, the larger event of its sequence (NULL: it is a mainshock).

    Without this, each M5 aftershock of a large event was forecast as a mainshock of its own and
    counted the rest of the large event's sequence as its aftershocks.
    """
    cur.execute(
        """
        SELECT event_id, source, event_time, magnitude, longitude, latitude, active, larger_event_id
        FROM mainshocks
        WHERE event_time >= (SELECT MIN(event_time) FROM mainshocks WHERE active) - make_interval(days => %s)
        """,
        (int(math.ceil(window_days)),),
    )
    rows = cur.fetchall()
    larger = find_larger_events([row[:6] for row in rows], AFTERSHOCK_RADIUS_KM, window_days)
    # Only active rows: every event that can be larger and near an active one was loaded above.
    changed = [(larger[row[0]], row[0]) for row in rows if row[6] and larger[row[0]] != row[7]]
    if changed:
        execute_values(
            cur,
            """
            UPDATE mainshocks AS m SET larger_event_id = v.larger_id::text
            FROM (VALUES %s) AS v(larger_id, event_id)
            WHERE m.event_id = v.event_id
            """,
            changed,
        )


def compute_aftershock_forecasts(batch_id: int, cfg: StreamConfig, pg_config: PostgresConfig) -> tuple[int, int]:
    """Fit Omori-Utsu to the aftershocks of every active mainshock and store the forecasts that changed.

    Every active mainshock is fitted, of every source (there used to be a LIMIT 20 on the newest
    ones, so older mainshocks, often the main event of a busy sequence, kept a stale forecast or
    never got one). M >= 5 events that belong to the sequence of a larger event are not mainshocks
    (see decluster_mainshocks). "Now" is each source's data clock (its latest event time), so a
    replayed catalogue is forecast on its own timeline just like the live feed.

    A new row is stored only when the fit differs from the mainshock's latest stored one (the
    clock moved or aftershocks arrived), so the latest row of every mainshock is always current.
    The clock each source was forecast at is stored in forecast_runs, in the same transaction:
    a forecast that does not start at its source's run clock was not refitted (the dashboard
    marks it). Comparing with the newest event instead would mark every forecast while a
    micro-batch that brought new events is still running.
    Returns (mainshocks with a forecast, rows stored).
    """
    with closing(pg_connect(pg_config)) as conn, conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE mainshocks m
            SET active = FALSE
            WHERE active = TRUE
              AND event_time < (SELECT MAX(event_time) FROM seismic_events s WHERE s.source = m.source)
                               - make_interval(days => %s)
            """,
            (cfg.mainshock_active_days,),
        )
        decluster_mainshocks(cur)
        cur.execute("SELECT source, MAX(event_time) FROM seismic_events GROUP BY source")
        clocks = {source: clock for source, clock in cur.fetchall() if clock is not None}
        cur.execute(
            """
            SELECT event_id, event_time, magnitude, longitude, latitude, source
            FROM mainshocks
            WHERE active = TRUE AND larger_event_id IS NULL
            ORDER BY event_time DESC
            """
        )
        mainshocks = [row for row in cur.fetchall() if row[5] in clocks]

        fitted: dict[str, tuple] = {}
        mainshocks_per_source: dict[str, int] = {}
        fitted_per_source: dict[str, int] = {}
        for main_id, main_time, main_mag, main_lon, main_lat, source in mainshocks:
            clock = clocks[source]
            mainshocks_per_source[source] = mainshocks_per_source.get(source, 0) + 1
            cur.execute(
                AFTERSHOCK_CANDIDATES_SQL,
                (source, main_time, main_time, AFTERSHOCK_WINDOW_DAYS, main_mag, *search_box_params(main_lon, main_lat)),
            )
            hours_after = aftershock_hours(main_time, main_lon, main_lat, cur.fetchall(), AFTERSHOCK_RADIUS_KM)
            elapsed_hours = (clock - main_time).total_seconds() / 3600.0
            # Aftershocks are only collected for AFTERSHOCK_WINDOW_DAYS, so that is the longest observed span.
            observed_hours = min(elapsed_hours, AFTERSHOCK_WINDOW_DAYS * 24.0)
            fit = fit_omori_utsu(hours_after, forecast_from_hours=elapsed_hours, observed_until_hours=observed_hours)
            if fit is not None:
                fitted[main_id] = tuple(getattr(fit, f) for f in FORECAST_FIELDS)
                fitted_per_source[source] = fitted_per_source.get(source, 0) + 1

        forecast_rows: list[tuple] = []
        if fitted:
            cur.execute(
                f"""
                SELECT DISTINCT ON (mainshock_id) mainshock_id, {", ".join(FORECAST_FIELDS)}
                FROM aftershock_forecasts
                WHERE mainshock_id = ANY(%s)
                ORDER BY mainshock_id, fitted_at DESC, id DESC
                """,
                (list(fitted),),
            )
            latest = {row[0]: tuple(row[1:]) for row in cur.fetchall()}
            forecast_rows = [
                (batch_id, main_id, *values) for main_id, values in fitted.items() if latest.get(main_id) != values
            ]
        if forecast_rows:
            # A micro-batch that is run again after a restart keeps its batch_id: overwrite its rows.
            updates = ", ".join(f"{f} = EXCLUDED.{f}" for f in FORECAST_FIELDS)
            execute_values(
                cur,
                f"""
                INSERT INTO aftershock_forecasts (batch_id, mainshock_id, {", ".join(FORECAST_FIELDS)})
                VALUES %s
                ON CONFLICT (batch_id, mainshock_id) DO UPDATE SET {updates}, fitted_at = now()
                """,
                forecast_rows,
                page_size=100,
            )
        if clocks:
            execute_values(
                cur,
                """
                INSERT INTO forecast_runs (source, clock, batch_id, mainshocks, forecasts, finished_at)
                VALUES %s
                ON CONFLICT (source) DO UPDATE SET clock = EXCLUDED.clock, batch_id = EXCLUDED.batch_id,
                    mainshocks = EXCLUDED.mainshocks, forecasts = EXCLUDED.forecasts, finished_at = now()
                """,
                [
                    (source, clock, batch_id, mainshocks_per_source.get(source, 0), fitted_per_source.get(source, 0))
                    for source, clock in clocks.items()
                ],
                template="(%s, %s, %s, %s, %s, now())",
            )
    return len(fitted), len(forecast_rows)


def process_batch(
    batch_df: DataFrame,
    batch_id: int,
    spark: SparkSession,
    baseline_df: DataFrame,
    long_term_b: dict[str, float],
    long_term_mc: dict[str, float],
    cfg: StreamConfig,
    pg_config: PostgresConfig,
) -> None:
    started = time.time()
    timings: dict[str, float] = {}

    def lap(name: str, t0: float) -> float:
        now = time.time()
        timings[name] = now - t0
        return now

    # Materialise the micro-batch once; every later step reads the cached rows.
    batch_df = batch_df.persist()
    # Records that broke a cleaning rule are stored with their reason instead of vanishing.
    rejected = batch_df.filter(F.col("reject_reason").isNotNull()).select(*REJECTED_COLUMNS).collect()
    rejected_count = write_rejected_rows(rejected, batch_id, pg_config)
    rejected_text = (
        f", rejected={rejected_count} ({reason_summary(r['reject_reason'] for r in rejected)}; see rejected_records)"
        if rejected_count
        else ""
    )
    df = batch_df.filter(F.col("reject_reason").isNull()).dropDuplicates(["event_id"]).persist()
    rows = df.select(*EVENT_COLUMNS).collect()
    if not rows:
        df.unpersist()
        batch_df.unpersist()
        if rejected_count:
            print(f"Batch {batch_id}: received=0{rejected_text}", flush=True)
        else:
            print(f"Batch {batch_id}: empty", flush=True)
        return
    t = lap("read", started)

    event_count = write_event_rows(rows, pg_config)
    mainshock_count = write_mainshock_rows(rows, pg_config, cfg.mainshock_magnitude)
    t = lap("store", t)

    anomaly_count = bvalue_count = 0
    quake_times = [r["event_ts"] for r in rows if is_earthquake_type(r["event_type"])]
    if quake_times:
        quakes = df.filter(is_earthquake(F.col("event_type"))).select(
            "event_id", "event_ts", "magnitude", "cell_id", "grid_lon", "grid_lat", "region_id", "source"
        )
        # Every stored event that can share a sliding window with this batch.
        recent = load_stored_events(
            spark, pg_config, merge_time_ranges(quake_times, timedelta(hours=MAX_WINDOW_HOURS))
        ).cache()
        anomaly_count = upsert_anomalies(recent, quakes, baseline_df, batch_id, cfg, pg_config)
        t = lap("anomalies", t)
        bvalue_count = upsert_bvalues(recent, quakes, long_term_b, long_term_mc, batch_id, cfg, pg_config)
        t = lap("bvalues", t)
        recent.unpersist()

    region_count = refresh_active_regions(spark, batch_id, cfg, pg_config)
    t = lap("kmeans", t)
    forecast_count, forecasts_stored = compute_aftershock_forecasts(batch_id, cfg, pg_config)
    lap("omori", t)
    df.unpersist()
    batch_df.unpersist()
    steps = " ".join(f"{k}={v:.1f}s" for k, v in timings.items())
    print(
        f"Batch {batch_id}: received={len(rows)}{rejected_text}, new_events={event_count}, mainshocks={mainshock_count}, "
        f"anomaly_windows={anomaly_count}, bvalue_windows={bvalue_count}, active_regions={region_count}, "
        f"forecasts={forecast_count} (changed={forecasts_stored}) ({time.time() - started:.1f}s: {steps})",
        flush=True,
    )


def build_spark() -> SparkSession:
    master = os.getenv("SPARK_MASTER_URL", "local[*]")
    return (
        SparkSession.builder.appName("RealTimeSeismicActivityAnalysis")
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
        .getOrCreate()
    )


def kafka_messages(kafka_raw: DataFrame) -> DataFrame:
    """The Kafka columns the pipeline uses: message text, where it is in the topic, and when it arrived."""
    return kafka_raw.select(
        F.col("value").cast("string").alias("value"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
    )


def _not_a_number(raw, number):
    """A field that holds something (not JSON null) that is not a finite number."""
    return raw.isNotNull() & (number.isNull() | F.isnan(number) | (F.abs(number) == F.lit(float("inf"))))


def clean_events(messages: DataFrame) -> DataFrame:
    """Parse, validate and enrich Kafka messages (grid cell, coarse region, source).

    ``messages`` has the columns of MESSAGE_SCHEMA. Every message stays in the output; a message
    that breaks a cleaning rule gets the first rule it breaks in ``reject_reason`` (NULL for a
    valid event), and process_batch() stores it in rejected_records instead of seismic_events.
    Before, such records were filtered out here without any count or log line.
    """
    parsed = messages.select(
        "*",
        # NULL for anything that is not a JSON object: broken JSON, plain text, arrays, empty messages.
        F.expr("json_object_keys(value)").isNotNull().alias("_is_object"),
        F.from_json("value", EVENT_SCHEMA).alias("_record"),
    ).select("value", "kafka_partition", "kafka_offset", "kafka_ts", "_is_object", "_record.*")

    numbers = {name: F.col(name).cast("double") for name in DOUBLE_FIELDS + INT_FIELDS}
    event_ts = F.to_timestamp("event_time")
    reason = (
        F.when(~F.col("_is_object"), "not a JSON object")
        .when(F.coalesce(F.trim(F.col("event_id")), F.lit("")) == "", "missing event_id")
        .when(F.col("event_time").isNull(), "missing event_time")
        .when(event_ts.isNull(), "unparseable event_time")
        .when(F.col("longitude").isNull() | F.col("latitude").isNull(), "missing longitude or latitude")
    )
    for name, number in numbers.items():
        reason = reason.when(_not_a_number(F.col(name), number), f"{name} is not a number")
    reason = reason.when(~numbers["longitude"].between(-180.0, 180.0), "longitude out of range").when(
        ~numbers["latitude"].between(-90.0, 90.0), "latitude out of range"
    )

    cleaned = parsed.withColumn("reject_reason", reason).withColumn("event_ts", event_ts)
    for name in DOUBLE_FIELDS:
        cleaned = cleaned.withColumn(name, numbers[name])
    for name in INT_FIELDS:
        cleaned = cleaned.withColumn(name, numbers[name].cast("int"))
    return (
        cleaned.withColumn(
            "ingestion_ts", F.coalesce(F.to_timestamp("ingestion_time"), F.col("kafka_ts"), F.current_timestamp())
        )
        .withColumn("grid_lon", F.coalesce(F.col("grid_lon"), F.floor(F.col("longitude")).cast("int")))
        .withColumn("grid_lat", F.coalesce(F.col("grid_lat"), F.floor(F.col("latitude")).cast("int")))
        .withColumn("cell_id", F.coalesce(F.col("cell_id"), cell_id_expr(F.col("grid_lon"), F.col("grid_lat"))))
        .withColumn("region_id", F.coalesce(F.col("region_id"), region_id_expr(F.col("longitude"), F.col("latitude"))))
        .withColumn("source", F.coalesce(F.col("source"), F.lit("unknown")))
        # A valid event is de-duplicated by its id; a rejected message by its place in the topic,
        # so rejected messages never hide each other or a later valid copy of the same event.
        .withColumn(
            "dedup_key",
            F.coalesce(
                F.when(F.col("reject_reason").isNull(), F.col("event_id")).otherwise(
                    F.concat(
                        F.lit("rejected:"),
                        F.col("kafka_partition").cast("string"),
                        F.lit(":"),
                        F.col("kafka_offset").cast("string"),
                    )
                ),
                F.lit(""),
            ),
        )
        .drop("_is_object")
    )


# How long Spark remembers an event id, counted in arrival time (the Kafka message timestamp).
# USGS revises events for days after they happen, and the live producer backfills the last week,
# so a duplicate can arrive up to about 7 days after the first copy. PostgreSQL's
# ON CONFLICT (event_id) catches anything older.
DEDUP_WATERMARK = os.getenv("DEDUP_WATERMARK", "7 days")


def prepare_stream(messages: DataFrame) -> DataFrame:
    """Clean and enrich the Kafka messages and drop repeated event ids across micro-batches.

    dropDuplicatesWithinWatermark keeps each id in the streaming state until the watermark has
    passed it by DEDUP_WATERMARK, so the state stays bounded (plain dropDuplicates on event_id
    alone would keep every id forever).

    The watermark is on the arrival time (kafka_ts), not on the event time. With the event
    time, Spark also dropped every record whose event_time was more than 7 days older than the
    newest event seen so far, without any message: a 10-day-old USGS event, or the replay
    catalogue sent with REPLAY_REBASE_TO_NOW=false after the live feed, never reached
    PostgreSQL. Arrival times only move forward, so nothing is dropped for being old. A record
    is only dropped as late if its Kafka timestamp is more than 7 days older than the newest one
    already processed (a producer that sets old message timestamps, or a partition read more
    than 7 days behind the others); LateRecordsListener logs every such drop.
    """
    return (
        clean_events(messages)
        .withWatermark("kafka_ts", DEDUP_WATERMARK)
        .dropDuplicatesWithinWatermark(["dedup_key"])
    )


def rows_dropped_by_watermark(progress: dict) -> int:
    """Rows the stateful operators of one micro-batch dropped as late (from its progress JSON)."""
    return sum(int(op.get("numRowsDroppedByWatermark") or 0) for op in progress.get("stateOperators") or [])


try:
    from pyspark.sql.streaming import StreamingQueryListener
except ImportError:  # pragma: no cover - PySpark before 3.4
    StreamingQueryListener = object


class LateRecordsListener(StreamingQueryListener):
    """Logs every record Spark drops because it arrived behind the de-duplication watermark."""

    def __init__(self):
        super().__init__()
        self.warnings: list[str] = []

    def onQueryStarted(self, event):  # noqa: N802 - method names given by Spark
        pass

    def onQueryProgress(self, event):  # noqa: N802
        progress = json.loads(event.progress.json)
        dropped = rows_dropped_by_watermark(progress)
        if dropped:
            watermark = (progress.get("eventTime") or {}).get("watermark")
            message = (
                f"WARNING batch {progress.get('batchId')}: Spark dropped {dropped} record(s) whose Kafka timestamp "
                f"is more than {DEDUP_WATERMARK} older than the newest one (watermark {watermark}); they are not stored"
            )
            self.warnings.append(message)
            print(message, flush=True)

    def onQueryIdle(self, event):  # noqa: N802
        pass

    def onQueryTerminated(self, event):  # noqa: N802
        pass


def main() -> int:
    cfg = StreamConfig()
    kafka_config = KafkaConfig()
    pg_config = PostgresConfig()
    ensure_schema(pg_config)
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
    long_term_b = load_long_term_bvalues(cfg.region_bvalue_csv)
    long_term_mc = load_long_term_mc(cfg.region_bvalue_csv)
    print(
        f"Loaded {baseline_df.count()} cell baselines from {cfg.baseline_csv} and "
        f"{len(long_term_b)} regional b-values (with {len(long_term_mc)} completeness magnitudes) "
        f"from {cfg.region_bvalue_csv}",
        flush=True,
    )

    kafka_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_config.bootstrap_servers)
        .option("subscribe", kafka_config.topic)
        .option("startingOffsets", kafka_config.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    stream_df = prepare_stream(kafka_messages(kafka_raw))
    if StreamingQueryListener is not object:
        spark.streams.addListener(LateRecordsListener())

    query = (
        stream_df.writeStream.foreachBatch(
            lambda df, bid: process_batch(df, bid, spark, baseline_df, long_term_b, long_term_mc, cfg, pg_config)
        )
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
