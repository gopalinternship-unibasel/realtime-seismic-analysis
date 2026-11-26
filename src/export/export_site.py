"""Write what the Dash dashboard shows as JSON files for the static page in site/.

    python -m src.export.export_site --out DIR

It reads the same PostgreSQL as the dashboard (the POSTGRES_* settings; the connection URL and the
mainshock and forecast queries come from src/dashboard/app.py) and writes, into DIR:

    meta.json            generation time (UTC), commit, Actions run address, newest event per source, file list
    events.json          every stored event (newest first), the map and the event count
    anomaly_windows.json flagged 6-hour windows per grid cell (the page groups them per cell)
    active_regions.json  the K-Means regions
    bvalue_windows.json  24-hour b-value windows per 10 x 10 degree region
    mainshocks.json      mainshocks with their latest forecast and the points of the curves
    status.json          row counts per table and source, rejected records by reason, data clocks

Tables are written as {"columns": [...], "rows": [[...], ...]} to keep the files small. The page
filters by data source, time range and magnitude itself, with the rules of app.py (time ranges count
back from generated_at in meta.json, because the dashboard counts back from the database's now()).
Numbers are rounded; nothing here is secret (no connection details are written).
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.dashboard import app as dash

SCHEMA_VERSION = 1
DEFAULT_MAX_EVENTS = 50000
SOURCES = list(dash.SOURCES)


# --------------------------------------------------------------------------- value helpers


def iso(value) -> str | None:
    """UTC time to the second, e.g. 2026-11-14T00:26:44Z; None for a missing value."""
    if value is None or pd.isna(value):
        return None
    ts = pd.Timestamp(value)
    ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def num(value, digits: int):
    """A float rounded to ``digits`` decimals (a whole number when it has none); None when missing."""
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    rounded = round(number, digits)
    return int(rounded) if digits == 0 else rounded


def whole(value):
    return None if value is None or pd.isna(value) else int(value)


def text(value):
    return None if value is None or (isinstance(value, float) and math.isnan(value)) else str(value)


def table(columns: list[str], rows: list[list]) -> dict:
    return {"columns": columns, "rows": rows}


# --------------------------------------------------------------------------- the files


def export_events(engine, max_events: int) -> tuple[dict, int]:
    total = int(dash.query_df("SELECT COUNT(*) AS n FROM seismic_events", None, engine)["n"].iloc[0])
    df = dash.query_df(
        """
        SELECT event_id, event_time, latitude, longitude, depth_km, magnitude, place, source, cell_id, region_id, event_type
        FROM seismic_events
        ORDER BY event_time DESC, event_id
        LIMIT :limit
        """,
        {"limit": int(max_events)},
        engine,
    )
    rows = [
        [
            r.event_id,
            iso(r.event_time),
            num(r.latitude, 3),
            num(r.longitude, 3),
            num(r.depth_km, 1),
            num(r.magnitude, 2),
            text(r.place),
            r.source,
            text(r.cell_id),
            text(r.region_id),
            text(r.event_type),
        ]
        for r in df.itertuples(index=False)
    ]
    # "type" is the USGS event type (earthquake, quarry blast, explosion, ...): the analytics use earthquakes
    # only, and the page labels the others.
    columns = ["id", "time", "lat", "lon", "depth_km", "mag", "place", "source", "cell_id", "region_id", "type"]
    return table(columns, rows), total


def export_anomaly_windows(engine) -> dict:
    df = dash.query_df(
        """
        SELECT source, cell_id, grid_lon, grid_lat, window_end, event_count, z_score, cluster_id
        FROM anomaly_windows
        ORDER BY source, window_end, cell_id
        """,
        None,
        engine,
    )
    rows = [
        [r.source, r.cell_id, whole(r.grid_lon), whole(r.grid_lat), iso(r.window_end), whole(r.event_count),
         num(r.z_score, 2), whole(r.cluster_id)]
        for r in df.itertuples(index=False)
    ]
    return table(["source", "cell_id", "grid_lon", "grid_lat", "window_end", "event_count", "z", "cluster_id"], rows)


def export_active_regions(engine) -> dict:
    df = dash.query_df("SELECT * FROM active_regions ORDER BY source, cluster_id", None, engine)
    rows = []
    for r in df.to_dict("records"):
        flag = r.get("within_radius")
        rows.append(
            [
                r["source"],
                whole(r["cluster_id"]),
                num(r["centroid_lon"], 3),
                num(r["centroid_lat"], 3),
                whole(r["cell_count"]),
                text(r.get("cells")),
                num(r.get("max_z_score"), 2),
                whole(r.get("max_event_count")),
                num(r.get("radius_km"), 1),
                None if flag is None or pd.isna(flag) else bool(flag),
                num(r.get("radius_limit_km"), 1),
                whole(r.get("max_k")),
            ]
        )
    columns = ["source", "cluster_id", "centroid_lon", "centroid_lat", "cell_count", "cells", "max_z_score",
               "max_event_count", "radius_km", "within_radius", "radius_limit_km", "max_k"]
    return table(columns, rows)


def export_bvalue_windows(engine) -> dict:
    df = dash.query_df(
        """
        SELECT source, region_id, window_end, event_count, b_value, b_sigma, long_term_b, is_alert
        FROM bvalue_windows
        ORDER BY source, region_id, window_end
        """,
        None,
        engine,
    )
    rows = [
        [r.source, r.region_id, iso(r.window_end), whole(r.event_count), num(r.b_value, 3), num(r.b_sigma, 3),
         num(r.long_term_b, 3), bool(r.is_alert) if not pd.isna(r.is_alert) else False]
        for r in df.itertuples(index=False)
    ]
    return table(["source", "region_id", "window_end", "event_count", "b_value", "b_sigma", "long_term_b", "is_alert"],
                 rows)


def forecast_entry(main: dict, forecast: dict, hours: list[float]) -> dict:
    """The latest forecast of one mainshock, with the series the Dash chart draws (app.forecast_series)."""
    series = dash.forecast_series(forecast, hours)
    start = dash.forecast_start_time(main, forecast)
    return {
        "aftershocks": int(forecast["aftershock_count"]),
        "k": num(forecast["k_value"], 3),
        "c": num(forecast["c_value"], 4),
        "p": num(forecast["p_value"], 3),
        "p_estimated": bool(forecast.get("p_estimated", True)),
        "r_squared": num(forecast.get("r_squared"), 3),
        "next_hour": num(forecast["next_hour_rate"], 3),
        "next_six_hours": num(forecast["next_six_hour_expected"], 3),
        "origin_hours": num(series["origin"], 4),
        "start": iso(start),
        "hist": [int(v) for v in series["hist_y"]],
        "curve": {"x": [num(v, 3) for v in series["curve_x"]], "y": [num(v, 4) for v in series["curve_y"]]},
        "points": {"x": [num(v, 3) for v in series["point_x"]], "y": [num(v, 4) for v in series["point_y"]]},
    }


def export_mainshocks(engine) -> dict:
    options = dash.load_mainshock_options(SOURCES, engine)
    items = []
    for opt in options.itertuples(index=False):
        main, forecast, hours = dash.load_forecast_detail(opt.event_id, engine)
        if main is None or forecast is None:
            continue
        items.append(
            {
                "id": main["event_id"],
                "time": iso(main["event_time"]),
                "mag": num(main["magnitude"], 2),
                "lon": num(main["longitude"], 3),
                "lat": num(main["latitude"], 3),
                "place": text(main.get("place")),
                "source": main["source"],
                "active": bool(main["active"]),
                "clock": iso(main.get("clock")),
                "forecast": forecast_entry(main, forecast, hours),
            }
        )
    counts = dash.query_df(
        "SELECT source, COUNT(*) AS n FROM mainshocks WHERE active = TRUE AND larger_event_id IS NULL GROUP BY source",
        None,
        engine,
    )
    # M >= 5 events that belong to a larger event's sequence: no mainshock of their own, no forecast.
    declustered = dash.query_df(
        "SELECT source, COUNT(*) AS n FROM mainshocks WHERE larger_event_id IS NOT NULL GROUP BY source",
        None,
        engine,
    )
    return {
        # Same rule as is_stale() in app.py: stale when the forecast starts more than this many seconds
        # before its source's clock.
        "stale_seconds": dash.STALE_FORECAST_SECONDS,
        # The numbers in the text above the chart (forecast_summary in app.py).
        "radius_km": dash.AFTERSHOCK_RADIUS_KM,
        "window_days": dash.AFTERSHOCK_WINDOW_DAYS,
        "min_aftershocks_for_p": dash.MIN_AFTERSHOCKS_FOR_P,
        "active_counts": {r.source: int(r.n) for r in counts.itertuples(index=False)},
        "declustered_counts": {r.source: int(r.n) for r in declustered.itertuples(index=False)},
        "items": items,
    }


ROW_COUNT_SQL = {
    "seismic_events": "SELECT source, COUNT(*) AS n FROM seismic_events GROUP BY source",
    "anomaly_windows": "SELECT source, COUNT(*) AS n FROM anomaly_windows GROUP BY source",
    "active_regions": "SELECT source, COUNT(*) AS n FROM active_regions GROUP BY source",
    "bvalue_windows": "SELECT source, COUNT(*) AS n FROM bvalue_windows GROUP BY source",
    "mainshocks": "SELECT source, COUNT(*) AS n FROM mainshocks GROUP BY source",
    "aftershock_forecasts": "SELECT m.source AS source, COUNT(*) AS n FROM aftershock_forecasts f "
    "JOIN mainshocks m ON m.event_id = f.mainshock_id GROUP BY m.source",
    "rejected_records": "SELECT COALESCE(source, '(none)') AS source, COUNT(*) AS n FROM rejected_records GROUP BY source",
}


def export_status(engine) -> dict:
    rows = {}
    for name, sql in ROW_COUNT_SQL.items():
        df = dash.query_df(sql, None, engine)
        rows[name] = {r.source: int(r.n) for r in df.itertuples(index=False)}
    rejected = dash.query_df(
        "SELECT reason, COUNT(*) AS n FROM rejected_records GROUP BY reason ORDER BY n DESC, reason", None, engine
    )
    runs = dash.query_df("SELECT * FROM forecast_runs ORDER BY source", None, engine)
    return {
        "rows": rows,
        "rejected": {
            "total": int(rejected["n"].sum()) if not rejected.empty else 0,
            "by_reason": {r.reason: int(r.n) for r in rejected.itertuples(index=False)},
        },
        # The data clock of each source: the latest event time the last forecast run used.
        "forecast_runs": {
            r.source: {
                "clock": iso(r.clock),
                "batch_id": int(r.batch_id),
                "mainshocks": int(r.mainshocks),
                "forecasts": int(r.forecasts),
            }
            for r in runs.itertuples(index=False)
        },
    }


def run_url() -> str | None:
    """Address of the GitHub Actions run that writes the page; None outside GitHub Actions."""
    server, repo, run_id = (os.getenv(n) for n in ("GITHUB_SERVER_URL", "GITHUB_REPOSITORY", "GITHUB_RUN_ID"))
    if not (server and repo and run_id):
        return None
    return f"{server.rstrip('/')}/{repo}/actions/runs/{run_id}"


def export_meta(engine, events_total: int, events_exported: int, files: list[str]) -> dict:
    now = dash.query_df("SELECT now() AS now", None, engine)["now"].iloc[0]
    newest = dash.query_df(
        "SELECT source, MAX(event_time) AS newest FROM seismic_events GROUP BY source ORDER BY source", None, engine
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": iso(now),
        "commit": os.getenv("GITHUB_SHA") or None,
        "run_url": run_url(),
        "newest_event": {r.source: iso(r.newest) for r in newest.itertuples(index=False)},
        "events_total": events_total,
        "events_exported": events_exported,
        "files": files,
    }


def write_json(path: Path, data) -> int:
    # allow_nan=False: a stray NaN would make invalid JSON that the browser cannot parse.
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    path.write_text(payload, encoding="utf-8", newline="\n")
    return len(payload.encode("utf-8"))


def export_site(engine, out_dir, max_events: int = DEFAULT_MAX_EVENTS) -> dict[str, int]:
    """Write every file into ``out_dir``; return the size in bytes of each."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    events, total = export_events(engine, max_events)
    parts = {
        "events.json": events,
        "anomaly_windows.json": export_anomaly_windows(engine),
        "active_regions.json": export_active_regions(engine),
        "bvalue_windows.json": export_bvalue_windows(engine),
        "mainshocks.json": export_mainshocks(engine),
        "status.json": export_status(engine),
    }
    parts["meta.json"] = export_meta(engine, total, len(events["rows"]), sorted(parts) + ["meta.json"])
    return {name: write_json(out / name, data) for name, data in parts.items()}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export the dashboard data as JSON for the static page.")
    parser.add_argument("--out", required=True, help="folder to write the JSON files into")
    parser.add_argument(
        "--max-events",
        type=int,
        default=int(os.getenv("EXPORT_MAX_EVENTS", DEFAULT_MAX_EVENTS)),
        help="newest events to export (default %(default)s)",
    )
    args = parser.parse_args(argv)
    sizes = export_site(dash.ENGINE, args.out, args.max_events)
    for name, size in sorted(sizes.items()):
        print(f"{name}: {size:,} bytes")
    print(f"Written to {args.out} at {datetime.now(tz=timezone.utc):%Y-%m-%d %H:%M:%S} UTC")
    return 0


if __name__ == "__main__":
    sys.exit(main())
