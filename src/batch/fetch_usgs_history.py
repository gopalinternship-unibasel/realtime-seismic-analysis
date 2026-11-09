"""Download a historical earthquake catalogue from the USGS FDSN event service.

The output feeds ``compute_baselines.py``: per-cell 6-hour rates and regional
long-term b-values come from real USGS history instead of hand-set numbers.

    python src/batch/fetch_usgs_history.py            # last 90 days, ending today 00:00 UTC

Environment: HISTORY_DAYS (default 90), HISTORY_END (YYYY-MM-DD, default today UTC),
HISTORY_CHUNK_DAYS (default 10), HISTORICAL_CSV (output path).
"""

from __future__ import annotations

import csv
import io
import json
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

FDSN_URL = os.getenv("USGS_FDSN_URL", "https://earthquake.usgs.gov/fdsnws/event/1/query")
HISTORICAL_CSV = Path(os.getenv("HISTORICAL_CSV", "/opt/workspace/data/historical/usgs_historical_events.csv"))
HISTORY_DAYS = int(os.getenv("HISTORY_DAYS", "90"))
HISTORY_CHUNK_DAYS = int(os.getenv("HISTORY_CHUNK_DAYS", "10"))
FDSN_LIMIT = 20000  # Service maximum per request.
USER_AGENT = "seismic-spark-dis-workshop/1.0 (student project; historical baseline download)"

OUTPUT_COLUMNS = ["event_id", "event_time", "magnitude", "longitude", "latitude", "depth_km", "event_type"]


def meta_path(csv_path: Path) -> Path:
    return csv_path.with_name(csv_path.stem + ".meta.json")


def history_period(end: date | None = None, days: int = HISTORY_DAYS) -> tuple[datetime, datetime]:
    end_day = end or datetime.now(tz=timezone.utc).date()
    end_dt = datetime(end_day.year, end_day.month, end_day.day, tzinfo=timezone.utc)
    return end_dt - timedelta(days=days), end_dt


def fdsn_rows(text: str) -> list[dict]:
    """Convert an FDSN ``format=csv`` response into the project's historical CSV rows."""
    rows = []
    for rec in csv.DictReader(io.StringIO(text)):
        try:
            lon, lat = float(rec["longitude"]), float(rec["latitude"])
        except (KeyError, TypeError, ValueError):
            continue
        if not rec.get("id") or not rec.get("time") or not (-180 <= lon <= 180 and -90 <= lat <= 90):
            continue
        rows.append(
            {
                "event_id": rec["id"],
                "event_time": rec["time"],
                "magnitude": rec.get("mag", ""),
                "longitude": lon,
                "latitude": lat,
                "depth_km": rec.get("depth", ""),
                "event_type": rec.get("type", "earthquake"),
            }
        )
    return rows


def fetch_chunk(start: datetime, end: datetime) -> list[dict]:
    params = {
        "format": "csv",
        "starttime": start.strftime("%Y-%m-%dT%H:%M:%S"),
        "endtime": end.strftime("%Y-%m-%dT%H:%M:%S"),
        "eventtype": "earthquake",
        "orderby": "time-asc",
        "limit": FDSN_LIMIT,
    }
    for attempt in range(4):
        try:
            response = requests.get(FDSN_URL, params=params, timeout=120, headers={"User-Agent": USER_AGENT})
            if response.status_code == 204:
                return []
            response.raise_for_status()
            return fdsn_rows(response.text)
        except requests.RequestException as exc:
            if attempt == 3:
                raise
            print(f"USGS request failed ({exc}); retrying in {5 * (attempt + 1)} s", flush=True)
            time.sleep(5 * (attempt + 1))
    return []


def main() -> int:
    end_env = os.getenv("HISTORY_END")
    start, end = history_period(date.fromisoformat(end_env) if end_env else None)
    print(f"Downloading USGS earthquakes {start.isoformat()} .. {end.isoformat()} from {FDSN_URL}", flush=True)
    seen: set[str] = set()
    rows: list[dict] = []
    chunk_start = start
    while chunk_start < end:
        chunk_end = min(chunk_start + timedelta(days=HISTORY_CHUNK_DAYS), end)
        chunk = fetch_chunk(chunk_start, chunk_end)
        if len(chunk) >= FDSN_LIMIT:
            print("A chunk hit the USGS 20000-event limit; lower HISTORY_CHUNK_DAYS.", file=sys.stderr)
            return 1
        fresh = [r for r in chunk if r["event_id"] not in seen]
        seen.update(r["event_id"] for r in fresh)
        rows.extend(fresh)
        print(f"  {chunk_start.date()} .. {chunk_end.date()}: {len(fresh)} events", flush=True)
        chunk_start = chunk_end
        time.sleep(1)  # Be polite to the public service.

    HISTORICAL_CSV.parent.mkdir(parents=True, exist_ok=True)
    tmp = HISTORICAL_CSV.with_suffix(".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(HISTORICAL_CSV)
    # compute_baselines.py needs the period (not just the events) to count the empty 6-hour windows.
    meta_path(HISTORICAL_CSV).write_text(
        json.dumps(
            {
                "source": FDSN_URL,
                "eventtype": "earthquake",
                "period_start": start.isoformat(),
                "period_end": end.isoformat(),
                "events": len(rows),
                "downloaded_at": datetime.now(tz=timezone.utc).isoformat(timespec="seconds"),
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"Wrote {len(rows)} events to {HISTORICAL_CSV}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
