"""Add a synthetic aftershock sequence to the replay demo catalogue (deterministic, seeded).

The hand-made demo catalogue had 20 aftershocks and 18 swarm events around its M5.6
Northern California mainshock: 39 events in region R-130_+30, of which only about 15 sit
at or above the completeness magnitude. That is too few for a 24-hour Gutenberg-Richter
b-value (the streaming job needs BVALUE_MIN_EVENTS = 20 above Mc), so the replay demo
never showed a b-value window. A real M5.6 sequence has far more small aftershocks: the
generic California model of Reasenberg & Jones (1989) expects about 130 aftershocks of
M >= 2 in the first 22 hours.

This script appends ``N_AFTERSHOCKS`` aftershocks of the mainshock ``demo_ca_main_001`` to
the catalogue, drawn from
  * the Omori-Utsu law n(t) = K (t + c)^-p with c = 0.05 days and p = 1.08 (times),
  * the Gutenberg-Richter law with b = 1.0 above M 2.0, capped at M 4.4 (Bath's law),
  * epicentres within about 20 km of the mainshock (all in grid cell -123:37).
Earlier generated events (ids ``demo_ca_gen_*``) are replaced, so running it twice gives the
same file. The original 84 records are kept unchanged.

    python src/producer/extend_replay_catalog.py [path/to/sample_replay_events.jsonl]
"""

from __future__ import annotations

import json
import math
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

from src.common.geo import coarse_region, grid_cell

DEFAULT_FILE = Path(__file__).resolve().parents[2] / "data/replay_catalog/sample_replay_events.jsonl"
MAINSHOCK_ID = "demo_ca_main_001"
GENERATED_PREFIX = "demo_ca_gen_"
N_AFTERSHOCKS = 100
SEED = 20260517
C_DAYS, P_VALUE = 0.05, 1.08
B_VALUE, M_MIN, M_MAX = 1.0, 2.0, 4.4
SPAN_HOURS = 21.9  # before the catalogue's last event (22 h after the mainshock), so the timeline is unchanged
MAX_DISTANCE_KM = 20.0


def omori_time_hours(rng: random.Random, span_hours: float) -> float:
    """Inverse-transform sample of an aftershock time (hours) from (t + c)^-p on [0, span]."""
    q = 1.0 - P_VALUE
    total = ((span_hours / 24.0 + C_DAYS) ** q - C_DAYS**q) / q
    t_days = (C_DAYS**q + rng.random() * total * q) ** (1.0 / q) - C_DAYS
    return t_days * 24.0


def gutenberg_richter_magnitude(rng: random.Random) -> float:
    """Magnitude above M_MIN with b = B_VALUE, rounded to 0.01 like the rest of the catalogue."""
    beta = B_VALUE * math.log(10.0)
    while True:
        magnitude = round((M_MIN - 0.005) + rng.expovariate(beta), 2)
        if M_MIN <= magnitude <= M_MAX:
            return magnitude


def generate(mainshock: dict, rng: random.Random) -> list[dict]:
    t0 = datetime.fromisoformat(mainshock["event_time"])
    lon0, lat0 = float(mainshock["longitude"]), float(mainshock["latitude"])
    events = []
    for _ in range(N_AFTERSHOCKS):
        hours = omori_time_hours(rng, SPAN_HOURS)
        distance = MAX_DISTANCE_KM * math.sqrt(rng.random())
        azimuth = rng.uniform(0.0, 2.0 * math.pi)
        lat = round(lat0 + distance * math.cos(azimuth) / 111.19, 4)
        lon = round(lon0 + distance * math.sin(azimuth) / (111.19 * math.cos(math.radians(lat0))), 4)
        magnitude = gutenberg_richter_magnitude(rng)
        grid_lon, grid_lat, cell_id = grid_cell(lon, lat)
        events.append(
            {
                "azimuthal_gap": round(rng.uniform(40.0, 140.0), 1),
                "cell_id": cell_id,
                "depth_km": round(rng.uniform(3.0, 14.0), 2),
                "event_id": "",
                "event_time": (t0 + timedelta(seconds=round(hours * 3600))).isoformat(),
                "event_type": "earthquake",
                "felt_count": int(rng.uniform(0, 10 * magnitude)),
                "grid_lat": grid_lat,
                "grid_lon": grid_lon,
                "ingestion_time": mainshock.get("ingestion_time"),
                "latitude": lat,
                "longitude": lon,
                "magnitude": magnitude,
                "place": "Demo aftershock - Northern California",
                "region_id": coarse_region(lon, lat),
                "rms": round(rng.uniform(0.05, 0.9), 2),
                "significance": int(round(magnitude * 100)),
                "station_count": int(rng.uniform(15, 70)),
                "status": "reviewed",
                "tsunami": 0,
                "url": None,
            }
        )
    events.sort(key=lambda e: e["event_time"])
    for i, event in enumerate(events):
        event["event_id"] = f"{GENERATED_PREFIX}{i:03d}"
    return events


def main(path: Path = DEFAULT_FILE) -> int:
    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    records = [json.loads(line) for line in lines]
    kept = [line for line, rec in zip(lines, records) if not rec["event_id"].startswith(GENERATED_PREFIX)]
    mainshock = next(r for r in records if r["event_id"] == MAINSHOCK_ID)
    generated = generate(mainshock, random.Random(SEED))
    out = kept + [json.dumps(e, sort_keys=True) for e in generated]
    path.write_text("\n".join(out) + "\n", encoding="utf-8", newline="\n")
    print(f"{path}: {len(kept)} original records + {len(generated)} generated aftershocks = {len(out)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_FILE))
