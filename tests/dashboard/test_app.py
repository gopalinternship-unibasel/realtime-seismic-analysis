import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.dashboard import app as dash_app  # noqa: E402  (imports without touching the database)


def collect_ids(component, found=None):
    found = set() if found is None else found
    cid = getattr(component, "id", None)
    if cid:
        found.add(cid)
    children = getattr(component, "children", None)
    if isinstance(children, (list, tuple)):
        for child in children:
            collect_ids(child, found)
    elif children is not None and hasattr(children, "children"):
        collect_ids(children, found)
    return found


def test_layout_has_user_controls_and_panels():
    ids = collect_ids(dash_app.app.layout)
    for expected in [
        "source",
        "time-range",
        "min-mag",
        "auto-refresh",
        "bvalue-regions",
        "mainshock",
        "world-map",
        "regions-table",
        "bvalue-chart",
        "forecast-chart",
        "metrics",
    ]:
        assert expected in ids


def test_source_and_time_filters():
    assert dash_app.selected_sources("all") == ["usgs-live", "replay"]
    assert dash_app.selected_sources("replay") == ["replay"]
    assert dash_app.time_clause("event_time", "24h") == (
        " AND event_time > now() - make_interval(hours => :hours)",
        {"hours": 24},
    )
    assert dash_app.time_clause("event_time", "all") == ("", {})


def test_region_selection_survives_refresh_and_defaults_to_busiest():
    options = pd.DataFrame(
        {
            "region_id": ["R-130_+30", "R-160_+50", "R+140_+30", "R-080_-40", "R+120_+20"],
            "windows": [50, 40, 30, 20, 10],
            "alerts": [2, 0, 0, 1, 0],
            "max_events": [100, 40, 30, 20, 10],
        }
    )
    _opts, default = dash_app.pick_regions(options, None)
    assert default == ["R-130_+30", "R-160_+50", "R+140_+30", "R-080_-40"]
    _opts, kept = dash_app.pick_regions(options, ["R+120_+20", "gone"])
    assert kept == ["R+120_+20"]


def test_forecast_chart_shows_observations_fit_and_forecast():
    t0 = datetime(2026, 11, 9, tzinfo=timezone.utc)
    main = {"event_id": "m1", "event_time": t0, "magnitude": 5.6, "place": "Demo", "source": "replay"}
    forecast = {
        "k_value": 2.0,
        "c_value": 0.05,
        "p_value": 1.1,
        "r_squared": 0.8,
        "aftershock_count": 6,
        "next_hour_rate": 0.4,
        "next_six_hour_expected": 2.1,
        "forecast_origin_hours": 22.0,
    }
    fig = dash_app.build_forecast_chart(main, forecast, [0.2, 0.4, 1.5, 2.5, 6.0, 21.0])
    names = [trace.name for trace in fig.data]
    assert names[0] == "Observed aftershocks per hour"
    assert names[1].startswith("Fitted Omori-Utsu")
    assert names[2] == "Forecast, next 6 h"
    assert list(fig.data[2].x) == [22.5, 23.5, 24.5, 25.5, 26.5, 27.5]
    # The six hourly expectations add up to the stored 6-hour forecast of the same parameters.
    from src.common.seismo_models import omori_expected

    assert abs(sum(fig.data[2].y) - omori_expected(2.0, 0.05, 1.1, 22.0, 28.0)) < 1e-9
    empty = dash_app.build_forecast_chart(None, None, [])
    assert len(empty.data) == 0


def test_map_shows_events_cells_and_kmeans_regions():
    events = pd.DataFrame(
        {
            "event_id": ["a"],
            "event_time": [datetime(2026, 11, 9, tzinfo=timezone.utc)],
            "magnitude": [2.0],
            "longitude": [-122.3],
            "latitude": [37.6],
            "place": ["X"],
            "source": ["replay"],
        }
    )
    cells = pd.DataFrame(
        {
            "source": ["replay"],
            "cell_id": ["-123:37"],
            "grid_lon": [-123],
            "grid_lat": [37],
            "max_count": [9],
            "max_z": [17.6],
            "cluster_id": [0],
            "windows": [6],
        }
    )
    regions = pd.DataFrame(
        {
            "source": ["replay"],
            "cluster_id": [0],
            "centroid_lon": [-122.5],
            "centroid_lat": [37.5],
            "cell_count": [1],
            "radius_km": [0.0],
            "max_z_score": [17.6],
            "max_event_count": [9],
            "cells": ["-123:37"],
        }
    )
    fig = dash_app.build_map(events, cells, regions)
    assert [t.name for t in fig.data] == ["Events", "Anomalous grid cells", "K-Means active regions"]
    table = dash_app.regions_table(regions)
    assert table.id == "regions-table-content"


def test_bvalue_chart_marks_alerts():
    now = datetime(2026, 11, 9, tzinfo=timezone.utc)
    bvalues = pd.DataFrame(
        {
            "source": ["replay", "replay"],
            "region_id": ["R-130_+30", "R-130_+30"],
            "window_start": [now - timedelta(hours=24), now - timedelta(hours=23)],
            "window_end": [now, now + timedelta(hours=1)],
            "event_count": [20, 25],
            "b_value": [0.6, 0.65],
            "b_sigma": [0.13, 0.13],
            "long_term_b": [1.0, 1.0],
            "is_alert": [True, False],
        }
    )
    fig = dash_app.build_bvalue_chart(bvalues)
    assert len(fig.data) == 2 and fig.data[1].name.endswith("alert")
