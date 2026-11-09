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
    # Every chosen region gone (e.g. a shorter time range): back to the defaults.
    assert dash_app.pick_regions(options, ["gone"])[1] == default
    # Cleared by the user: stays empty across refreshes.
    assert dash_app.pick_regions(options, [])[1] == []


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
    assert len(names[1]) <= 30  # a long legend entry is clipped on a phone
    assert names[2] == "Forecast, next 6 h"
    # No SVG title (it was clipped at the card edge); the text is HTML in forecast_summary().
    assert not fig.layout.title.text
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


def test_selectors_reset_when_the_data_source_changes(monkeypatch):
    regions = {
        "replay": pd.DataFrame({"region_id": ["R-130_+30"], "windows": [33], "alerts": [0], "max_events": [122]}),
        "usgs-live": pd.DataFrame(
            {"region_id": ["R-120_+30", "R-130_+30", "R-160_+60"], "windows": [133, 114, 27], "alerts": [3, 14, 0],
             "max_events": [101, 35, 26]}
        ),
    }
    mainshocks = {
        "replay": pd.DataFrame({"event_id": ["demo"], "event_time": [datetime(2026, 11, 8, tzinfo=timezone.utc)],
                                "magnitude": [5.6], "place": ["Demo"], "source": ["replay"], "active": [True],
                                "next_six_hour_expected": [8.5], "fitted_at": [None]}),
        "usgs-live": pd.DataFrame({"event_id": ["us1"], "event_time": [datetime(2026, 11, 7, tzinfo=timezone.utc)],
                                   "magnitude": [5.5], "place": ["Japan"], "source": ["usgs-live"], "active": [True],
                                   "next_six_hour_expected": [0.08], "fitted_at": [None]}),
    }
    monkeypatch.setattr(dash_app, "load_bvalue_region_options", lambda sources, _t: regions[sources[0]])
    monkeypatch.setattr(dash_app, "load_mainshock_options", lambda sources: mainshocks[sources[0]])
    # The function behind the Dash callback (Dash wraps it to read the request context).
    update_selectors = getattr(dash_app.update_selectors, "__wrapped__", dash_app.update_selectors)

    # Selections made for the replay; an auto-refresh after switching to live (the radio's own
    # callback still in flight) must not keep them: live gets its own defaults.
    _ro, region_value, _mo, main_value, made_for = update_selectors(
        5, "usgs-live", "7d", ["R-130_+30"], "demo", "replay"
    )
    assert region_value == ["R-120_+30", "R-130_+30", "R-160_+60"]
    assert main_value == "us1" and made_for == "usgs-live"

    # Same source: the user's choice survives the refresh.
    _ro, region_value, _mo, main_value, made_for = update_selectors(
        6, "usgs-live", "7d", ["R-160_+60"], "us1", "usgs-live"
    )
    assert region_value == ["R-160_+60"] and main_value == "us1" and made_for == "usgs-live"


def text_of(component) -> str:
    """All text inside a Dash component tree."""
    if component is None:
        return ""
    if isinstance(component, (str, int, float)):
        return str(component)
    if isinstance(component, (list, tuple)):
        return " ".join(text_of(c) for c in component)
    return text_of(getattr(component, "children", None))


def test_forecast_summary_is_wrapping_html_with_the_generic_p_note_and_stale_warning():
    t0 = datetime(2026, 11, 7, 11, 7, tzinfo=timezone.utc)
    main = {"event_id": "us1", "event_time": t0, "magnitude": 5.5, "place": "74 km NE of Hasaki, Japan",
            "source": "usgs-live", "active": True, "clock": t0 + timedelta(hours=40)}
    forecast = {"k_value": 1.2, "c_value": 0.05, "p_value": 1.08, "r_squared": 0.1, "aftershock_count": 4,
                "next_hour_rate": 0.01, "next_six_hour_expected": 0.08, "forecast_origin_hours": 40.0,
                "p_estimated": False}
    text = text_of(dash_app.forecast_summary(main, forecast))
    assert "M5.5 74 km NE of Hasaki, Japan" in text
    # The phrase QA saw clipped as "generic p=1.08 (under 5 afte" is complete.
    assert "p = 1.08 (generic value: fewer than 5 aftershocks)" in text
    assert "0.08 in the next 6 hours" in text and "after 2026-11-09 03:07 UTC" in text
    assert "Not refitted" not in text and "Inactive" not in text
    # The origin is stored to 4 decimals of an hour: 6.2047 h after 04:47:43 is 10:59:59.9, shown as 11:00.
    near = {**main, "event_time": datetime(2026, 11, 9, 4, 47, 43, tzinfo=timezone.utc)}
    assert "after 2026-11-09 11:00 UTC" in text_of(dash_app.forecast_summary(near, {**forecast, "forecast_origin_hours": 6.2047}))

    # The data moved on but the stored forecast did not: say so.
    stale_main = {**main, "clock": t0 + timedelta(hours=42)}
    assert "Not refitted since 2026-11-09 03:07 UTC" in text_of(dash_app.forecast_summary(stale_main, forecast))
    assert "Inactive" in text_of(dash_app.forecast_summary({**main, "active": False}, forecast))
    assert "No Omori" in text_of(dash_app.forecast_summary(None, None))


def test_mainshock_list_marks_forecasts_that_were_not_refitted():
    t0 = datetime(2026, 11, 2, 16, 49, tzinfo=timezone.utc)
    clock = datetime(2026, 11, 9, 10, 23, tzinfo=timezone.utc)
    options = pd.DataFrame({
        "event_id": ["current", "stale", "old"],
        "event_time": [t0, t0, t0],
        "magnitude": [5.6, 5.3, 5.1],
        "place": ["A", "Palekastro, Greece", "C"],
        "source": ["usgs-live"] * 3,
        "active": [True, True, False],
        "next_six_hour_expected": [1.0, 0.02, 0.0],
        "fitted_at": [None] * 3,
        "forecast_start": [clock, clock - timedelta(hours=2), clock - timedelta(days=40)],
        "clock": [clock] * 3,
    })
    opts, value = dash_app.pick_mainshock(options, None)
    labels = [o["label"] for o in opts]
    assert value == "current"
    assert "(not refitted" not in labels[0] and "(inactive)" not in labels[0]
    assert labels[1].endswith("(not refitted since 11-09 08:23)")
    assert labels[2].endswith("(inactive)")


def test_layout_uses_the_responsive_stylesheet():
    css = Path(dash_app.ASSETS_FOLDER, "responsive.css").read_text(encoding="utf-8")
    assert "@media (max-width: 640px)" in css and "minmax(min(100%, 420px), 1fr)" in css

    def classes(component, found=None):
        found = {} if found is None else found
        if getattr(component, "id", None):
            found[component.id] = getattr(component, "className", None)
        children = getattr(component, "children", None)
        for child in children if isinstance(children, (list, tuple)) else [children]:
            if hasattr(child, "children") or getattr(child, "id", None):
                classes(child, found)
        return found

    found = classes(dash_app.app.layout)
    assert found["controls"] == "card controls-grid" and found["metrics"] == "metrics-grid"
    assert found["forecast-summary"] == "forecast-summary"
    # No fixed-width grid is left in inline styles (they caused the 519 px overflow on a phone).
    assert "gridTemplateColumns" not in str(dash_app.app.layout)
