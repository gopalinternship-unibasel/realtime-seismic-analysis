from __future__ import annotations

import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, Input, Output, State, dcc, html
from sqlalchemy import create_engine, text

from src.common.seismo_models import (
    AFTERSHOCK_RADIUS_KM,
    AFTERSHOCK_WINDOW_DAYS,
    MIN_AFTERSHOCKS_FOR_P,
    aftershock_hours,
    omori_expected,
    omori_rate,
    search_box_params,
)

DB_USER = os.getenv("POSTGRES_USER", "seismic")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "seismic")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "seismic")
DASH_HOST = os.getenv("DASH_HOST", "127.0.0.1")
DASH_PORT = int(os.getenv("DASH_PORT", "8050"))
REFRESH_MS = int(os.getenv("DASH_REFRESH_MS", "10000"))

ENGINE = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}", pool_pre_ping=True
)

SOURCES = {"usgs-live": "Live USGS feed", "replay": "Replay demo"}
SOURCE_OPTIONS = [
    {"label": "Both", "value": "all"},
    {"label": "Live USGS feed", "value": "usgs-live"},
    {"label": "Replay demo", "value": "replay"},
]
TIME_RANGES = {"6h": 6, "24h": 24, "72h": 72, "7d": 168, "30d": 720, "all": None}
TIME_RANGE_OPTIONS = [
    {"label": "Last 6 hours", "value": "6h"},
    {"label": "Last 24 hours", "value": "24h"},
    {"label": "Last 3 days", "value": "72h"},
    {"label": "Last 7 days", "value": "7d"},
    {"label": "Last 30 days", "value": "30d"},
    {"label": "All time", "value": "all"},
]
DEFAULT_REGIONS = 4
CLUSTER_COLORS = ["#d62728", "#1f77b4", "#2ca02c", "#9467bd", "#ff7f0e", "#8c564b", "#e377c2", "#17becf"]
# A forecast whose start is more than this before its source's clock in forecast_runs (the data
# clock of the latest forecast run) was not refitted in that run. The stored origin is rounded to
# 4 decimals of an hour (at most 0.18 s off), so 1 s is enough.
STALE_FORECAST_SECONDS = 1

# The page layout (cards, responsive grids) is in assets/responsive.css.
ASSETS_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets")
app = Dash(__name__, assets_folder=ASSETS_FOLDER)
app.title = "Real-Time Seismic Activity Analysis"

LABEL_STYLE = {"color": "#374151", "fontWeight": 600, "marginBottom": "6px", "display": "block"}


def query_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    try:
        return pd.read_sql_query(text(sql), ENGINE, params=params or {})
    except Exception as exc:  # noqa: BLE001 - dashboard should stay up while pipeline starts.
        print(f"Dashboard query failed: {exc}", flush=True)
        return pd.DataFrame()


def selected_sources(source: str | None) -> list[str]:
    if source in SOURCES:
        return [source]
    return list(SOURCES)


def time_clause(column: str, time_range: str | None) -> tuple[str, dict]:
    hours = TIME_RANGES.get(time_range or "7d", 168)
    if hours is None:
        return "", {}
    return f" AND {column} > now() - make_interval(hours => :hours)", {"hours": int(hours)}


def source_label(source: str) -> str:
    return SOURCES.get(source, source)


# --------------------------------------------------------------------------- figures


def build_map(events: pd.DataFrame, cells: pd.DataFrame, regions: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if not events.empty:
        fig.add_trace(
            go.Scattergeo(
                lon=events["longitude"],
                lat=events["latitude"],
                text=events.apply(
                    lambda r: f"{r.get('place', '')}<br>M {r.get('magnitude', '')}<br>{r.get('event_time', '')}"
                    f"<br>{source_label(r.get('source', ''))}",
                    axis=1,
                ),
                hoverinfo="text",
                mode="markers",
                marker={
                    "size": events["magnitude"].fillna(1).clip(lower=0.5) * 3 + 3,
                    "color": events["magnitude"].fillna(0),
                    "colorscale": "Viridis",
                    "showscale": True,
                    "colorbar": {"title": "Magnitude", "x": 1.0},
                    "opacity": 0.72,
                },
                name="Events",
            )
        )
    if not cells.empty:
        colors = [
            CLUSTER_COLORS[int(c) % len(CLUSTER_COLORS)] if pd.notna(c) else "#111827" for c in cells["cluster_id"]
        ]
        fig.add_trace(
            go.Scattergeo(
                lon=cells["grid_lon"] + 0.5,
                lat=cells["grid_lat"] + 0.5,
                text=cells.apply(
                    lambda r: f"Anomalous cell {r['cell_id']}<br>max 6 h count={int(r['max_count'])}"
                    f"<br>max z={r['max_z']:.2f}<br>active region: {'none' if pd.isna(r['cluster_id']) else 'R' + str(int(r['cluster_id']))}"
                    f"<br>{source_label(r['source'])}",
                    axis=1,
                ),
                hoverinfo="text",
                mode="markers",
                marker={"size": 16, "symbol": "square-open", "line": {"width": 3}, "color": colors},
                name="Anomalous grid cells",
            )
        )
    if not regions.empty:
        fig.add_trace(
            go.Scattergeo(
                lon=regions["centroid_lon"],
                lat=regions["centroid_lat"],
                text=[f"R{int(c)}" for c in regions["cluster_id"]],
                hovertext=regions.apply(
                    lambda r: f"Active region R{int(r['cluster_id'])} ({source_label(r['source'])})"
                    f"<br>{int(r['cell_count'])} cells, radius {radius_text(r)}<br>max z={r['max_z_score']:.2f}",
                    axis=1,
                ),
                hoverinfo="text",
                mode="markers+text",
                textposition="top center",
                marker={
                    "size": 14,
                    "symbol": "x",
                    "color": [CLUSTER_COLORS[int(c) % len(CLUSTER_COLORS)] for c in regions["cluster_id"]],
                },
                name="K-Means active regions",
            )
        )
    fig.update_geos(
        projection_type="natural earth",
        showland=True,
        landcolor="rgb(243,244,246)",
        showcountries=True,
        showocean=True,
        oceancolor="rgb(229,247,255)",
    )
    fig.update_layout(margin={"r": 0, "t": 20, "l": 0, "b": 0}, height=520, legend={"orientation": "h"})
    return fig


def build_bvalue_chart(bvalues: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if not bvalues.empty:
        multi_source = bvalues["source"].nunique() > 1
        for (region, source), group in bvalues.groupby(["region_id", "source"]):
            name = f"{region} ({source_label(source)})" if multi_source else str(region)
            fig.add_trace(
                go.Scatter(
                    x=group["window_end"],
                    y=group["b_value"],
                    mode="lines+markers",
                    name=name,
                    error_y={"type": "data", "array": group["b_sigma"], "visible": True},
                    customdata=np.stack([group["event_count"], group["long_term_b"]], axis=-1),
                    hovertemplate="%{x}<br>b=%{y:.2f}<br>events above Mc=%{customdata[0]}"
                    "<br>long-term b=%{customdata[1]:.2f}<extra>" + name + "</extra>",
                )
            )
            alerts = group[group["is_alert"]]
            if not alerts.empty:
                fig.add_trace(
                    go.Scatter(
                        x=alerts["window_end"],
                        y=alerts["b_value"],
                        mode="markers",
                        marker={"color": "#dc2626", "size": 11, "symbol": "circle-open", "line": {"width": 2}},
                        name=f"{name} alert",
                        showlegend=False,
                        hovertemplate="b-value alert<br>b=%{y:.2f}<extra>" + name + "</extra>",
                    )
                )
    else:
        fig.add_annotation(text="No b-value windows for this selection yet", showarrow=False, x=0.5, y=0.5,
                           xref="paper", yref="paper")
    # The chart title is the card heading (HTML wraps on a narrow screen; an SVG title is clipped).
    fig.update_layout(
        xaxis_title="Window end (UTC)",
        yaxis_title="b-value",
        margin={"r": 20, "t": 20, "l": 50, "b": 40},
        height=380,
        legend={"orientation": "h", "y": -0.25},
    )
    return fig


def forecast_start_time(mainshock: dict, forecast: dict) -> pd.Timestamp | None:
    """The time the stored forecast starts: mainshock time + forecast_origin_hours."""
    origin = forecast.get("forecast_origin_hours")
    if origin is None or pd.isna(origin):
        return None
    # origin is stored to 4 decimals of an hour (0.36 s): round so 10:59:59.99 shows as 11:00.
    return (pd.Timestamp(mainshock["event_time"]) + pd.Timedelta(hours=float(origin))).round("s")


def is_stale(start, clock, active: bool = True) -> bool:
    """An active mainshock's forecast is stale when it starts before the clock of its source's latest
    forecast run (forecast_runs). Not the newest event: that moves as soon as a micro-batch stores
    new events, while the forecasts of that batch are fitted at its end."""
    if not active or start is None or clock is None or pd.isna(start) or pd.isna(clock):
        return False
    return (pd.Timestamp(clock) - pd.Timestamp(start)).total_seconds() > STALE_FORECAST_SECONDS


def forecast_summary(mainshock: dict | None, forecast: dict | None) -> list:
    """The mainshock and the fitted numbers as text above the Omori-Utsu chart.

    They used to be the chart's SVG title and subtitle, which Plotly does not wrap: the
    subtitle was clipped at the right edge of the card (for example "generic p=1.08 (under 5 afte").
    """
    if not mainshock or not forecast:
        return [html.Div("No Omori–Utsu forecast for this selection yet.")]
    p = float(forecast["p_value"])
    count = int(forecast["aftershock_count"])
    p_was_fitted = bool(forecast.get("p_estimated", True))
    p_text = (
        f"p = {p:.2f} (fitted), R² = {float(forecast['r_squared']):.2f}"
        if p_was_fitted
        else f"p = {p:.2f} (generic value: fewer than {MIN_AFTERSHOCKS_FOR_P} aftershocks)"
    )
    start = forecast_start_time(mainshock, forecast)
    start_text = f" after {start:%Y-%m-%d %H:%M} UTC" if start is not None else ""
    lines = [
        html.Div(
            html.Strong(
                f"M{float(mainshock['magnitude']):.1f} {mainshock.get('place') or mainshock.get('event_id')} · "
                f"{pd.Timestamp(mainshock['event_time']):%Y-%m-%d %H:%M} UTC"
            )
        ),
        html.Div(
            f"{count} aftershocks within {AFTERSHOCK_RADIUS_KM:.0f} km and {AFTERSHOCK_WINDOW_DAYS} days · "
            f"K = {float(forecast['k_value']):.1f}/day, c = {float(forecast['c_value']):.2f} d, {p_text}"
        ),
        html.Div(
            f"Expected aftershocks{start_text}: {float(forecast['next_hour_rate']):.2f} in the next hour, "
            f"{float(forecast['next_six_hour_expected']):.2f} in the next 6 hours"
        ),
    ]
    clock = mainshock.get("clock")
    if not mainshock.get("active", True):
        lines.append(html.Div("Inactive: more than 30 days before the latest event; this is its last forecast.",
                              className="forecast-warning"))
    elif is_stale(start, clock):
        lines.append(
            html.Div(
                f"Not refitted since {start:%Y-%m-%d %H:%M} UTC: the latest forecast run used the data up to "
                f"{pd.Timestamp(clock):%Y-%m-%d %H:%M} UTC.",
                className="forecast-warning",
            )
        )
    return lines


def build_forecast_chart(mainshock: dict | None, forecast: dict | None, hours_after: list[float]) -> go.Figure:
    fig = go.Figure()
    if not mainshock or not forecast:
        fig.add_annotation(text="No Omori–Utsu forecast for this selection yet", showarrow=False, x=0.5, y=0.5,
                           xref="paper", yref="paper")
        fig.update_layout(height=380, margin={"r": 20, "t": 20, "l": 50, "b": 40})
        return fig

    k, c, p = float(forecast["k_value"]), float(forecast["c_value"]), float(forecast["p_value"])
    origin = forecast.get("forecast_origin_hours")
    if origin is None or pd.isna(origin):
        origin = float(np.ceil(max(hours_after))) if hours_after else 0.0
    origin = float(origin)

    if hours_after:
        max_hour = int(np.ceil(max(hours_after)))
        counts, edges = np.histogram(hours_after, bins=np.arange(0, max_hour + 2, 1))
        fig.add_trace(
            go.Bar(
                x=(edges[:-1] + 0.5),
                y=counts,
                width=0.9,
                name="Observed aftershocks per hour",
                marker_color="#94a3b8",
            )
        )
    curve_h = np.linspace(0.25, origin + 6, 200)
    fig.add_trace(
        go.Scatter(
            x=curve_h,
            y=[omori_rate(k, c, p, h) for h in curve_h],
            mode="lines",
            # Short: a long legend entry is clipped on a narrow screen (the parameters are in the summary).
            name="Fitted Omori-Utsu rate",
            hovertemplate=f"%{{x:.1f}} h: %{{y:.2f}} per hour<extra>K={k:.1f}/day, c={c:.2f} d, p={p:.2f}</extra>",
            line={"color": "#1d4ed8", "width": 2},
        )
    )
    # Expected aftershocks in each of the next six hours, plotted at the middle of the hour.
    fig.add_trace(
        go.Scatter(
            x=[origin + h + 0.5 for h in range(6)],
            y=[omori_expected(k, c, p, origin + h, origin + h + 1) for h in range(6)],
            mode="markers",
            name="Forecast, next 6 h",
            marker={"color": "#dc2626", "size": 9},
        )
    )
    fig.add_vrect(x0=origin, x1=origin + 6, fillcolor="#fee2e2", opacity=0.5, line_width=0)
    # Mainshock and fitted numbers are in forecast_summary() (HTML, wraps on any screen width).
    fig.update_layout(
        xaxis_title="Hours after mainshock",
        yaxis_title="Aftershocks per hour",
        margin={"r": 20, "t": 20, "l": 50, "b": 40},
        height=380,
        legend={"orientation": "h", "y": -0.25},
        bargap=0.05,
    )
    return fig


def metric_card(label: str, value, card_id: str) -> html.Div:
    return html.Div(
        [html.Div(label, style={"color": "#6b7280"}), html.H2(str(value), style={"margin": "4px 0"})],
        className="card",
        id=card_id,
    )


def is_wide_region(region) -> bool:
    """A region the streaming job flagged as wider than REGION_RADIUS_KM (k reached MAX_KMEANS_CLUSTERS).

    Rows written before the flag existed have NULL there and are not marked.
    """
    flag = region.get("within_radius")
    return flag is not None and not pd.isna(flag) and not bool(flag)


def radius_text(region) -> str:
    shown = f"{region['radius_km']:,.0f} km"
    if is_wide_region(region):
        limit = region.get("radius_limit_km")
        shown += f" (over the {limit:,.0f} km limit)" if limit is not None and pd.notna(limit) else " (over the limit)"
    return shown


def wide_regions_note(regions: pd.DataFrame) -> html.Div | None:
    wide = [r for _, r in regions.iterrows() if is_wide_region(r)]
    if not wide:
        return None
    limits = {float(r["radius_limit_km"]) for r in wide if pd.notna(r.get("radius_limit_km"))}
    caps = {int(r["max_k"]) for r in wide if pd.notna(r.get("max_k"))}
    limit_text = f"{min(limits):,.0f} km" if limits else "the radius limit"
    cap_text = f"MAX_KMEANS_CLUSTERS = {min(caps)}" if caps else "MAX_KMEANS_CLUSTERS"
    sources = sorted({source_label(r["source"]) for r in wide})
    return html.Div(
        f"{len(wide)} region(s) of {', '.join(sources)} are wider than {limit_text}: the anomalous cells lie in more "
        f"separate areas than {cap_text} regions can cover, so cells far apart share a region. "
        "Raise MAX_KMEANS_CLUSTERS in .env and run start.ps1 again to split them.",
        id="regions-warning",
        className="regions-warning",
    )


def regions_table(regions: pd.DataFrame) -> html.Div:
    if regions.empty:
        return html.Div("No active regions: no grid cell is anomalous in the last 24 hours of data.",
                        style={"color": "#6b7280"})
    header = ["Region", "Source", "Centroid (lon, lat)", "Cells", "Radius", "Max z", "Max 6 h count", "Cell ids"]
    rows = [
        html.Tr(
            [
                html.Td(f"R{int(r['cluster_id'])}"),
                html.Td(source_label(r["source"])),
                html.Td(f"{r['centroid_lon']:.2f}, {r['centroid_lat']:.2f}"),
                html.Td(int(r["cell_count"])),
                html.Td(radius_text(r), className="radius-over" if is_wide_region(r) else None),
                html.Td(f"{r['max_z_score']:.1f}"),
                html.Td(int(r["max_event_count"])),
                html.Td(r["cells"], style={"fontFamily": "monospace", "fontSize": "12px"}),
            ]
        )
        for _, r in regions.iterrows()
    ]
    table = html.Table(
        [html.Thead(html.Tr([html.Th(h, style={"textAlign": "left", "paddingRight": "16px"}) for h in header]))]
        + [html.Tbody(rows)],
        id="regions-table-content",
        style={"width": "100%", "borderCollapse": "collapse"},
    )
    note = wide_regions_note(regions)
    return html.Div([note, table]) if note is not None else table


# --------------------------------------------------------------------------- data access


def load_bvalue_region_options(sources: list[str], time_range: str) -> pd.DataFrame:
    clause, params = time_clause("window_end", time_range)
    return query_df(
        f"""
        SELECT region_id, COUNT(*) AS windows, SUM(CASE WHEN is_alert THEN 1 ELSE 0 END) AS alerts,
               MAX(event_count) AS max_events
        FROM bvalue_windows
        WHERE source = ANY(:sources){clause}
        GROUP BY region_id
        ORDER BY windows DESC, max_events DESC
        LIMIT 60
        """,
        {"sources": sources, **params},
    )


def load_mainshock_options(sources: list[str]) -> pd.DataFrame:
    """Mainshocks with a forecast, newest active first.

    M >= 5 events that belong to the sequence of a larger event (larger_event_id set by the
    streaming job) are not listed: the larger event's forecast covers them. forecast_start and
    clock (the data clock of the source's latest forecast run) let the list mark a forecast that
    was not refitted in that run.
    """
    return query_df(
        """
        SELECT m.event_id, m.event_time, m.magnitude, m.place, m.source, m.active,
               f.next_six_hour_expected, f.fitted_at,
               m.event_time + make_interval(secs => f.forecast_origin_hours * 3600) AS forecast_start,
               r.clock
        FROM mainshocks m
        JOIN LATERAL (
            SELECT next_six_hour_expected, fitted_at, forecast_origin_hours FROM aftershock_forecasts af
            WHERE af.mainshock_id = m.event_id ORDER BY af.fitted_at DESC, af.id DESC LIMIT 1
        ) f ON TRUE
        LEFT JOIN forecast_runs r ON r.source = m.source
        WHERE m.source = ANY(:sources) AND m.larger_event_id IS NULL
        ORDER BY m.active DESC, m.event_time DESC
        LIMIT 200
        """,
        {"sources": sources},
    )


def load_forecast_detail(mainshock_id: str) -> tuple[dict | None, dict | None, list[float]]:
    mainshock = query_df(
        """
        SELECT m.event_id, m.event_time, m.magnitude, m.longitude, m.latitude, m.place, m.source, m.active,
               (SELECT r.clock FROM forecast_runs r WHERE r.source = m.source) AS clock
        FROM mainshocks m WHERE m.event_id = :id
        """,
        {"id": mainshock_id},
    )
    if mainshock.empty:
        return None, None, []
    main = mainshock.iloc[0].to_dict()
    forecast = query_df(
        "SELECT * FROM aftershock_forecasts WHERE mainshock_id = :id ORDER BY fitted_at DESC, id DESC LIMIT 1",
        {"id": mainshock_id},
    )
    # The same aftershock search as the streaming job (compute_aftershock_forecasts).
    lat0, lat1, lon0, lon1, lon2, lon3 = search_box_params(float(main["longitude"]), float(main["latitude"]))
    candidates = query_df(
        """
        SELECT event_time, longitude, latitude FROM seismic_events
        WHERE source = :source AND event_time > :t0 AND event_time <= :t0 + make_interval(days => :days)
          AND magnitude < :mag AND COALESCE(LOWER(event_type), 'earthquake') = 'earthquake'
          AND latitude BETWEEN :lat0 AND :lat1
          AND (longitude BETWEEN :lon0 AND :lon1 OR longitude BETWEEN :lon2 AND :lon3)
        ORDER BY event_time
        """,
        {
            "source": main["source"],
            "t0": main["event_time"],
            "days": AFTERSHOCK_WINDOW_DAYS,
            "mag": float(main["magnitude"]),
            "lat0": lat0,
            "lat1": lat1,
            "lon0": lon0,
            "lon1": lon1,
            "lon2": lon2,
            "lon3": lon3,
        },
    )
    rows = []
    if not candidates.empty:
        rows = [
            (r.event_time.to_pydatetime(), r.longitude, r.latitude) for r in candidates.itertuples(index=False)
        ]
    main_time = pd.Timestamp(main["event_time"]).to_pydatetime()
    hours = aftershock_hours(main_time, float(main["longitude"]), float(main["latitude"]), rows, AFTERSHOCK_RADIUS_KM)
    return main, (forecast.iloc[0].to_dict() if not forecast.empty else None), hours


# --------------------------------------------------------------------------- layout

controls = html.Div(
    [
        html.Div(
            [
                html.Label("Data source", htmlFor="source", style=LABEL_STYLE),
                dcc.RadioItems(
                    id="source",
                    options=SOURCE_OPTIONS,
                    value="all",
                    inline=True,
                    inputStyle={"marginRight": "4px", "marginLeft": "10px"},
                ),
            ]
        ),
        html.Div(
            [
                html.Label("Time range", htmlFor="time-range", style=LABEL_STYLE),
                dcc.Dropdown(id="time-range", options=TIME_RANGE_OPTIONS, value="7d", clearable=False),
            ]
        ),
        html.Div(
            [
                html.Label("Minimum magnitude (map and event count)", htmlFor="min-mag", style=LABEL_STYLE),
                dcc.Slider(
                    id="min-mag",
                    min=0,
                    max=7,
                    step=0.5,
                    value=0,
                    marks={0: "all", 1: "1", 2: "2", 3: "3", 4: "4", 5: "5", 6: "6", 7: "7"},
                ),
            ]
        ),
        html.Div(
            [
                html.Label("Auto-refresh", htmlFor="auto-refresh", style=LABEL_STYLE),
                dcc.Checklist(
                    id="auto-refresh",
                    options=[{"label": f" every {REFRESH_MS // 1000} s", "value": "on"}],
                    value=["on"],
                ),
            ]
        ),
    ],
    # Four columns on a wide screen, two on a tablet, one on a phone (assets/responsive.css).
    className="card controls-grid",
    id="controls",
)

app.layout = html.Div(
    [
        html.Div(
            [
                html.H1("Real-Time Seismic Activity Analysis", style={"marginBottom": "4px"}),
                html.P(
                    "Kafka + Spark Structured Streaming + PostgreSQL + Plotly Dash, fed by the USGS GeoJSON feed "
                    "or the replay demo catalogue",
                    style={"color": "#4b5563", "marginTop": 0},
                ),
                html.Div(id="last-updated", style={"color": "#6b7280", "fontSize": "13px", "marginBottom": "12px"}),
            ]
        ),
        dcc.Interval(id="refresh", interval=REFRESH_MS, n_intervals=0),
        # The data source the region and mainshock selections were made for.
        dcc.Store(id="selector-source"),
        controls,
        html.Div(id="metrics", className="metrics-grid"),
        html.Div([dcc.Graph(id="world-map")], className="card", style={"marginTop": "16px"}),
        html.Div(
            [
                html.H3("Active seismic regions (K-Means over anomalous cells)", style={"marginTop": 0}),
                html.Div(id="regions-table"),
            ],
            className="card",
            style={"marginTop": "16px", "overflowX": "auto"},
        ),
        html.Div(
            [
                html.Div(
                    [
                        html.H3("Gutenberg–Richter b-value (24 h sliding windows)", className="card-title"),
                        html.Label("Regions (10° × 10°)", htmlFor="bvalue-regions", style=LABEL_STYLE),
                        dcc.Dropdown(id="bvalue-regions", multi=True, placeholder="Choose regions"),
                        dcc.Graph(id="bvalue-chart"),
                    ],
                    className="card",
                ),
                html.Div(
                    [
                        html.H3("Omori–Utsu aftershock forecast", className="card-title"),
                        html.Label("Mainshock (M ≥ 5)", htmlFor="mainshock", style=LABEL_STYLE),
                        dcc.Dropdown(id="mainshock", placeholder="Choose a mainshock"),
                        html.Div(id="forecast-summary", className="forecast-summary"),
                        dcc.Graph(id="forecast-chart"),
                    ],
                    className="card",
                ),
            ],
            className="charts-grid",
        ),
    ],
    className="page",
)


# --------------------------------------------------------------------------- callbacks


@app.callback(Output("refresh", "disabled"), Input("auto-refresh", "value"))
def toggle_refresh(value):
    return "on" not in (value or [])


def pick_regions(options: pd.DataFrame, current: list[str] | None) -> tuple[list[dict], list[str]]:
    if options.empty:
        return [], []
    opts = [
        {
            "label": f"{r.region_id}  ({int(r.windows)} windows{', ' + str(int(r.alerts)) + ' alerts' if r.alerts else ''})",
            "value": r.region_id,
        }
        for r in options.itertuples(index=False)
    ]
    valid = {o["value"] for o in opts}
    kept = [v for v in (current or []) if v in valid]
    # Defaults on first load (None) or when every chosen region has gone; a selection the
    # user cleared on purpose ([]) stays empty so the next refresh does not refill it.
    if not kept and (current is None or len(current) > 0):
        kept = [o["value"] for o in opts[:DEFAULT_REGIONS]]
    return opts, kept


def pick_mainshock(options: pd.DataFrame, current: str | None) -> tuple[list[dict], str | None]:
    if options.empty:
        return [], None
    def status(r) -> str:
        if not r.active:
            return " (inactive)"
        start, clock = getattr(r, "forecast_start", None), getattr(r, "clock", None)
        return f" (not refitted since {pd.Timestamp(start).round('s'):%m-%d %H:%M})" if is_stale(start, clock) else ""

    opts = [
        {
            "label": f"M{r.magnitude:.1f} {r.place or r.event_id} · {pd.Timestamp(r.event_time):%Y-%m-%d %H:%M} UTC"
            f" · next 6 h ≈ {r.next_six_hour_expected:.2f}" + status(r),
            "value": r.event_id,
        }
        for r in options.itertuples(index=False)
    ]
    valid = {o["value"] for o in opts}
    return opts, current if current in valid else opts[0]["value"]


@app.callback(
    Output("bvalue-regions", "options"),
    Output("bvalue-regions", "value"),
    Output("mainshock", "options"),
    Output("mainshock", "value"),
    Output("selector-source", "data"),
    Input("refresh", "n_intervals"),
    Input("source", "value"),
    Input("time-range", "value"),
    State("bvalue-regions", "value"),
    State("mainshock", "value"),
    State("selector-source", "data"),
)
def update_selectors(_n, source, time_range, current_regions, current_mainshock, selections_source):
    sources = selected_sources(source)
    if selections_source != source:
        # A different data source has different regions and mainshocks: start from its defaults.
        # Comparing with the stored source (not just "the source radio triggered this") also
        # covers an auto-refresh that fires while the source change is still in flight, which
        # used to win and keep the previous source's selection.
        current_regions, current_mainshock = None, None
    region_opts, region_value = pick_regions(load_bvalue_region_options(sources, time_range), current_regions)
    main_opts, main_value = pick_mainshock(load_mainshock_options(sources), current_mainshock)
    return region_opts, region_value, main_opts, main_value, source


@app.callback(
    Output("metrics", "children"),
    Output("world-map", "figure"),
    Output("regions-table", "children"),
    Output("bvalue-chart", "figure"),
    Output("forecast-chart", "figure"),
    Output("forecast-summary", "children"),
    Output("last-updated", "children"),
    Input("refresh", "n_intervals"),
    Input("source", "value"),
    Input("time-range", "value"),
    Input("min-mag", "value"),
    Input("bvalue-regions", "value"),
    Input("mainshock", "value"),
)
def refresh(_n, source, time_range, min_mag, regions_selected, mainshock_id):
    sources = selected_sources(source)
    ev_clause, ev_params = time_clause("event_time", time_range)
    mag_clause = " AND magnitude >= :min_mag" if (min_mag or 0) > 0 else ""
    params = {"sources": sources, "min_mag": float(min_mag or 0), **ev_params}
    events = query_df(
        f"""
        SELECT event_id, event_time, magnitude, longitude, latitude, place, source
        FROM seismic_events
        WHERE source = ANY(:sources){ev_clause}{mag_clause}
        ORDER BY event_time DESC
        LIMIT 5000
        """,
        params,
    )
    summary = query_df(
        f"""
        SELECT COUNT(*) AS events, MAX(event_time) AS latest
        FROM seismic_events
        WHERE source = ANY(:sources){ev_clause}{mag_clause}
        """,
        params,
    )
    win_clause, win_params = time_clause("window_end", time_range)
    cells = query_df(
        f"""
        SELECT source, cell_id, grid_lon, grid_lat, MAX(event_count) AS max_count, MAX(z_score) AS max_z,
               (ARRAY_AGG(cluster_id ORDER BY window_end DESC))[1] AS cluster_id, COUNT(*) AS windows
        FROM anomaly_windows
        WHERE source = ANY(:sources){win_clause}
        GROUP BY source, cell_id, grid_lon, grid_lat
        ORDER BY max_z DESC
        LIMIT 500
        """,
        {"sources": sources, **win_params},
    )
    regions = query_df(
        "SELECT * FROM active_regions WHERE source = ANY(:sources) ORDER BY source, cluster_id",
        {"sources": sources},
    )
    bvalues = pd.DataFrame()
    if regions_selected:
        bvalues = query_df(
            f"""
            SELECT source, region_id, window_start, window_end, event_count, b_value, b_sigma, long_term_b, is_alert
            FROM bvalue_windows
            WHERE source = ANY(:sources) AND region_id = ANY(:regions){win_clause}
            ORDER BY window_end ASC
            LIMIT 5000
            """,
            {"sources": sources, "regions": list(regions_selected), **win_params},
        )
    bstats = query_df(
        f"""
        SELECT COUNT(*) AS windows, COALESCE(SUM(CASE WHEN is_alert THEN 1 ELSE 0 END), 0) AS alerts
        FROM bvalue_windows WHERE source = ANY(:sources){win_clause}
        """,
        {"sources": sources, **win_params},
    )
    # Mainshocks only: M >= 5 events inside the sequence of a larger event are not counted.
    mainshocks = query_df(
        "SELECT COUNT(*) AS count FROM mainshocks WHERE active = TRUE AND larger_event_id IS NULL "
        "AND source = ANY(:sources)",
        {"sources": sources},
    )
    main, forecast, hours = load_forecast_detail(mainshock_id) if mainshock_id else (None, None, [])

    n_events = int(summary["events"].iloc[0]) if not summary.empty else 0
    n_windows = int(bstats["windows"].iloc[0]) if not bstats.empty else 0
    n_alerts = int(bstats["alerts"].iloc[0]) if not bstats.empty else 0
    cards = [
        metric_card("Events", f"{n_events:,}", "metric-events"),
        metric_card("Anomalous cells", len(cells), "metric-anomalous-cells"),
        metric_card("Active regions (K-Means)", len(regions), "metric-active-regions"),
        metric_card("b-value windows", f"{n_windows:,}", "metric-bvalue-windows"),
        metric_card("b-value alerts", n_alerts, "metric-bvalue-alerts"),
        metric_card("Active mainshocks", int(mainshocks["count"].iloc[0]) if not mainshocks.empty else 0,
                    "metric-mainshocks"),
    ]
    latest = summary["latest"].iloc[0] if not summary.empty else None
    latest_text = f"{pd.Timestamp(latest):%Y-%m-%d %H:%M:%S} UTC" if latest is not None and pd.notna(latest) else "none"
    updated = (
        f"Updated {datetime.now(tz=timezone.utc):%Y-%m-%d %H:%M:%S} UTC · source: "
        f"{', '.join(source_label(s) for s in sources)} · latest event in view: {latest_text}"
    )
    return (
        cards,
        build_map(events, cells, regions),
        regions_table(regions),
        build_bvalue_chart(bvalues),
        build_forecast_chart(main, forecast, hours),
        forecast_summary(main, forecast),
        updated,
    )


if __name__ == "__main__":
    app.run(host=DASH_HOST, port=DASH_PORT, debug=False)
