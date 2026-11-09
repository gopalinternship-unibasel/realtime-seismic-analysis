from __future__ import annotations

import os

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, Input, Output, dcc, html
from sqlalchemy import create_engine, text

DB_USER = os.getenv("POSTGRES_USER", "seismic")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "seismic")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "seismic")
DASH_HOST = os.getenv("DASH_HOST", "127.0.0.1")
DASH_PORT = int(os.getenv("DASH_PORT", "8050"))

ENGINE = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

app = Dash(__name__)
app.title = "Real-Time Seismic Activity Analysis"

CARD_STYLE = {
    "border": "1px solid #e5e7eb",
    "borderRadius": "12px",
    "padding": "16px",
    "boxShadow": "0 1px 3px rgba(0,0,0,0.08)",
    "backgroundColor": "white",
}


def query_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    try:
        return pd.read_sql_query(text(sql), ENGINE, params=params or {})
    except Exception as exc:  # noqa: BLE001 - dashboard should stay up while pipeline starts.
        print(f"Dashboard query failed: {exc}", flush=True)
        return pd.DataFrame()


def build_map(events: pd.DataFrame, anomalies: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if not events.empty:
        fig.add_trace(
            go.Scattergeo(
                lon=events["longitude"],
                lat=events["latitude"],
                text=events.apply(
                    lambda r: f"{r.get('place', '')}<br>M {r.get('magnitude', '')}<br>{r.get('event_time', '')}", axis=1
                ),
                mode="markers",
                marker={
                    "size": events["magnitude"].fillna(1).clip(lower=0.5) * 3 + 3,
                    "color": events["magnitude"].fillna(0),
                    "colorscale": "Viridis",
                    "showscale": True,
                    "colorbar": {"title": "Magnitude"},
                    "opacity": 0.72,
                },
                name="Events",
            )
        )
    if not anomalies.empty:
        fig.add_trace(
            go.Scattergeo(
                lon=anomalies["grid_lon"] + 0.5,
                lat=anomalies["grid_lat"] + 0.5,
                text=anomalies.apply(
                    lambda r: f"Anomaly cell {r['cell_id']}<br>count={r['event_count']}<br>z={r['z_score']:.2f}<br>cluster={r['cluster_id']}",
                    axis=1,
                ),
                mode="markers",
                marker={"size": 16, "symbol": "square-open", "line": {"width": 3}},
                name="Anomalous grid cells",
            )
        )
        centroids = anomalies.dropna(subset=["centroid_lon", "centroid_lat"]).drop_duplicates("cluster_id")
        if not centroids.empty:
            fig.add_trace(
                go.Scattergeo(
                    lon=centroids["centroid_lon"],
                    lat=centroids["centroid_lat"],
                    text=centroids["cluster_id"].apply(lambda c: f"Cluster centroid {c}"),
                    mode="markers",
                    marker={"size": 12, "symbol": "x"},
                    name="Cluster centroids",
                )
            )
    fig.update_geos(showland=True, landcolor="rgb(243,244,246)", showcountries=True, showocean=True, oceancolor="rgb(229,247,255)")
    fig.update_layout(margin={"r": 0, "t": 20, "l": 0, "b": 0}, height=520, legend={"orientation": "h"})
    return fig


def build_bvalue_chart(bvalues: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if not bvalues.empty:
        for region, group in bvalues.groupby("region_id"):
            fig.add_trace(
                go.Scatter(
                    x=group["window_end"],
                    y=group["b_value"],
                    mode="lines+markers",
                    name=str(region),
                    error_y={"type": "data", "array": group["b_sigma"], "visible": True},
                )
            )
    fig.update_layout(
        title="Gutenberg–Richter b-value by region",
        xaxis_title="Window end",
        yaxis_title="b-value",
        margin={"r": 20, "t": 50, "l": 50, "b": 40},
        height=360,
    )
    return fig


def build_forecast_chart(forecasts: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if not forecasts.empty:
        latest = forecasts.sort_values("fitted_at").tail(30)
        fig.add_trace(
            go.Bar(
                x=latest["mainshock_id"],
                y=latest["next_six_hour_expected"],
                text=latest.apply(lambda r: f"p={r['p_value']:.2f}, R²={r['r_squared']:.2f}", axis=1),
                name="Expected aftershocks in next 6h",
            )
        )
    fig.update_layout(
        title="Omori–Utsu aftershock forecast",
        xaxis_title="Mainshock ID",
        yaxis_title="Expected aftershocks",
        margin={"r": 20, "t": 50, "l": 50, "b": 90},
        height=360,
    )
    return fig


app.layout = html.Div(
    [
        html.Div(
            [
                html.H1("Real-Time Seismic Activity Analysis", style={"marginBottom": "4px"}),
                html.P(
                    "Spark Structured Streaming + USGS GeoJSON + PostgreSQL + Plotly Dash",
                    style={"color": "#4b5563", "marginTop": 0},
                ),
            ]
        ),
        dcc.Interval(id="refresh", interval=10_000, n_intervals=0),
        html.Div(id="metrics", style={"display": "grid", "gridTemplateColumns": "repeat(4, 1fr)", "gap": "12px"}),
        html.Div([dcc.Graph(id="world-map")], style={**CARD_STYLE, "marginTop": "16px"}),
        html.Div(
            [
                html.Div([dcc.Graph(id="bvalue-chart")], style=CARD_STYLE),
                html.Div([dcc.Graph(id="forecast-chart")], style=CARD_STYLE),
            ],
            style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px", "marginTop": "16px"},
        ),
    ],
    style={"fontFamily": "Inter, system-ui, -apple-system, Segoe UI, sans-serif", "padding": "24px", "backgroundColor": "#f9fafb"},
)


@app.callback(
    Output("metrics", "children"),
    Output("world-map", "figure"),
    Output("bvalue-chart", "figure"),
    Output("forecast-chart", "figure"),
    Input("refresh", "n_intervals"),
)
def refresh(_n: int):
    events = query_df(
        """
        SELECT event_id, event_time, magnitude, longitude, latitude, place
        FROM seismic_events
        WHERE event_time > now() - interval '9000 days'
        ORDER BY event_time DESC
        LIMIT 1000
        """
    )
    anomalies = query_df(
        """
        SELECT * FROM anomaly_windows
        WHERE window_end > now() - interval '9000 days'
        ORDER BY created_at DESC
        LIMIT 300
        """
    )
    bvalues = query_df(
        """
        SELECT * FROM bvalue_windows
        WHERE window_end > now() - interval '9000 days'
        ORDER BY window_end ASC
        LIMIT 1000
        """
    )
    forecasts = query_df(
        """
        SELECT * FROM aftershock_forecasts
        ORDER BY fitted_at DESC
        LIMIT 100
        """
    )
    mainshocks = query_df("SELECT COUNT(*) AS count FROM mainshocks WHERE active = TRUE")

    cards = [
        ("Events", len(events)),
        ("Anomalous cells", len(anomalies)),
        ("b-value windows", len(bvalues)),
        ("Active mainshocks", int(mainshocks["count"].iloc[0]) if not mainshocks.empty else 0),
    ]
    metric_cards = [
        html.Div([html.Div(label, style={"color": "#6b7280"}), html.H2(str(value), style={"margin": "4px 0"})], style=CARD_STYLE)
        for label, value in cards
    ]
    return metric_cards, build_map(events, anomalies), build_bvalue_chart(bvalues), build_forecast_chart(forecasts)


if __name__ == "__main__":
    app.run_server(host=DASH_HOST, port=DASH_PORT, debug=False)
