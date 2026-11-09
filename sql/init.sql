CREATE TABLE IF NOT EXISTS seismic_events (
    event_id TEXT PRIMARY KEY,
    event_time TIMESTAMPTZ NOT NULL,
    ingestion_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    magnitude DOUBLE PRECISION,
    depth_km DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    place TEXT,
    event_type TEXT,
    status TEXT,
    tsunami INTEGER,
    significance INTEGER,
    station_count INTEGER,
    rms DOUBLE PRECISION,
    azimuthal_gap DOUBLE PRECISION,
    felt_count INTEGER,
    url TEXT,
    grid_lon INTEGER,
    grid_lat INTEGER,
    cell_id TEXT,
    region_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_seismic_events_time ON seismic_events(event_time DESC);
CREATE INDEX IF NOT EXISTS idx_seismic_events_cell_time ON seismic_events(cell_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_seismic_events_region_time ON seismic_events(region_id, event_time DESC);

CREATE TABLE IF NOT EXISTS anomaly_windows (
    id BIGSERIAL PRIMARY KEY,
    batch_id BIGINT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    cell_id TEXT NOT NULL,
    grid_lon INTEGER,
    grid_lat INTEGER,
    event_count BIGINT NOT NULL,
    baseline_mean DOUBLE PRECISION,
    baseline_std DOUBLE PRECISION,
    z_score DOUBLE PRECISION,
    cluster_id INTEGER,
    centroid_lon DOUBLE PRECISION,
    centroid_lat DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (batch_id, window_start, window_end, cell_id)
);

CREATE INDEX IF NOT EXISTS idx_anomaly_windows_time ON anomaly_windows(window_end DESC);

CREATE TABLE IF NOT EXISTS bvalue_windows (
    id BIGSERIAL PRIMARY KEY,
    batch_id BIGINT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    region_id TEXT NOT NULL,
    event_count INTEGER NOT NULL,
    completeness_mag DOUBLE PRECISION,
    b_value DOUBLE PRECISION,
    b_sigma DOUBLE PRECISION,
    long_term_b DOUBLE PRECISION,
    deviation DOUBLE PRECISION,
    is_alert BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (batch_id, window_start, window_end, region_id)
);

CREATE INDEX IF NOT EXISTS idx_bvalue_region_time ON bvalue_windows(region_id, window_end DESC);

CREATE TABLE IF NOT EXISTS mainshocks (
    event_id TEXT PRIMARY KEY REFERENCES seismic_events(event_id) ON DELETE CASCADE,
    event_time TIMESTAMPTZ NOT NULL,
    magnitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    place TEXT,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS aftershock_forecasts (
    id BIGSERIAL PRIMARY KEY,
    batch_id BIGINT NOT NULL,
    mainshock_id TEXT NOT NULL REFERENCES mainshocks(event_id) ON DELETE CASCADE,
    fitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    aftershock_count INTEGER NOT NULL,
    k_value DOUBLE PRECISION,
    c_value DOUBLE PRECISION,
    p_value DOUBLE PRECISION,
    next_hour_rate DOUBLE PRECISION,
    next_six_hour_expected DOUBLE PRECISION,
    r_squared DOUBLE PRECISION,
    UNIQUE (batch_id, mainshock_id)
);

CREATE INDEX IF NOT EXISTS idx_forecasts_mainshock_time ON aftershock_forecasts(mainshock_id, fitted_at DESC);
