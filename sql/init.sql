-- Schema for the seismic pipeline.
-- PostgreSQL runs this file when the pgdata volume is first created, and the
-- streaming job runs it again on every start, so every statement must be
-- idempotent (IF NOT EXISTS). The ALTER statements upgrade databases created
-- by older versions of this file.

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
    region_id TEXT,
    -- 'usgs-live' (live USGS feed) or 'replay' (demo catalogue); analytics never mix the two.
    source TEXT NOT NULL DEFAULT 'unknown'
);
ALTER TABLE seismic_events ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'unknown';

CREATE INDEX IF NOT EXISTS idx_seismic_events_time ON seismic_events(event_time DESC);
CREATE INDEX IF NOT EXISTS idx_seismic_events_cell_time ON seismic_events(cell_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_seismic_events_region_time ON seismic_events(region_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_seismic_events_source_time ON seismic_events(source, event_time DESC);

-- One row per flagged 6-hour sliding window and grid cell. Windows are recomputed
-- from all stored events whenever a micro-batch adds events to them (upsert).
CREATE TABLE IF NOT EXISTS anomaly_windows (
    id BIGSERIAL PRIMARY KEY,
    batch_id BIGINT NOT NULL,
    source TEXT NOT NULL DEFAULT 'unknown',
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
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE anomaly_windows ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE anomaly_windows ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE anomaly_windows DROP CONSTRAINT IF EXISTS anomaly_windows_batch_id_window_start_window_end_cell_id_key;
CREATE UNIQUE INDEX IF NOT EXISTS uq_anomaly_windows_window_cell
    ON anomaly_windows(source, window_start, window_end, cell_id);
CREATE INDEX IF NOT EXISTS idx_anomaly_windows_time ON anomaly_windows(window_end DESC);

-- One row per 24-hour sliding window and 10 x 10 degree region (upsert, like anomaly_windows).
CREATE TABLE IF NOT EXISTS bvalue_windows (
    id BIGSERIAL PRIMARY KEY,
    batch_id BIGINT NOT NULL,
    source TEXT NOT NULL DEFAULT 'unknown',
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
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE bvalue_windows ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE bvalue_windows ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE bvalue_windows DROP CONSTRAINT IF EXISTS bvalue_windows_batch_id_window_start_window_end_region_id_key;
CREATE UNIQUE INDEX IF NOT EXISTS uq_bvalue_windows_window_region
    ON bvalue_windows(source, window_start, window_end, region_id);
CREATE INDEX IF NOT EXISTS idx_bvalue_region_time ON bvalue_windows(region_id, window_end DESC);

-- K-Means groups of the currently anomalous cells (replaced per source after each micro-batch).
CREATE TABLE IF NOT EXISTS active_regions (
    source TEXT NOT NULL,
    cluster_id INTEGER NOT NULL,
    centroid_lon DOUBLE PRECISION NOT NULL,
    centroid_lat DOUBLE PRECISION NOT NULL,
    cell_count INTEGER NOT NULL,
    cells TEXT,
    max_z_score DOUBLE PRECISION,
    max_event_count BIGINT,
    radius_km DOUBLE PRECISION,
    k INTEGER,
    batch_id BIGINT,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- REGION_RADIUS_KM and MAX_KMEANS_CLUSTERS of the clustering run. within_radius is FALSE for a
    -- region wider than radius_limit_km: k reached max_k before every cell was within the limit.
    radius_limit_km DOUBLE PRECISION,
    max_k INTEGER,
    within_radius BOOLEAN,
    PRIMARY KEY (source, cluster_id)
);
ALTER TABLE active_regions ADD COLUMN IF NOT EXISTS radius_limit_km DOUBLE PRECISION;
ALTER TABLE active_regions ADD COLUMN IF NOT EXISTS max_k INTEGER;
ALTER TABLE active_regions ADD COLUMN IF NOT EXISTS within_radius BOOLEAN;

-- Kafka messages the cleaning rules rejected (not JSON, no id or time, coordinates missing or out
-- of range, a numeric field that is not a number), one row per message, with the reason.
CREATE TABLE IF NOT EXISTS rejected_records (
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    received_at TIMESTAMPTZ,
    batch_id BIGINT NOT NULL,
    reason TEXT NOT NULL,
    event_id TEXT,
    source TEXT,
    -- The message text, cut to 2,000 characters.
    raw_value TEXT,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (kafka_partition, kafka_offset)
);

CREATE TABLE IF NOT EXISTS mainshocks (
    event_id TEXT PRIMARY KEY REFERENCES seismic_events(event_id) ON DELETE CASCADE,
    event_time TIMESTAMPTZ NOT NULL,
    magnitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    place TEXT,
    source TEXT NOT NULL DEFAULT 'unknown',
    active BOOLEAN NOT NULL DEFAULT TRUE,
    -- Set when a larger M >= 5 event of the same source lies within 300 km and 7 days of this
    -- one (this event is its aftershock or foreshock). Only rows with NULL here are forecast.
    larger_event_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE mainshocks ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE mainshocks ADD COLUMN IF NOT EXISTS larger_event_id TEXT;
CREATE INDEX IF NOT EXISTS idx_mainshocks_source_time ON mainshocks(source, event_time DESC);

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
    -- Hours after the mainshock at which the forecast starts (the source's data clock).
    forecast_origin_hours DOUBLE PRECISION,
    -- FALSE when the sequence was too short to fit p and the generic p was used.
    p_estimated BOOLEAN NOT NULL DEFAULT TRUE,
    UNIQUE (batch_id, mainshock_id)
);
ALTER TABLE aftershock_forecasts ADD COLUMN IF NOT EXISTS forecast_origin_hours DOUBLE PRECISION;
ALTER TABLE aftershock_forecasts ADD COLUMN IF NOT EXISTS p_estimated BOOLEAN NOT NULL DEFAULT TRUE;

CREATE INDEX IF NOT EXISTS idx_forecasts_mainshock_time ON aftershock_forecasts(mainshock_id, fitted_at DESC);

-- The data clock (latest event time) each source was last forecast at, written in the same
-- transaction as the forecasts. An active mainshock whose latest forecast starts before its
-- source's clock here was not refitted in the latest run; the dashboard marks it.
CREATE TABLE IF NOT EXISTS forecast_runs (
    source TEXT PRIMARY KEY,
    clock TIMESTAMPTZ NOT NULL,
    batch_id BIGINT NOT NULL,
    mainshocks INTEGER NOT NULL,
    forecasts INTEGER NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
