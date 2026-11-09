# Real-Time Seismic Activity Analysis and Aftershock Forecasting with Apache Spark and Kafka

This is a runnable implementation of the DIS workshop idea from the abstract: a streaming seismic analytics system using the USGS real-time GeoJSON feed, Apache Kafka (KRaft), Spark Structured Streaming on a standalone cluster, PostgreSQL, and a Plotly Dash dashboard, all started with Docker Compose.

For exact start, stop and test commands on Windows see **[RUNNING.md](RUNNING.md)**.

## What is implemented

1. **Kafka streaming source**
   - `src/producer/live_usgs_producer.py` loads the USGS `all_week.geojson` feed once at start-up (backfill), then polls `all_hour.geojson` every 60 s and publishes only events it has not sent before.
   - `src/producer/replay_producer.py` replays `data/replay_catalog/sample_replay_events.jsonl` (184 synthetic events: a Northern California `M5.6` sequence, events in Japan and Chile, background events). By default the catalogue is shifted in time so its last event happens within the last hour, so the replay behaves like live data. The shift is a whole number of hours, so the events keep their place in the hourly window grid and every start gives the same windows.
   - 84 of those records are the original hand-made demo; the other 100 are aftershocks of the M5.6 added by `src/producer/extend_replay_catalog.py` (seeded; Omori-Utsu times with p = 1.08, Gutenberg-Richter magnitudes with b = 1.0 above M 2.0, within 20 km). They were added because the hand-made sequence alone has too few events for a 24-hour b-value with at least 20 events above Mc.
   - Producers publish one JSON earthquake event per Kafka message (key = `event_id`) to the `seismic-events` topic. Each record carries `source` = `usgs-live` or `replay`.

2. **Spark Structured Streaming job** (`src/streaming/streaming_job.py`, submitted to the Spark master with 2 workers)
   - Consumes Kafka with Spark's Kafka connector and parses each message with a fixed schema. Every field is read as text and converted, so a number sent as text (`"3.0"`) is a number.
   - Cleans: a message is rejected when it is not a JSON object, has no `event_id`, has no `event_time` or one that cannot be parsed, has no longitude or latitude, has a numeric field that holds something that is not a finite number (`"strong"`, `true`, `NaN`), or has a longitude outside ±180 or a latitude outside ±90. A JSON `null` magnitude is allowed (stored as NULL). Rejected messages are not dropped silently: each is stored in `rejected_records` with its reason, Kafka partition and offset, and the micro-batch log line counts them by reason.
   - Deduplicates by `event_id` across micro-batches (`dropDuplicatesWithinWatermark`) with a 7-day watermark on the **arrival time** (the Kafka message timestamp), so the state stays bounded and no event is dropped for being old: events from any date are stored and analysed (a USGS event 10 days late, the replay catalogue without `REPLAY_REBASE_TO_NOW`). Until QA round 2 the watermark was on the event time, and Spark silently dropped every event more than 7 days older than the newest one. A message whose Kafka timestamp is more than 7 days older than the newest one already processed would still be dropped as late; the job then logs a `WARNING` line (a listener on Spark's progress reports).
   - Enriches with 1° × 1° grid cells and 10° × 10° regions.
   - `foreachBatch` stores each micro-batch in PostgreSQL (idempotent on `event_id`) and runs the analytics below. Live and replay data are analysed separately (`source` is part of every window key).

3. **Analytics** (`src/streaming/analytics.py`, `src/common/seismo_models.py`)
   - **Sliding windows across micro-batches:** after each micro-batch, Spark reads back (JDBC) every stored event that can share a window with the batch (the merged 24-hour ranges around the batch's events, so an old event does not pull in every event since) and recomputes the windows the batch touched with `window(event_ts, ...)`. Windows therefore count events from earlier micro-batches and late events update the window they belong to; results are upserted.
   - **Grid anomaly detection:** 6-hour windows sliding by 1 hour; z-score of the window count against the historical per-cell baseline (mean and standard deviation of 6-hour counts).
   - **Historical baselines:** `src/batch/fetch_usgs_history.py` downloads 90 days of USGS earthquakes from the FDSN event service, and the Spark batch job `src/batch/compute_baselines.py` computes per-cell 6-hour rates (empty windows count as zero) and per-region long-term b-values. The shipped `data/baseline/*.csv` come from 32,544 USGS earthquakes between 2026-08-11 and 2026-11-09 (`data/baseline/baseline_metadata.json`).
   - **K-Means active regions:** the cells flagged in the last 24 hours of data are clustered with Spark MLlib `KMeans` on the unit sphere; k starts at a greedy estimate of how many 300 km regions the cells need and grows until every cell lies within 300 km (`REGION_RADIUS_KM`) of its region centroid. `MAX_KMEANS_CLUSTERS` (default 30) only bounds the work: if k reaches it first, the regions of that fit are kept, every region wider than 300 km gets `within_radius = false` in `active_regions` (with `radius_limit_km` and `max_k`), the job logs a `WARNING` and the dashboard marks the region ("over the 300 km limit") and says how to raise the cap. Until QA round 2 the cap was 5: 22 separate hot spots became 5 regions up to 4,478 km wide without any sign. Results are in `active_regions`.
   - **Gutenberg–Richter monitoring:** 24-hour windows sliding by 1 hour per region; `Mc` by maximum curvature but never below the region's long-term Mc (so the window's b is comparable with the long-term b), b-value by Aki–Utsu maximum likelihood (σ = b/√n, at least 20 events above Mc). A window is an alert when |b − long-term b| > max(0.25, 2σ).
   - **Omori–Utsu forecasting:** for every `M >= 5.0` mainshock active in the last 30 days (all of them, of every source, after every micro-batch with new events), aftershocks within 300 km and 7 days are fitted by maximum likelihood to `n(t) = K(t+c)^(-p)` (c fixed at 0.05 days; p fixed at the generic 1.08 for sequences of 3–4 aftershocks), and the expected number of aftershocks in the next hour and next six hours after "now" is stored. A new row is written only when the fit changed (the source's clock moved or aftershocks arrived), so each mainshock's latest row is always its current forecast. The clock of every forecast run is stored per source in `forecast_runs`, in the same transaction; the dashboard flags a listed forecast that does not start at that clock (comparing with the newest event instead would flag every forecast while a micro-batch with new events is still running).
   - **Mainshocks are declustered:** an M ≥ 5 event that has a larger event of the same source within 300 km and 7 days (before it: it is an aftershock; after it: it was a foreshock) is not a mainshock. Its row in `mainshocks` gets `larger_event_id`, it gets no forecast of its own, and the larger event's forecast counts it among its aftershocks. Without this, each M5 aftershock of a large event was forecast as a mainshock that counted the rest of the sequence as its own aftershocks.
   - The aftershock search box is as wide as the 300 km radius at every latitude and wraps across the antimeridian (Fiji, Tonga, the Aleutians); a fixed ±4° box missed aftershocks east and west of Alaskan mainshocks.

4. **Storage and visualization**
   - PostgreSQL tables are created by `sql/init.sql` (the streaming job re-applies it on start, so older databases are upgraded).
   - Dash dashboard (`src/dashboard/app.py`) with controls for data source (live, replay, both), time range, minimum magnitude, auto-refresh, b-value regions and mainshock. It shows:
     - world map with events, anomalous grid cells and K-Means region centroids (hover for details)
     - table of active seismic regions
     - evolving b-value chart with error bars and alerts
     - Omori–Utsu chart: observed aftershocks per hour, fitted decay curve and the six-hour forecast
   - If PostgreSQL cannot be read, the page says so (a red "Database unavailable" alert with the error, "n/a" in every metric card, "No data: the database cannot be read" in the map and charts) instead of showing zeros, keeps the region and mainshock selections, and recovers by itself at the next refresh once the database answers. Until QA round 2 an outage showed "Events 0, Anomalous cells 0, ..." like an empty feed.

## Project structure

```text
.
├── docker-compose.yml          # project name cvfix-seismic; profiles app, replay, live, batch, test
├── Dockerfile.spark            # one image for Spark master/workers, streaming job, producers, batch, tests
├── Dockerfile.dashboard
├── requirements-spark.txt / requirements-dashboard.txt / requirements-test.txt
├── sql/init.sql
├── scripts
│   ├── start.ps1  stop.ps1  check.ps1  test.ps1  refresh_baselines.ps1   # Windows PowerShell
│   └── reset_demo.sh  run_batch_baselines.sh                             # bash
├── src
│   ├── batch/fetch_usgs_history.py
│   ├── batch/compute_baselines.py
│   ├── common/config.py
│   ├── common/geo.py
│   ├── common/kafka_io.py
│   ├── common/seismo_models.py
│   ├── dashboard/app.py
│   ├── producer/live_usgs_producer.py
│   ├── producer/replay_producer.py
│   ├── producer/extend_replay_catalog.py   # adds the seeded synthetic aftershocks to the demo catalogue
│   ├── streaming/analytics.py
│   └── streaming/streaming_job.py
├── tests
│   ├── pipeline/               # pytest + local Spark (run in the Spark image)
│   ├── dashboard/              # pytest (run in the dashboard image)
│   └── e2e/dashboard.e2e.mjs   # headless browser check of the running dashboard
└── data
    ├── baseline/cell_baselines.csv, region_bvalues.csv, baseline_metadata.json
    ├── historical/usgs_historical_events.csv (+ .meta.json), sample_historical_events.csv (old synthetic sample)
    ├── replay_catalog/sample_replay_events.jsonl
    └── static/plate_boundaries_sample.geojson
```

## Requirements

- Docker Desktop with Docker Compose v2 (about 6 GB of free memory for the containers)
- Internet access for the image build, the Spark connector download on first start, and the USGS feeds

No local Spark, Kafka, Python or PostgreSQL installation is required.

## Quick start

Windows PowerShell (see RUNNING.md for details):

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start.ps1            # live USGS feed + replay demo
powershell -ExecutionPolicy Bypass -File .\scripts\check.ps1
powershell -ExecutionPolicy Bypass -File .\scripts\stop.ps1
```

Any shell:

```bash
cp .env.example .env
docker compose --profile app --profile replay --profile live up --build -d
```

Use only one producer with `--profile app --profile replay` or `--profile app --profile live`.

Open:

- Dashboard: <http://localhost:8050>
- Spark master UI: <http://localhost:8080>
- Spark worker 1 UI: <http://localhost:8081>
- Spark worker 2 UI: <http://localhost:8082>
- Kafka broker: internal Docker address `kafka:9092`, host port `localhost:9092`
- PostgreSQL: `localhost:5432`, database, user and password `seismic`

The first micro-batch takes a few minutes on a fresh start (Spark downloads the Kafka and PostgreSQL connectors and warms up). The replay demo then shows anomalies in California, Japan and Chile, three K-Means regions, b-value windows and an Omori forecast for the M5.6 mainshock. The live feed shows whatever the USGS recorded in the last week.

## Kafka commands for checking the pipeline

The Kafka tools are in `/opt/kafka/bin` inside the `kafka` container.

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic seismic-events
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic seismic-events --from-beginning --max-messages 5
```

(In Git Bash on Windows, prefix these with `MSYS_NO_PATHCONV=1` so the `/opt/...` path is not rewritten.)

## Recompute historical baselines

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\refresh_baselines.ps1
```

or `./scripts/run_batch_baselines.sh`, or directly `docker compose up -d --no-recreate spark-master spark-worker-1 spark-worker-2` followed by `docker compose --profile batch run --rm --no-deps baselines` (without `--no-deps`, `run` recreates the Spark master and workers whenever Compose considers them out of date, for example after an image rebuild, and that cuts a running streaming job off from its executors). The job downloads `HISTORY_DAYS` (default 90) days of USGS earthquakes ending today, runs `compute_baselines.py` on the Spark cluster and rewrites `data/baseline/`. Both scripts then restart a running streaming job so it loads the new baselines (with `docker compose ... run` directly, restart it yourself: `docker compose --profile app restart streaming-job`).

The pipeline may keep running meanwhile: each Spark worker offers 3 cores, the streaming job holds at most 4 (`spark.cores.max`, `STREAMING_MAX_CORES`) and the batch job uses the other 2 (`SPARK_BATCH_CORES`). Before, the streaming job held all 4 cores of a 2 + 2 cluster and the batch job waited forever ("Initial job has not accepted any resources"). `src/batch/wait_for_cores.py` runs before the download and before `spark-submit` and stops the job with an error after 3 minutes if the cluster still has no free cores.

## Useful database checks

```bash
docker compose exec postgres psql -U seismic -d seismic -c "SELECT source, COUNT(*) FROM seismic_events GROUP BY source;"
docker compose exec postgres psql -U seismic -d seismic -c "SELECT source, cell_id, event_count, round(z_score::numeric,1) AS z FROM anomaly_windows ORDER BY z_score DESC LIMIT 10;"
docker compose exec postgres psql -U seismic -d seismic -c "SELECT source, cluster_id, centroid_lon, centroid_lat, radius_km, within_radius, cells FROM active_regions;"
docker compose exec postgres psql -U seismic -d seismic -c "SELECT kafka_partition, kafka_offset, reason, event_id, left(raw_value, 60) FROM rejected_records ORDER BY received_at DESC LIMIT 10;"
docker compose exec postgres psql -U seismic -d seismic -c "SELECT source, region_id, window_end, b_value, b_sigma, is_alert FROM bvalue_windows ORDER BY window_end DESC LIMIT 10;"
docker compose exec postgres psql -U seismic -d seismic -c "SELECT mainshock_id, aftershock_count, p_value, next_six_hour_expected FROM aftershock_forecasts ORDER BY fitted_at DESC LIMIT 10;"
```

## Tests

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\test.ps1
```

runs `tests/pipeline` (geo, producers, the cleaning rules with every reject reason, the streaming ingest path with de-duplication by arrival time across micro-batches that keeps old events and logs late arrivals, b-value and Omori models, declustering and the aftershock search box, sliding windows across micro-batches, replay windows that do not depend on the start minute, baselines, K-Means with more separate hot spots than the old cap and the radius flag, the cluster-cores check) with a local SparkSession in the Spark image, `tests/pipeline/test_forecasts_db.py` and `tests/pipeline/test_ingest_db.py` against a throwaway PostgreSQL (`test-postgres`, data in memory: every active mainshock forecast, a large sequence forecast once, unchanged fits not stored twice, antimeridian and high-latitude aftershocks, `process_batch` with rejected records, a text magnitude and a 10-day-old event, the stored region radius flag), and `tests/dashboard` in the dashboard image (including the database-outage state). `tests/e2e/dashboard.e2e.mjs` drives the running dashboard in headless Edge, at desktop and phone width (see RUNNING.md).

## How the pipeline maps to the workshop abstract

| Abstract requirement | Implementation |
|---|---|
| USGS Earthquake Hazards Real-Time GeoJSON feed | `live_usgs_producer.py` polls `all_hour.geojson` (after an `all_week.geojson` backfill) |
| Replay source for deterministic demo | `replay_producer.py` publishes the sample catalogue to Kafka, shifted to the present |
| Message broker / streaming middleware | Kafka (KRaft) service in `docker-compose.yml`, topic `seismic-events`, persistent volume |
| Spark Structured Streaming source | `spark.readStream.format("kafka")` in `streaming_job.py` |
| Deduplication by event ID | Producer seen-ID state for live mode + Spark `dropDuplicatesWithinWatermark(event_id)` (7-day watermark on the Kafka arrival time) + `ON CONFLICT` in PostgreSQL |
| Cleaning | Reject rules in `clean_events()`; rejected messages with their reason in `rejected_records` |
| Historical catalog baselines | `fetch_usgs_history.py` + `compute_baselines.py` → `data/baseline/cell_baselines.csv`, `region_bvalues.csv` |
| 1° × 1° grid cells | `grid_lon`, `grid_lat`, `cell_id` fields |
| 6-hour sliding windows | `window(event_ts, "6 hours", "1 hour")` over stored events, recomputed per micro-batch |
| Z-score anomaly detection | `score_anomalies()` in `analytics.py` |
| K-Means grouping of flagged cells | Spark MLlib `KMeans` in `cluster_active_cells()` → `active_regions` |
| Gutenberg–Richter b-value monitoring | `bvalue_rows()` + `seismo_models.py` |
| Omori–Utsu aftershock model | `compute_aftershock_forecasts()` + `fit_omori_utsu()` (maximum likelihood) |
| PostgreSQL sink after micro-batch | Spark `foreachBatch` writes with `psycopg2` |
| Plotly Dash dashboard | `src/dashboard/app.py` |
| Multi-node Spark cluster | Docker Compose: 1 master + 2 workers |

## Troubleshooting

### Spark fails with a connector error

The streaming job resolves `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5` and `org.postgresql:postgresql:42.7.4` from Maven Central on its first start (cached afterwards in the `ivy-cache` volume). The machine needs internet access the first time.

### The baselines job stops with "fewer than 2 free cores"

Another Spark application holds the cluster's cores (for example a streaming job started with a larger `STREAMING_MAX_CORES`, or a second baselines run). The message names the running applications; stop the one holding the cores (`docker compose --profile app stop streaming-job`) and run the job again.

### Dashboard starts before events appear

The dashboard refreshes every 10 seconds. Wait until the streaming job logs `Batch N: received=...`.

### Dashboard says "Database unavailable"

The dashboard could not query PostgreSQL; the alert gives the driver's error. Check `docker compose ps postgres` and start it again (`start.ps1`); the page recovers at its next refresh.

### Records missing from `seismic_events`

Look in `rejected_records` (or the `rejected=` part of the micro-batch log lines, or `check.ps1`): every message the cleaning rules rejected is there with its reason. Duplicates of an event id are dropped on purpose.

### No anomalies or forecasts in live mode

This is normal when the live USGS feed is quiet; outside the US the feed lists mostly M4+ events, so few mainshocks have enough recorded aftershocks. Use replay mode for a deterministic demo.

### Resetting the demo

Kafka messages, PostgreSQL rows, Spark checkpoints and the live producer's seen-ID state are Docker volumes of the `cvfix-seismic` project, so they are always kept or removed together:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stop.ps1 -Reset
```

(bash: `./scripts/reset_demo.sh`). The replay producer sends the same event IDs every time, so replaying again without a reset adds nothing new (they are deduplicated).

### Port conflicts

If ports `5432`, `7077`, `8080`, `8081`, `8082`, `8050`, or `9092` are already used, change the left-hand side of the relevant `ports:` mapping in `docker-compose.yml`.
