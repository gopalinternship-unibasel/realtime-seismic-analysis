# Real-Time Seismic Activity Analysis and Aftershock Forecasting with Apache Spark and Kafka

This is a runnable implementation of the DIS workshop idea from the abstract: a streaming seismic analytics system using the USGS real-time GeoJSON feed, Apache Kafka, Spark Structured Streaming, PostgreSQL, and a Plotly Dash dashboard.

## What is implemented

1. **Kafka streaming source**
   - `src/producer/live_usgs_producer.py` polls the USGS `all_hour.geojson` feed.
   - `src/producer/replay_producer.py` replays `data/replay_catalog/sample_replay_events.jsonl` at accelerated speed so the demo always shows meaningful clusters, b-value windows, and aftershock forecasts.
   - Producers publish one JSON earthquake event per Kafka message to the `seismic-events` topic.
   - Kafka replaces the previous file-based `data/incoming/` streaming source.

2. **Spark Structured Streaming job**
   - `src/streaming/streaming_job.py` consumes Kafka messages with Spark's Kafka connector.
   - Parses each Kafka message value as JSON using a fixed schema.
   - Deduplicates by `event_id` using watermarking.
   - Computes 1° × 1° spatial grid cells.
   - Uses `foreachBatch` to write each micro-batch into PostgreSQL and run the analytics stages.

3. **Analytics**
   - **Grid anomaly detection:** 6-hour windows, 1-hour step, z-score against historical per-cell baselines.
   - **K-Means grouping:** flagged anomalous cells are clustered by centroid so nearby anomalies become active regions.
   - **Gutenberg–Richter monitoring:** completeness magnitude `Mc` is estimated using maximum curvature, then b-value is estimated by maximum likelihood.
   - **Omori–Utsu forecasting:** after a mainshock `M >= 5.0`, nearby aftershocks are used to fit `n(t) = K(t+c)^(-p)` and forecast the next-hour and next-six-hour rate.

4. **Storage and visualization**
   - PostgreSQL tables are created by `sql/init.sql`.
   - Dash dashboard shows:
     - live world map with events, anomalous cells, and cluster centroids
     - evolving b-value chart with error bars
     - Omori–Utsu aftershock forecast chart

## Project structure

```text
.
├── docker-compose.yml
├── Dockerfile.spark
├── Dockerfile.dashboard
├── requirements-spark.txt
├── requirements-dashboard.txt
├── sql/init.sql
├── src
│   ├── batch/compute_baselines.py
│   ├── common/config.py
│   ├── common/geo.py
│   ├── common/kafka_io.py
│   ├── common/seismo_models.py
│   ├── dashboard/app.py
│   ├── producer/live_usgs_producer.py
│   ├── producer/replay_producer.py
│   └── streaming/streaming_job.py
└── data
    ├── baseline/cell_baselines.csv
    ├── baseline/region_bvalues.csv
    ├── historical/sample_historical_events.csv
    ├── replay_catalog/sample_replay_events.jsonl
    └── static/plate_boundaries_sample.geojson
```

## Requirements

- Docker Desktop
- Docker Compose v2
- Internet access for the live USGS producer and initial image builds

No local Spark, Kafka, or PostgreSQL installation is required.

## Fast demo using replay data

From the project root:

```bash
cp .env.example .env
./scripts/reset_demo.sh --full
```

On Windows PowerShell, use:

```powershell
Copy-Item .env.example .env
Remove-Item data\checkpoints\seismic_stream -Recurse -Force -ErrorAction SilentlyContinue
docker compose down -v --remove-orphans
```

Start Kafka, Spark, PostgreSQL, the streaming job, dashboard, and replay producer:

```bash
docker compose --profile app --profile replay up --build
```

Open:

- Kafka broker: internal Docker address `kafka:9092`, host port `localhost:9092`
- Spark master UI: <http://localhost:8080>
- Spark worker 1 UI: <http://localhost:8081>
- Spark worker 2 UI: <http://localhost:8082>
- Dashboard: <http://localhost:8050>

The replay producer sends 84 demo events into Kafka. The data includes a synthetic Northern California `M5.6` mainshock and aftershocks, so the dashboard should show anomalies and an Omori forecast.

## Live USGS mode

Use the real USGS feed instead of replay:

```bash
./scripts/reset_demo.sh --full
docker compose --profile app --profile live up --build
```

The live feed can be quiet depending on current earthquake activity. Replay mode is better for a guaranteed class demo.

## Kafka commands for checking the pipeline

List topics:

```bash
docker compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --list
```

Describe the seismic topic:

```bash
docker compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic seismic-events
```

Read a few raw messages from the topic:

```bash
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic seismic-events \
  --from-beginning \
  --max-messages 5
```

## Recompute historical baselines

The repository already includes ready-to-use sample baselines. To recompute them from `data/historical/sample_historical_events.csv`:

```bash
./scripts/run_batch_baselines.sh
```

Equivalent manual command:

```bash
docker compose up -d postgres spark-master spark-worker-1 spark-worker-2
docker compose run --rm streaming-job bash -lc "spark-submit --master spark://spark-master:7077 src/batch/compute_baselines.py"
```

## Useful database checks

```bash
docker compose exec postgres psql -U seismic -d seismic -c "SELECT COUNT(*) FROM seismic_events;"
docker compose exec postgres psql -U seismic -d seismic -c "SELECT cell_id, event_count, z_score FROM anomaly_windows ORDER BY created_at DESC LIMIT 10;"
docker compose exec postgres psql -U seismic -d seismic -c "SELECT region_id, b_value, b_sigma, is_alert FROM bvalue_windows ORDER BY created_at DESC LIMIT 10;"
docker compose exec postgres psql -U seismic -d seismic -c "SELECT mainshock_id, p_value, next_six_hour_expected FROM aftershock_forecasts ORDER BY fitted_at DESC LIMIT 10;"
```

## How the pipeline maps to the workshop abstract

| Abstract requirement | Implementation |
|---|---|
| USGS Earthquake Hazards Real-Time GeoJSON feed | `live_usgs_producer.py` polls `all_hour.geojson` |
| Replay source for deterministic demo | `replay_producer.py` publishes sample events to Kafka |
| Message broker / streaming middleware | Kafka service in `docker-compose.yml`, topic `seismic-events` |
| Spark Structured Streaming source | `spark.readStream.format("kafka")` in `streaming_job.py` |
| Deduplication by event ID | Producer state file for live mode + Spark `dropDuplicates(event_id)` |
| Historical catalog baselines | `compute_baselines.py` and `data/baseline/cell_baselines.csv` |
| 1° × 1° grid cells | `grid_lon`, `grid_lat`, `cell_id` fields |
| 6-hour sliding windows | `window(event_ts, "6 hours", "1 hour")` |
| Z-score anomaly detection | `compute_anomalies()` |
| K-Means grouping of flagged cells | Spark MLlib `KMeans` in `compute_anomalies()` |
| Gutenberg–Richter b-value monitoring | `compute_bvalues()` + `seismo_models.py` |
| Omori–Utsu aftershock model | `compute_aftershock_forecasts()` + `fit_omori_utsu()` |
| PostgreSQL sink after micro-batch | Spark `foreachBatch` writes with `psycopg2` |
| Plotly Dash dashboard | `src/dashboard/app.py` |
| Multi-node Spark cluster | Docker Compose: 1 master + 2 workers |

## Notes for submission/demo

Recommended screenshots:

1. Docker Compose terminal showing Kafka, Spark master, two workers, PostgreSQL, streaming job, dashboard, and replay/live producer running.
2. Kafka topic check showing `seismic-events` exists.
3. Kafka console consumer showing raw JSON earthquake events being published.
4. Spark master UI showing the submitted streaming application and two active workers.
5. PostgreSQL query output showing non-empty `seismic_events`, `anomaly_windows`, `bvalue_windows`, and `aftershock_forecasts`.
6. Dash world map showing recent events and highlighted anomalous grid cells.
7. Dash b-value chart.
8. Dash Omori–Utsu forecast chart.

## Troubleshooting

### Spark fails with a Kafka connector error

The streaming job uses this package at submit time:

```text
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5
```

Make sure the machine has internet access the first time Docker builds/runs the Spark image so Spark can resolve the Kafka connector dependency.

### Dashboard starts before events appear

The dashboard refreshes every 10 seconds. Wait until the replay producer has published a few Kafka messages and the streaming job logs a non-empty batch.

### No anomalies in live mode

This is normal if the live USGS feed is quiet. Use replay mode for a deterministic demo.

### Resetting the demo

Spark checkpointing remembers processed Kafka offsets. Kafka also stores topic messages in the `kafka-data` Docker volume, and PostgreSQL stores rows in the `pgdata` Docker volume.

For a clean replay demo, run:

```bash
./scripts/reset_demo.sh --full
```

On Windows PowerShell:

```powershell
docker compose down -v --remove-orphans
Remove-Item data\checkpoints\seismic_stream -Recurse -Force -ErrorAction SilentlyContinue
```

### Port conflicts

If ports `5432`, `7077`, `8080`, `8081`, `8082`, `8050`, or `9092` are already used, change the left-hand side of the relevant `ports:` mapping in `docker-compose.yml`.
