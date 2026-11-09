# Running the seismic pipeline on Windows

Everything runs in Docker: Kafka (KRaft), a Spark standalone cluster (1 master, 2 workers), the Spark Structured Streaming job, PostgreSQL, the live USGS producer, the replay producer and the Plotly Dash dashboard. All commands below are for **PowerShell**, run from the project folder (the folder that contains `docker-compose.yml`).

## 1. Prerequisites

| Need | Details |
|---|---|
| Windows 10/11 with Docker Desktop | WSL 2 backend. Docker Desktop must be running ("Engine running") before you start. Give Docker at least 6 GB of memory and 4 CPUs. |
| Free disk | About 4 GB for the images and volumes. |
| Internet | First build (Debian and PyPI packages), first start of the streaming job (Spark downloads the Kafka and PostgreSQL connectors from Maven Central), and the public USGS feeds. |
| Free ports | 5432 (PostgreSQL), 9092 (Kafka), 7077, 8080, 8081, 8082 (Spark), 8050 (dashboard). They are published on 127.0.0.1 only. |
| Optional | Node 18+ for the browser test in section 7. |

No local Python, Java, Spark, Kafka or PostgreSQL is needed.

## 2. One-time setup

Nothing to install. The start script creates `.env` from `.env.example` on its first run, and the first `start.ps1` builds two images (`cvfix-seismic-spark`, `cvfix-seismic-dashboard`). The first build downloads Java, PySpark and the other Python packages and can take 10 to 20 minutes; later starts reuse the images.

The historical baselines in `data/baseline/` are already computed from real USGS data (90 days to 2026-11-09). Running the same job again on 9 Nov 2026 (into a scratch folder) reproduced them closely: 32,552 instead of 32,544 earthquakes (USGS adds late events), 1,888 instead of 1,887 cells, regional b-values within 0.02. To recompute them from the latest 90 days (optional; it rewrites `data/historical/usgs_historical_events.csv` and `data/baseline/`):

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\refresh_baselines.ps1
```

The pipeline may keep running meanwhile. Each Spark worker offers 3 cores; the streaming job holds at most 4 of the 6 (`STREAMING_MAX_CORES`) and the batch job uses the other 2 (`SPARK_BATCH_CORES`). The script leaves a running Spark master and workers as they are (`up --no-recreate`, then `run --no-deps`): a plain `docker compose run baselines` recreates them whenever Compose considers them out of date, for example after `start.ps1` or `test.ps1` rebuilt the image, and that cut the running streaming job off from its executors ("Lost executor 0/1") until it was restarted. Afterwards the script restarts a running streaming job so it loads the new baselines (it resumes from its checkpoint; `-NoRestart` skips this). If the cluster has no free cores, the job stops with an error after 3 minutes and names the application holding them, instead of waiting forever. Measured on 9 Nov 2026 from a fresh clone with the whole pipeline running and the image tag rebuilt in between: 268 s in all (32,555 events downloaded, the master and workers kept running, the batch job on 2 cores beside the streaming job on 4, 259 s, then the restart, after which the job loaded the new 1,889 cell baselines and took its 4 cores again).

## 3. Start

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start.ps1
```

This starts the whole stack with **both** producers, waits until the dashboard answers and until the streaming job has processed its first micro-batch, then prints the URLs. Options:

| Command | What it does |
|---|---|
| `.\scripts\start.ps1` | live USGS feed and replay demo (default) |
| `.\scripts\start.ps1 -Mode replay` | replay demo only |
| `.\scripts\start.ps1 -Mode live` | live USGS feed only |
| `.\scripts\start.ps1 -Reset` | first delete this project's data (Kafka topic, PostgreSQL, Spark checkpoints, producer state), then start |
| `.\scripts\start.ps1 -NoBuild` | skip the image build check |

(Prefix each with `powershell -ExecutionPolicy Bypass -File`. Windows blocks `.ps1` files by default; this flag allows only this run.)

Timing on a fresh start (measured on this PC on 9 Nov 2026): the image build takes 4 to 6 minutes when the images already exist and only the source changed, 10 to 20 minutes the very first time; then the dashboard is up within a minute and the streaming job needs about 3 minutes for its first micro-batch (connector download, Spark warm-up, first K-Means fit). A whole `start.ps1 -Reset` took 8 to 10 minutes; from a fresh clone with cached base layers, `start.ps1` took 213 s with both producers and `start.ps1 -Reset -Mode replay` 437 s (both on 9 Nov 2026 while a shared LLM container used about 6 of the 8 CPUs; QA measured 931 s for a first start on a busier machine). Later batches take 10 to 40 seconds depending on free CPU (a batch with a new M ≥ 5 sequence up to 2 minutes).

Running `start.ps1` again on a running or stopped stack is safe: the streaming job resumes from its checkpoint, the live producer sends only events it has not sent before, and the replay producer re-sends the same 184 event ids, which the streaming job drops as duplicates (the script then reports `Batch N: empty`).

What happens:

- The **replay producer** sends the 184-event synthetic demo catalogue (an M5.6 Northern California sequence, events in Japan and Chile, background events), shifted by a whole number of hours so the last event falls within the last hour, then exits. Whole hours keep every event in the same place of the hourly window grid, so every start gives the same windows (section 4). 84 events are the original hand-made demo; 100 are aftershocks of the M5.6 added by `src/producer/extend_replay_catalog.py` (seeded, Omori–Utsu and Gutenberg–Richter distributed) so that the replay has enough events for 24-hour b-values.
- The **live producer** loads the last 7 days of the USGS feed once (about 1,800 events), then polls the last-hour feed every 60 seconds and sends only new events.

The Docker Compose project is named `cvfix-seismic`; all containers and volumes start with that name.

## 4. URLs and ports

| What | Address |
|---|---|
| Dashboard | <http://localhost:8050> |
| Spark master UI (2 workers, application `RealTimeSeismicActivityAnalysis`) | <http://localhost:8080> |
| Spark worker UIs | <http://localhost:8081>, <http://localhost:8082> |
| Kafka from the host | `localhost:9092`, topic `seismic-events` (inside Docker: `kafka:9092`) |
| PostgreSQL from the host | `localhost:5432`, database `seismic`, user `seismic`, password `seismic` (local development values from `.env`) |

### Using the dashboard

- **Data source**: Both, Live USGS feed, or Replay demo. Analytics are computed per source, so the replay never mixes into live results.
- **Time range** (6 hours to all time) and **minimum magnitude** filter the map and the counts.
- **Auto-refresh** every 10 seconds; untick it to freeze the view.
- **Regions** chooses the 10° x 10° regions in the b-value chart (default: the 4 with most windows). Red rings are b-value alerts.
- **Mainshock** chooses the mainshock whose Omori–Utsu fit and six-hour forecast are shown; the text above the chart gives the mainshock, the fit (K, c, p, R² or the generic-p note) and the expected aftershocks. Every active mainshock is refitted after each micro-batch with new events, and the streaming job records the data clock of each forecast run (table `forecast_runs`); a listed forecast that does not start at its source's latest run clock would be marked "(not refitted since ...)". (While a micro-batch is still running, the forecasts shown are those of the previous run; they are not marked.)
- M ≥ 5 events that belong to the sequence of a larger event (within 300 km and 7 days) are not listed or counted as mainshocks: the larger event's forecast counts them among its aftershocks.
- Hover over map points for place, magnitude, time, z-score and region.
- The page fits a phone screen (one column of controls below 640 px wide).

Replay demo, expected on every fresh start (the replay is shifted by whole hours, so these figures do not depend on the start time; seen identically on fresh starts on 9 Nov 2026 at 11:56, 12:23, 13:35 and 14:02 UTC, the last one with `-Reset -Mode replay`): 184 events, 7 anomalous cells (62 anomaly windows), 3 K-Means regions (Northern California, Japan, Chile), 34 b-value windows for R-130_+30 (b from 0.668 to 1.027 against a long-term 0.88, no alerts), and a forecast for "Demo M5.6 - Northern California" with 138 aftershocks, p = 1.14, R² = 0.87 and 8.51 aftershocks expected in the next 6 hours. (Databases filled before this change keep the windows of their own start: QA saw 33 or 34 b-value windows and 61 or 63 anomaly windows.)

Live feed, seen on 9 Nov 2026 at 13:35 UTC (it changes with the real world): 1,712 events of the last week, 37 anomalous cells (167 anomaly windows), 2 K-Means regions (Alaska), 267 b-value windows in 3 regions (R-130_+30 northern and central California, R-120_+30 southern California and Nevada, R-160_+60 Alaska) with 15 alerts, 21 active M ≥ 5 events of which 19 are mainshocks (the M5.5 and M5.7 off Hasaki belong to the M6.1 of 6 Nov), and 4 forecasts (the mainshocks with 3 or more recorded aftershocks: Japan M6.1, Kyrgyzstan, Philippines, Greece).

## 5. Check that every stage works

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\check.ps1
```

prints the containers, the Kafka topic offsets and two raw messages, the Spark cluster (alive workers, running application), the last micro-batch log lines, row counts per table and source in PostgreSQL, and the dashboard HTTP status. Single commands:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic seismic-events
docker compose --profile app logs -f streaming-job
docker compose exec postgres psql -U seismic -d seismic -c "SELECT source, COUNT(*) FROM seismic_events GROUP BY source;"
```

## 6. Stop

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stop.ps1
```

stops every container of the project and keeps the data; the next `start.ps1` resumes from the Spark checkpoint and the stored Kafka offsets. The streaming job gets 90 s to stop by itself (`stop_grace_period`); it exits with code 143 (stopped by SIGTERM), not 137 (killed). On 9 Nov 2026 it took 4 to 7 s, also when stopped in the middle of a micro-batch, which the next start then ran again; QA had seen 137 with the 10 s default while a shared LLM container kept the CPUs busy. To delete this project's data as well:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stop.ps1 -Reset
```

`-Reset` removes only the `cvfix-seismic_*` volumes. The replay producer always sends the same event IDs, so to see the replay arrive again you need a reset.

## 7. Tests

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\test.ps1
```

runs, inside the project's images, `tests/pipeline` (pytest with a local SparkSession: USGS parsing, producers and the demo-catalogue generator, the streaming ingest path with de-duplication across micro-batches, b-value and Omori–Utsu models, declustering and the aftershock search box, the regional Mc floor, sliding windows across micro-batches, replay windows that do not depend on the start minute, K-Means regions, baselines, the cluster-cores check) and `tests/dashboard` (layout and stylesheet, controls, selections across data sources, figures, forecast text and stale marks). The forecasting tests (`tests/pipeline/test_forecasts_db.py`) run the real SQL against `test-postgres`, a throwaway PostgreSQL the script starts (data in memory, no port published) and removes afterwards. It does not need the stack to be running. About 3 to 5 minutes on this PC (47 pipeline and 11 dashboard tests; 186 s on 9 Nov 2026).

Browser test of the running dashboard (optional; needs Node 18+ and the stack started with both producers):

```powershell
cd tests\e2e
$env:PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD = "1"
npm install
node dashboard.e2e.mjs
cd ..\..
```

It drives the installed Microsoft Edge headless, works every control, checks that no live forecast is stale and that a 390 px phone screen has no horizontal overflow, and prints PASS/FAIL per check (29 checks); screenshots go to `tests\e2e\screenshots`.

## 8. Sign-in and AI provider

- **Sign-in**: none. The project has no user accounts and no test-user file; the dashboard is open on localhost. PostgreSQL uses the local development values in `.env`.
- **AI provider**: none to switch. The project calls no LLM or hosted AI service; the machine-learning part (K-Means) runs in Spark MLlib inside the containers, and the seismological models run in Python. The only outside services are the public USGS GeoJSON feed and the USGS FDSN event service (for baselines), no key needed.

## 9. Configuration

`.env` (copied from `.env.example`) holds every setting with its default: PostgreSQL name/user/password, Kafka topic, live poll interval and backfill feed (`USGS_BACKFILL_URL`, empty disables it), `REPLAY_REBASE_TO_NOW`, the analytics thresholds (`Z_THRESHOLD`, `MIN_EVENTS_FOR_ANOMALY`, `BVALUE_MIN_EVENTS`, `MAX_KMEANS_CLUSTERS`, `ACTIVE_REGION_HOURS`, `REGION_RADIUS_KM`, `HISTORY_DAYS`) and the Spark cores (`STREAMING_MAX_CORES`, default 4 of the cluster's 6, and `SPARK_BATCH_CORES`, default 2; keep their sum at 6 or less, or the baselines job cannot run beside the streaming job). After changing it run `start.ps1` again. A `.env` created before a setting was added simply lacks it; the default in `docker-compose.yml` applies.

## 10. Known limits

- Live Omori–Utsu forecasts depend on the world: outside the US the USGS feed lists mostly M4+ events, so most M5+ mainshocks have 0 to 3 recorded aftershocks. Sequences with 3 or 4 aftershocks get a forecast with the generic p = 1.08; fewer than 3 get none. On 9 Nov 2026 the live week had 19 mainshocks and 4 forecasts: 3 with the generic p and one (Kyrgyzstan, 5 aftershocks spread over 5 days) whose fitted p sits at the lower grid limit 0.3, which says more about 5 events than about the sequence.
- Declustering uses the same fixed windows as the aftershock search (300 km, 7 days, any larger magnitude), not magnitude-dependent windows such as Gardner–Knopoff. Two equal-magnitude events (a doublet) both stay mainshocks.
- Until 9 Nov 2026 (QA round 1) forecasts were computed for the 20 newest active M ≥ 5 events only. Databases filled before the fix still hold the old rows; the first micro-batch after an upgrade refits every mainshock, and M5 events inside a larger sequence are no longer listed.
- The replay catalogue is synthetic, including the 100 generated aftershocks; it demonstrates the pipeline, it is not a real earthquake sequence. To regenerate the aftershocks (same seed, same file): `docker compose --profile test run --rm --no-deps tests python src/producer/extend_replay_catalog.py`.
- b-value windows use Mc = max(maximum curvature of the window, long-term Mc of the region), so regions with a high completeness magnitude (much of the world outside the US, where the feed lists mostly M4+) rarely reach the 20 events above Mc that a 24-hour window needs. On 9 Nov 2026 only three live regions (California, Nevada and Alaska) had b-value windows.
- Live anomalies and regions reflect real activity; on a quiet day there may be none in the last 24 hours.
- The baselines cover 90 days; refresh them now and then with `refresh_baselines.ps1` and restart the streaming job.
- Speed depends on free CPU. On this PC, with another container (a shared local LLM) using most of the 8 CPUs, the first micro-batch after a fresh start (1,943 events: the week backfill plus the replay) took 170 s, later 1-event micro-batches 15 to 40 s. Batches never drop data when they take longer than the 10-second trigger; they just run later.
- b-value windows that end after the latest event are still open (they include fewer than 24 hours of data so far).
- The browser test's documented `npm install` in `tests\e2e` was run on this PC by the QA pass of 9 Nov 2026; the fix round ran the same script from a folder that already had Playwright 1.56.1 (29 of 29 checks passed).
- The dashboard runs on Flask's development server (fine on localhost, not for a public deployment).
- Spark runs only inside the containers; running the job with a Windows-host Python/Spark is not supported.

## 11. What only Gopal can do

- Decide whether to change the CV wording now that the claims are backed by the running code (see the commit messages for what changed).
- Push the commits (they are local only) and decide whether to copy the fixes back to the original Desktop folder.
- Remove the old stopped containers, images and volumes of earlier copies (`seismic-spark-dis-workshop*`) if he no longer needs them; they were left untouched.
- Remove the per-service images `cvfix-seismic-spark-master`, `-spark-worker-1`, `-spark-worker-2`, `-streaming-job` and `-replay-producer` if he wants the space back (`docker image rm ...`): an earlier version of this copy's compose file built them; the current one uses only `cvfix-seismic-spark` and `cvfix-seismic-dashboard`.
