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

The historical baselines in `data/baseline/` are already computed from real USGS data (90 days to 2026-11-09). Running the same job again on 9 Nov 2026 (into a scratch folder) reproduced them closely: 32,552 instead of 32,544 earthquakes (USGS adds late events), 1,888 instead of 1,887 cells, regional b-values within 0.02. To recompute them from the latest 90 days (optional, about 4 minutes; it rewrites `data/historical/usgs_historical_events.csv` and `data/baseline/`):

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\refresh_baselines.ps1
```

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

Timing on a fresh start (measured on this PC on 9 Nov 2026): the image build takes 4 to 6 minutes when the images already exist and only the source changed, 10 to 20 minutes the very first time; then the dashboard is up within a minute and the streaming job needs about 3 minutes for its first micro-batch (connector download, Spark warm-up, first K-Means fit). A whole `start.ps1 -Reset` took 8 to 10 minutes. Later batches take 10 to 40 seconds depending on free CPU.

Running `start.ps1` again on a running or stopped stack is safe: the streaming job resumes from its checkpoint, the live producer sends only events it has not sent before, and the replay producer re-sends the same 184 event ids, which the streaming job drops as duplicates (the script then reports `Batch N: empty`).

What happens:

- The **replay producer** sends the 184-event synthetic demo catalogue (an M5.6 Northern California sequence, events in Japan and Chile, background events), shifted so the last event is "now", then exits. 84 events are the original hand-made demo; 100 are aftershocks of the M5.6 added by `src/producer/extend_replay_catalog.py` (seeded, Omori–Utsu and Gutenberg–Richter distributed) so that the replay has enough events for 24-hour b-values.
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
- **Mainshock** chooses the M ≥ 5 event whose Omori–Utsu fit and six-hour forecast are shown.
- Hover over map points for place, magnitude, time, z-score and region.

Replay demo, expected (seen on 9 Nov 2026): 184 events, 7 anomalous cells, 3 K-Means regions (Northern California, Japan, Chile), 33 b-value windows for R-130_+30 (b from 0.67 to 1.03 against a long-term 0.88, no alerts), and a forecast for "Demo M5.6 - Northern California" with 138 aftershocks, p = 1.14, R² = 0.87 and about 8.5 aftershocks expected in the next 6 hours.

Live feed, seen on 9 Nov 2026 (it changes with the real world): about 1,760 events of the last week, 36 anomalous cells, 1 to 2 K-Means regions (Alaska), 274 b-value windows in 3 regions (R-130_+30 northern and central California, R-120_+30 southern California and Nevada, R-160_+60 Alaska) with 17 alerts, 19 active M ≥ 5 mainshocks and 4 forecasts.

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

stops every container of the project and keeps the data; the next `start.ps1` resumes from the Spark checkpoint and the stored Kafka offsets. To delete this project's data as well:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stop.ps1 -Reset
```

`-Reset` removes only the `cvfix-seismic_*` volumes. The replay producer always sends the same event IDs, so to see the replay arrive again you need a reset.

## 7. Tests

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\test.ps1
```

runs, inside the project's images, `tests/pipeline` (pytest with a local SparkSession: USGS parsing, producers and the demo-catalogue generator, the streaming ingest path with de-duplication across micro-batches, b-value and Omori–Utsu models, the regional Mc floor, sliding windows across micro-batches, K-Means regions, baselines) and `tests/dashboard` (layout, controls, selections across data sources, figures). It does not need the stack to be running. About 4 minutes on this PC (36 pipeline and 7 dashboard tests).

Browser test of the running dashboard (optional; needs Node 18+ and the stack started with both producers):

```powershell
cd tests\e2e
$env:PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD = "1"
npm install
node dashboard.e2e.mjs
cd ..\..
```

It drives the installed Microsoft Edge headless, works every control and prints PASS/FAIL per check; screenshots go to `tests\e2e\screenshots`.

## 8. Sign-in and AI provider

- **Sign-in**: none. The project has no user accounts and no test-user file; the dashboard is open on localhost. PostgreSQL uses the local development values in `.env`.
- **AI provider**: none to switch. The project calls no LLM or hosted AI service; the machine-learning part (K-Means) runs in Spark MLlib inside the containers, and the seismological models run in Python. The only outside services are the public USGS GeoJSON feed and the USGS FDSN event service (for baselines), no key needed.

## 9. Configuration

`.env` (copied from `.env.example`) holds every setting with its default: PostgreSQL name/user/password, Kafka topic, live poll interval and backfill feed (`USGS_BACKFILL_URL`, empty disables it), `REPLAY_REBASE_TO_NOW`, and the analytics thresholds (`Z_THRESHOLD`, `MIN_EVENTS_FOR_ANOMALY`, `BVALUE_MIN_EVENTS`, `MAX_KMEANS_CLUSTERS`, `ACTIVE_REGION_HOURS`, `REGION_RADIUS_KM`, `HISTORY_DAYS`). After changing it run `start.ps1` again.

## 10. Known limits

- Live Omori–Utsu forecasts depend on the world: outside the US the USGS feed lists mostly M4+ events, so most M5+ mainshocks have 0 to 3 recorded aftershocks. Sequences with 3 or 4 aftershocks get a forecast with the generic p = 1.08; fewer than 3 get none. On 9 Nov 2026 the live week had 19 mainshocks and 4 forecasts: 3 with the generic p and one (Kyrgyzstan, 5 aftershocks spread over 5 days) whose fitted p sits at the lower grid limit 0.3, which says more about 5 events than about the sequence.
- The replay catalogue is synthetic, including the 100 generated aftershocks; it demonstrates the pipeline, it is not a real earthquake sequence. To regenerate the aftershocks (same seed, same file): `docker compose --profile test run --rm --no-deps tests python src/producer/extend_replay_catalog.py`.
- b-value windows use Mc = max(maximum curvature of the window, long-term Mc of the region), so regions with a high completeness magnitude (much of the world outside the US, where the feed lists mostly M4+) rarely reach the 20 events above Mc that a 24-hour window needs. On 9 Nov 2026 only three live regions (California, Nevada and Alaska) had b-value windows.
- Live anomalies and regions reflect real activity; on a quiet day there may be none in the last 24 hours.
- The baselines cover 90 days; refresh them now and then with `refresh_baselines.ps1` and restart the streaming job.
- Speed depends on free CPU. On this PC, with another container (a shared local LLM) using most of the 8 CPUs, the first micro-batch after a fresh start (1,943 events: the week backfill plus the replay) took 170 s, later 1-event micro-batches 15 to 40 s. Batches never drop data when they take longer than the 10-second trigger; they just run later.
- b-value windows that end after the latest event are still open (they include fewer than 24 hours of data so far).
- The browser test's `npm install` in `tests\e2e` was not run on this PC; the same script was run (25 of 25 checks passed) from a folder that already had Playwright 1.56.1.
- The dashboard runs on Flask's development server (fine on localhost, not for a public deployment).
- Spark runs only inside the containers; running the job with a Windows-host Python/Spark is not supported.

## 11. What only Gopal can do

- Decide whether to change the CV wording now that the claims are backed by the running code (see the commit messages for what changed).
- Push the commits (they are local only) and decide whether to copy the fixes back to the original Desktop folder.
- Remove the old stopped containers, images and volumes of earlier copies (`seismic-spark-dis-workshop*`) if he no longer needs them; they were left untouched.
- Remove the per-service images `cvfix-seismic-spark-master`, `-spark-worker-1`, `-spark-worker-2`, `-streaming-job` and `-replay-producer` if he wants the space back (`docker image rm ...`): an earlier version of this copy's compose file built them; the current one uses only `cvfix-seismic-spark` and `cvfix-seismic-dashboard`.
