#!/usr/bin/env bash
# Download USGS history (FDSN event service) and recompute data/baseline/*.csv on the Spark cluster.
# The pipeline may keep running (the streaming job leaves 2 of the 6 cluster cores free).
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose --profile batch run --rm baselines
cat data/baseline/baseline_metadata.json
if [ -n "$(docker compose --profile app ps --status running -q streaming-job 2>/dev/null)" ]; then
  echo "Restarting the streaming job so it loads the new baselines ..."
  docker compose --profile app restart streaming-job
else
  echo "The streaming job is not running; it loads the new baselines on its next start."
fi
