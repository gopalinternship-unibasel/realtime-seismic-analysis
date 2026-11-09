#!/usr/bin/env bash
# Download USGS history (FDSN event service) and recompute data/baseline/*.csv on the Spark cluster.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose --profile batch run --rm baselines
cat data/baseline/baseline_metadata.json
echo "Restart the streaming job to load the new baselines: docker compose --profile app restart streaming-job"
