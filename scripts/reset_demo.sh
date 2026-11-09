#!/usr/bin/env bash
# Reset the demo. Kafka messages, PostgreSQL rows, Spark checkpoints and the live producer's
# seen-ID state all live in Docker volumes of the cvfix-seismic project, so they are reset together.
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose --profile app --profile replay --profile live down -v --remove-orphans
# Checkpoints of older versions of this project were kept on the host; they must never be reused.
rm -rf data/checkpoints/seismic_stream
printf 'Removed the cvfix-seismic containers and volumes (Kafka topic, PostgreSQL, Spark checkpoints, producer state).\n'
