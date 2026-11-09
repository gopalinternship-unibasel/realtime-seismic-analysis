#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == "--full" ]]; then
  docker compose down -v --remove-orphans || true
  printf 'Stopped containers and removed PostgreSQL/Kafka volumes.\n'
fi

rm -rf data/checkpoints/seismic_stream
mkdir -p data/checkpoints
printf 'Spark checkpoint directory was reset. Kafka messages are stored in the kafka-data Docker volume; use ./scripts/reset_demo.sh --full to clear Kafka and PostgreSQL too.\n'
