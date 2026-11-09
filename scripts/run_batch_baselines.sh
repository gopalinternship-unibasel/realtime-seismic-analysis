#!/usr/bin/env bash
set -euo pipefail
docker compose up -d postgres spark-master spark-worker-1 spark-worker-2
docker compose run --rm streaming-job bash -lc 'spark-submit --master ${SPARK_MASTER_URL:-spark://spark-master:7077} src/batch/compute_baselines.py'
