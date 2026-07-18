#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="${PROJECT_ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}"

spark-submit \
  "${PROJECT_ROOT}/src/network_metrics/main.py" \
  --config "${PROJECT_ROOT}/config/pipeline.example.json" \
  --input-path "${PROJECT_ROOT}/sample_data" \
  --output-root "${PROJECT_ROOT}/output" \
  --processing-date "2025-07-23" \
  --stage all
