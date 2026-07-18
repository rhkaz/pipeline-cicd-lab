#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="${PROJECT_ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}"

MAIN="${PROJECT_ROOT}/src/network_metrics/main.py"
PREVIEW="${PROJECT_ROOT}/scripts/preview_output.py"
CONFIG="${PROJECT_ROOT}/config/pipeline.example.json"
INPUT_PATH="${PROJECT_ROOT}/sample_data"
OUTPUT_ROOT="${PROJECT_ROOT}/output"
PROCESSING_DATE="2025-07-23"
RUN_ID="local-20250723-001"

COMMON_ARGS=(
  --config "${CONFIG}"
  --input-path "${INPUT_PATH}"
  --output-root "${OUTPUT_ROOT}"
  --processing-date "${PROCESSING_DATE}"
  --run-id "${RUN_ID}"
)

echo "Run ID used by every stage: ${RUN_ID}"

echo "[1/5] BRONZE"
spark-submit "${MAIN}" "${COMMON_ARGS[@]}" --stage bronze
spark-submit "${PREVIEW}" --output-root "${OUTPUT_ROOT}" --dataset bronze --rows 10

echo "[2/5] SILVER"
spark-submit "${MAIN}" "${COMMON_ARGS[@]}" --stage silver
spark-submit "${PREVIEW}" --output-root "${OUTPUT_ROOT}" --dataset silver --rows 10
spark-submit "${PREVIEW}" --output-root "${OUTPUT_ROOT}" --dataset quality-logs --rows 20

echo "[3/5] GOLD HOURLY"
spark-submit "${MAIN}" "${COMMON_ARGS[@]}" --stage gold-hourly
spark-submit "${PREVIEW}" --output-root "${OUTPUT_ROOT}" --dataset gold-hourly --rows 20

echo "[4/5] GOLD DAILY"
spark-submit "${MAIN}" "${COMMON_ARGS[@]}" --stage gold-daily
spark-submit "${PREVIEW}" --output-root "${OUTPUT_ROOT}" --dataset gold-daily --rows 20

echo "[5/5] MONITORING"
spark-submit "${MAIN}" "${COMMON_ARGS[@]}" --stage monitoring
spark-submit "${PREVIEW}" --output-root "${OUTPUT_ROOT}" --dataset quality-logs --rows 50
spark-submit "${PREVIEW}" --output-root "${OUTPUT_ROOT}" --dataset run-logs --rows 50

echo "All stages completed successfully."
