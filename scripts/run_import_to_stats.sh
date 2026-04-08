#!/usr/bin/env bash

set -euo pipefail

# -----------------------------
# Configuration
# -----------------------------
INPUT_FILE="${INPUT_FILE:-/path/to/input.jsonl}"
DB_PATH="${DB_PATH:-data/pipeline.duckdb}"

LLM_ENDPOINT="${LLM_ENDPOINT:-http://127.0.0.1:8000/v1}"
LLM_API_KEY="${LLM_API_KEY:-dummy}"
LLM_MODEL_ID="${LLM_MODEL_ID:-qwen35}"

MAX_CONCURRENCY="${MAX_CONCURRENCY:-32}"
BATCH_SIZE="${BATCH_SIZE:-50}"
LLM_TIMEOUT="${LLM_TIMEOUT:-60}"
RUN_PRESSURE_TEST="${RUN_PRESSURE_TEST:-true}"

# Optional override for Python executable.
# Default uses the project's virtualenv.
PYTHON_BIN="${PYTHON_BIN:-}"


SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

if [[ -z "${PYTHON_BIN}" ]]; then
  PYTHON_BIN="${PROJECT_ROOT}/.venv/bin/python"
fi

if [[ ! -x "${PYTHON_BIN}" ]]; then
  echo "Python executable not found: ${PYTHON_BIN}" >&2
  echo "Please create .venv first or set PYTHON_BIN in the config section." >&2
  exit 1
fi

if [[ ! -f "${INPUT_FILE}" ]]; then
  echo "Input file not found: ${INPUT_FILE}" >&2
  echo "Please update INPUT_FILE in the config section." >&2
  exit 1
fi

mkdir -p "$(dirname -- "${PROJECT_ROOT}/${DB_PATH}")"

export LLM_ENDPOINT
export LLM_API_KEY
export LLM_MODEL_ID
export MAX_CONCURRENCY
export BATCH_SIZE
export LLM_TIMEOUT
export DB_PATH="${PROJECT_ROOT}/${DB_PATH}"

run_cli() {
  "${PYTHON_BIN}" -m claw_data_filter.cli --db-path "${DB_PATH}" --llm-endpoint "${LLM_ENDPOINT}" --llm-model-id "${LLM_MODEL_ID}" "$@"
}

echo "[1/4] Importing data: ${INPUT_FILE}"
run_cli import "${INPUT_FILE}"

if [[ "${RUN_PRESSURE_TEST}" == "true" ]]; then
  echo "[2/4] Running pressure test"
  run_cli pressure-test
else
  echo "[2/4] Skipping pressure test"
fi

echo "[3/4] Running round feedback with workers=${MAX_CONCURRENCY}, batch_size=${BATCH_SIZE}"
run_cli round-feedback --workers "${MAX_CONCURRENCY}" --batch-size "${BATCH_SIZE}"

echo "[4/4] Printing stats"
run_cli stats

echo "Done. Database written to: ${DB_PATH}"