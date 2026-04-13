#!/usr/bin/env bash

set -euo pipefail

timestamp() {
  date '+%Y-%m-%d %H:%M:%S'
}

log() {
  echo "[$(timestamp)] $*"
}

# -----------------------------
# Configuration
# -----------------------------
INPUT_FILE="${INPUT_FILE:-/path/to/input.jsonl}"
DB_PATH="${DB_PATH:-data/pipeline.duckdb}"
EXPORT_DIR="${EXPORT_DIR:-}"

LLM_ENDPOINT="${LLM_ENDPOINT:-http://127.0.0.1:8000/v1}"
LLM_API_KEY="${LLM_API_KEY:-dummy}"
LLM_MODEL_ID="${LLM_MODEL_ID:-qwen35}"

MAX_CONCURRENCY="${MAX_CONCURRENCY:-32}"
BATCH_SIZE="${BATCH_SIZE:-50}"
LLM_TIMEOUT="${LLM_TIMEOUT:-60}"
RUN_PRESSURE_TEST="${RUN_PRESSURE_TEST:-true}"
RUN_SESSION_MERGE="${RUN_SESSION_MERGE:-true}"
detect_cpu_count() {
  getconf _NPROCESSORS_ONLN 2>/dev/null || nproc 2>/dev/null || echo 8
}

shared_cpu_budget() {
  local cpu_count="$1"
  local cap="${2:-0}"
  local budget=$(( cpu_count * 70 / 100 ))
  if (( budget < 1 )); then
    budget=1
  fi
  if (( cap > 0 && budget > cap )); then
    budget="${cap}"
  fi
  echo "${budget}"
}

CPU_COUNT="$(detect_cpu_count)"
DEFAULT_IMPORT_WORKERS="$(shared_cpu_budget "${CPU_COUNT}" 8)"
IMPORT_WORKERS="${IMPORT_WORKERS:-${DEFAULT_IMPORT_WORKERS}}"
IMPORT_CHUNK_SIZE="${IMPORT_CHUNK_SIZE:-64}"

DEFAULT_SESSION_MERGE_WORKERS="$(shared_cpu_budget "${CPU_COUNT}" 16)"
SESSION_MERGE_WORKERS="${SESSION_MERGE_WORKERS:-${DEFAULT_SESSION_MERGE_WORKERS}}"
SESSION_MERGE_BATCH_SIZE="${SESSION_MERGE_BATCH_SIZE:-512}"
SESSION_MERGE_MIN_PREFIX_TURNS="${SESSION_MERGE_MIN_PREFIX_TURNS:-2}"

# Optional override for Python executable.
# Default uses the project's virtualenv.
PYTHON_BIN="${PYTHON_BIN:-}"


SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

if [[ -z "${PYTHON_BIN}" ]]; then
  PYTHON_BIN="${PROJECT_ROOT}/.venv/bin/python"
fi

if [[ ! -x "${PYTHON_BIN}" ]]; then
  log "Python executable not found: ${PYTHON_BIN}" >&2
  log "Please create .venv first or set PYTHON_BIN in the config section." >&2
  exit 1
fi

if [[ ! -f "${INPUT_FILE}" ]]; then
  log "Input file not found: ${INPUT_FILE}" >&2
  log "Please update INPUT_FILE in the config section." >&2
  exit 1
fi

if [[ "${INPUT_FILE}" == *.gz ]]; then
  log "Compressed input is not supported directly: ${INPUT_FILE}" >&2
  log "Please decompress items.jsonl.gz to a plain JSONL file first, then set INPUT_FILE to that .jsonl path." >&2
  exit 1
fi

mkdir -p "$(dirname -- "${PROJECT_ROOT}/${DB_PATH}")"

if [[ -n "${EXPORT_DIR}" ]]; then
  mkdir -p "${PROJECT_ROOT}/${EXPORT_DIR}"
fi

export LLM_ENDPOINT
export LLM_API_KEY
export LLM_MODEL_ID
export MAX_CONCURRENCY
export BATCH_SIZE
export LLM_TIMEOUT
FINAL_DB_PATH="${PROJECT_ROOT}/${DB_PATH}"
TMP_DB_PATH="${FINAL_DB_PATH}.tmp.$$"
export DB_PATH="${TMP_DB_PATH}"

cleanup() {
  rm -f "${TMP_DB_PATH}"
}

trap cleanup EXIT

run_cli() {
  "${PYTHON_BIN}" -m claw_data_filter.cli --db-path "${DB_PATH}" --llm-endpoint "${LLM_ENDPOINT}" --llm-model-id "${LLM_MODEL_ID}" "$@"
}

log "Pipeline configuration: input=${INPUT_FILE} final_db=${FINAL_DB_PATH} temp_db=${TMP_DB_PATH} cpu_count=${CPU_COUNT} import_workers=${IMPORT_WORKERS} import_chunk_size=${IMPORT_CHUNK_SIZE} session_merge_workers=${SESSION_MERGE_WORKERS} session_merge_batch_size=${SESSION_MERGE_BATCH_SIZE} llm_concurrency=${MAX_CONCURRENCY} batch_size=${BATCH_SIZE}"

if [[ "${RUN_PRESSURE_TEST}" == "true" ]]; then
  log "[0/5] Preflight pressure test"
  "${PYTHON_BIN}" -m claw_data_filter.cli --db-path "${DB_PATH}" --llm-endpoint "${LLM_ENDPOINT}" --llm-model-id "${LLM_MODEL_ID}" pressure-test
  rm -f "${TMP_DB_PATH}"
fi

log "[1/5] Importing JSONL data: ${INPUT_FILE}"
run_cli import --workers "${IMPORT_WORKERS}" --chunk-size "${IMPORT_CHUNK_SIZE}" "${INPUT_FILE}"
log "[1/5] Import complete; empty_response markers persisted for user-only samples"

if [[ "${RUN_PRESSURE_TEST}" == "true" ]]; then
  log "[2/5] Running pressure test"
  run_cli pressure-test
else
  log "[2/5] Skipping pressure test"
fi

if [[ "${RUN_SESSION_MERGE}" == "true" ]]; then
  log "[3/5] Running session merge with workers=${SESSION_MERGE_WORKERS}, batch_size=${SESSION_MERGE_BATCH_SIZE}, min_prefix_turns=${SESSION_MERGE_MIN_PREFIX_TURNS}"
  run_cli session-merge --workers "${SESSION_MERGE_WORKERS}" --batch-size "${SESSION_MERGE_BATCH_SIZE}" --min-prefix-turns "${SESSION_MERGE_MIN_PREFIX_TURNS}"
else
  log "[3/5] Skipping session merge"
fi

log "[4/5] Running round feedback with workers=${MAX_CONCURRENCY}, batch_size=${BATCH_SIZE}"
run_cli round-feedback --workers "${MAX_CONCURRENCY}" --batch-size "${BATCH_SIZE}"

log "[5/5] Printing stats"
run_cli stats

mv -f "${TMP_DB_PATH}" "${FINAL_DB_PATH}"
log "Done. Database written to: ${FINAL_DB_PATH}"