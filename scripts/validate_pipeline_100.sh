#!/usr/bin/env bash

set -euo pipefail

timestamp() {
  date '+%Y-%m-%d %H:%M:%S'
}

log() {
  echo "[$(timestamp)] $*"
}

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

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

INPUT_FILE="${INPUT_FILE:-${PROJECT_ROOT}/data/pipeline_e2e/items_100.jsonl}"
DB_PATH="${DB_PATH:-${PROJECT_ROOT}/data/pipeline_e2e/e2e_100_progress.duckdb}"
EXPORT_DIR="${EXPORT_DIR:-${PROJECT_ROOT}/data/pipeline_e2e/validation_progress}"
TMP_DB_PATH="${DB_PATH}.tmp.$$"

LLM_ENDPOINT="${LLM_ENDPOINT:-http://127.0.0.1:8000/v1}"
LLM_API_KEY="${LLM_API_KEY:-dummy}"
LLM_MODEL_ID="${LLM_MODEL_ID:-qwen35}"
MAX_CONCURRENCY="${MAX_CONCURRENCY:-16}"
BATCH_SIZE="${BATCH_SIZE:-20}"
LLM_TIMEOUT="${LLM_TIMEOUT:-60}"

CPU_COUNT="$(detect_cpu_count)"
IMPORT_WORKERS="${IMPORT_WORKERS:-$(shared_cpu_budget "${CPU_COUNT}" 8)}"
IMPORT_CHUNK_SIZE="${IMPORT_CHUNK_SIZE:-64}"
SESSION_MERGE_WORKERS="${SESSION_MERGE_WORKERS:-$(shared_cpu_budget "${CPU_COUNT}" 16)}"
SESSION_MERGE_BATCH_SIZE="${SESSION_MERGE_BATCH_SIZE:-100}"
SESSION_MERGE_MIN_PREFIX_TURNS="${SESSION_MERGE_MIN_PREFIX_TURNS:-2}"

PYTHON_BIN="${PYTHON_BIN:-${PROJECT_ROOT}/.venv/bin/python}"

if [[ ! -x "${PYTHON_BIN}" ]]; then
  log "Python executable not found: ${PYTHON_BIN}" >&2
  exit 1
fi

if [[ ! -f "${INPUT_FILE}" ]]; then
  log "Input file not found: ${INPUT_FILE}" >&2
  exit 1
fi

mkdir -p "${EXPORT_DIR}"

cleanup() {
  rm -f "${TMP_DB_PATH}"
}

trap cleanup EXIT

run_cli() {
  "${PYTHON_BIN}" -m claw_data_filter.cli \
    --db-path "${TMP_DB_PATH}" \
    --llm-endpoint "${LLM_ENDPOINT}" \
    --llm-model-id "${LLM_MODEL_ID}" \
    "$@"
}

log "Validation configuration: input=${INPUT_FILE} db=${DB_PATH} temp_db=${TMP_DB_PATH} cpu_count=${CPU_COUNT} import_workers=${IMPORT_WORKERS} import_chunk_size=${IMPORT_CHUNK_SIZE} session_merge_workers=${SESSION_MERGE_WORKERS} llm_concurrency=${MAX_CONCURRENCY} batch_size=${BATCH_SIZE} endpoint=${LLM_ENDPOINT} model=${LLM_MODEL_ID}"

log "[0/7] pressure-test preflight ${LLM_ENDPOINT}"
LLM_API_KEY="${LLM_API_KEY}" MAX_CONCURRENCY="${MAX_CONCURRENCY}" LLM_TIMEOUT="${LLM_TIMEOUT}" \
  "${PYTHON_BIN}" -m claw_data_filter.cli \
  --db-path "${TMP_DB_PATH}" \
  --llm-endpoint "${LLM_ENDPOINT}" \
  --llm-model-id "${LLM_MODEL_ID}" \
  pressure-test

rm -f "${TMP_DB_PATH}"

log "[1/7] import ${INPUT_FILE}"
run_cli import --workers "${IMPORT_WORKERS}" --chunk-size "${IMPORT_CHUNK_SIZE}" "${INPUT_FILE}"

log "[2/7] pressure-test ${LLM_ENDPOINT}"
LLM_API_KEY="${LLM_API_KEY}" LLM_TIMEOUT="${LLM_TIMEOUT}" run_cli pressure-test

log "[3/7] session-merge"
run_cli session-merge \
  --workers "${SESSION_MERGE_WORKERS}" \
  --batch-size "${SESSION_MERGE_BATCH_SIZE}" \
  --min-prefix-turns "${SESSION_MERGE_MIN_PREFIX_TURNS}"

log "[4/7] round-feedback"
LLM_API_KEY="${LLM_API_KEY}" MAX_CONCURRENCY="${MAX_CONCURRENCY}" BATCH_SIZE="${BATCH_SIZE}" LLM_TIMEOUT="${LLM_TIMEOUT}" \
  run_cli round-feedback --workers "${MAX_CONCURRENCY}" --batch-size "${BATCH_SIZE}"

log "[5/7] stats"
run_cli stats

log "[6/7] export openai_round_feedback"
run_cli filter \
  --session-merge-keep true \
  --has-error false \
  --export-format openai_round_feedback \
  --export "${EXPORT_DIR}/exported_round_feedback.jsonl" \
  --report "${EXPORT_DIR}/export_report_round_feedback.json"

log "[7/7] export raw_jsonl"
run_cli filter \
  --session-merge-keep true \
  --has-error false \
  --export "${EXPORT_DIR}/exported_raw.jsonl"

mv -f "${TMP_DB_PATH}" "${DB_PATH}"

log "Validation finished."
log "DB: ${DB_PATH}"
log "Exports: ${EXPORT_DIR}"