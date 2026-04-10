#!/usr/bin/env bash

set -euo pipefail

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

SESSION_MERGE_WORKERS="${SESSION_MERGE_WORKERS:-4}"
SESSION_MERGE_BATCH_SIZE="${SESSION_MERGE_BATCH_SIZE:-100}"
SESSION_MERGE_MIN_PREFIX_TURNS="${SESSION_MERGE_MIN_PREFIX_TURNS:-2}"

PYTHON_BIN="${PYTHON_BIN:-${PROJECT_ROOT}/.venv/bin/python}"

if [[ ! -x "${PYTHON_BIN}" ]]; then
  echo "Python executable not found: ${PYTHON_BIN}" >&2
  exit 1
fi

if [[ ! -f "${INPUT_FILE}" ]]; then
  echo "Input file not found: ${INPUT_FILE}" >&2
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

echo "[0/7] pressure-test preflight ${LLM_ENDPOINT}"
LLM_API_KEY="${LLM_API_KEY}" MAX_CONCURRENCY="${MAX_CONCURRENCY}" LLM_TIMEOUT="${LLM_TIMEOUT}" \
  "${PYTHON_BIN}" -m claw_data_filter.cli \
  --db-path "${TMP_DB_PATH}" \
  --llm-endpoint "${LLM_ENDPOINT}" \
  --llm-model-id "${LLM_MODEL_ID}" \
  pressure-test

rm -f "${TMP_DB_PATH}"

echo "[1/7] import ${INPUT_FILE}"
run_cli import "${INPUT_FILE}"

echo "[2/7] pressure-test ${LLM_ENDPOINT}"
LLM_API_KEY="${LLM_API_KEY}" LLM_TIMEOUT="${LLM_TIMEOUT}" run_cli pressure-test

echo "[3/7] session-merge"
run_cli session-merge \
  --workers "${SESSION_MERGE_WORKERS}" \
  --batch-size "${SESSION_MERGE_BATCH_SIZE}" \
  --min-prefix-turns "${SESSION_MERGE_MIN_PREFIX_TURNS}"

echo "[4/7] round-feedback"
LLM_API_KEY="${LLM_API_KEY}" MAX_CONCURRENCY="${MAX_CONCURRENCY}" BATCH_SIZE="${BATCH_SIZE}" LLM_TIMEOUT="${LLM_TIMEOUT}" \
  run_cli round-feedback --workers "${MAX_CONCURRENCY}" --batch-size "${BATCH_SIZE}"

echo "[5/7] stats"
run_cli stats

echo "[6/7] export openai_round_feedback"
run_cli filter \
  --session-merge-keep true \
  --has-error false \
  --export-format openai_round_feedback \
  --export "${EXPORT_DIR}/exported_round_feedback.jsonl" \
  --report "${EXPORT_DIR}/export_report_round_feedback.json"

echo "[7/7] export raw_jsonl"
run_cli filter \
  --session-merge-keep true \
  --has-error false \
  --export "${EXPORT_DIR}/exported_raw.jsonl"

mv -f "${TMP_DB_PATH}" "${DB_PATH}"

echo "Validation finished."
echo "DB: ${DB_PATH}"
echo "Exports: ${EXPORT_DIR}"