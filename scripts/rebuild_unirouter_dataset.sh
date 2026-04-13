#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

SOURCE_TAR_DIR="${SOURCE_TAR_DIR:-/kanas/nlp/liuchang/manydata/unirouter}"
UNCOMPRESS_DIR="${UNCOMPRESS_DIR:-/kanas/nlp/liuchang/manydata/unirouter_uncompress}"
BUILD_DIR="${BUILD_DIR:-${PROJECT_ROOT}/data/unirouter_refresh}"
MERGED_INPUT="${MERGED_INPUT:-${BUILD_DIR}/items_merged.jsonl}"
DB_PATH="${DB_PATH:-data/unirouter_refresh/unirouter_refresh.duckdb}"

LLM_ENDPOINT="${LLM_ENDPOINT:-http://182.242.159.76:31870/v1}"
LLM_API_KEY="${LLM_API_KEY:-dummy}"
LLM_MODEL_ID="${LLM_MODEL_ID:-qwen35}"
MAX_CONCURRENCY="${MAX_CONCURRENCY:-256}"
BATCH_SIZE="${BATCH_SIZE:-256}"
LLM_TIMEOUT="${LLM_TIMEOUT:-60}"
RUN_PRESSURE_TEST="${RUN_PRESSURE_TEST:-true}"
RUN_SESSION_MERGE="${RUN_SESSION_MERGE:-true}"
SESSION_MERGE_WORKERS="${SESSION_MERGE_WORKERS:-8}"
SESSION_MERGE_BATCH_SIZE="${SESSION_MERGE_BATCH_SIZE:-1024}"
SESSION_MERGE_MIN_PREFIX_TURNS="${SESSION_MERGE_MIN_PREFIX_TURNS:-2}"
PYTHON_BIN="${PYTHON_BIN:-${PROJECT_ROOT}/.venv/bin/python}"

if [[ ! -d "${SOURCE_TAR_DIR}" ]]; then
  echo "Source tar directory not found: ${SOURCE_TAR_DIR}" >&2
  exit 1
fi

mkdir -p "${UNCOMPRESS_DIR}"
mkdir -p "${BUILD_DIR}"

echo "[1/4] Sync tar archives into ${UNCOMPRESS_DIR}"
mapfile -t TAR_FILES < <(find "${SOURCE_TAR_DIR}" -maxdepth 1 -type f -name '*.tar' | sort)
for tar_file in "${TAR_FILES[@]}"; do
  tar_name="$(basename -- "${tar_file}")"
  target_dir="${UNCOMPRESS_DIR}/${tar_name%.tar}"
  if [[ ! -f "${target_dir}/items.jsonl" ]]; then
    mkdir -p "${target_dir}"
    tar -xf "${tar_file}" -C "${target_dir}"
    if [[ ! -f "${target_dir}/items.jsonl" && -f "${target_dir}/items.jsonl.gz" ]]; then
      gzip -dc "${target_dir}/items.jsonl.gz" > "${target_dir}/items.jsonl"
    fi
  fi
  if [[ ! -f "${target_dir}/items.jsonl" ]]; then
    echo "items.jsonl missing after extraction: ${target_dir}" >&2
    exit 1
  fi
done

echo "[2/4] Collect items.jsonl files"
mapfile -t ITEM_FILES < <(find "${UNCOMPRESS_DIR}" -type f -name 'items.jsonl' | sort)
if [[ ${#ITEM_FILES[@]} -eq 0 ]]; then
  echo "No items.jsonl files found under ${UNCOMPRESS_DIR}" >&2
  exit 1
fi

: > "${MERGED_INPUT}"
for item_file in "${ITEM_FILES[@]}"; do
  cat "${item_file}" >> "${MERGED_INPUT}"
done

echo "Merged ${#ITEM_FILES[@]} items.jsonl files into ${MERGED_INPUT}"

echo "[3/4] Run import/session-merge/round-feedback/stats"
INPUT_FILE="${MERGED_INPUT}" \
DB_PATH="${DB_PATH}" \
LLM_ENDPOINT="${LLM_ENDPOINT}" \
LLM_API_KEY="${LLM_API_KEY}" \
LLM_MODEL_ID="${LLM_MODEL_ID}" \
MAX_CONCURRENCY="${MAX_CONCURRENCY}" \
BATCH_SIZE="${BATCH_SIZE}" \
LLM_TIMEOUT="${LLM_TIMEOUT}" \
RUN_PRESSURE_TEST="${RUN_PRESSURE_TEST}" \
RUN_SESSION_MERGE="${RUN_SESSION_MERGE}" \
SESSION_MERGE_WORKERS="${SESSION_MERGE_WORKERS}" \
SESSION_MERGE_BATCH_SIZE="${SESSION_MERGE_BATCH_SIZE}" \
SESSION_MERGE_MIN_PREFIX_TURNS="${SESSION_MERGE_MIN_PREFIX_TURNS}" \
PYTHON_BIN="${PYTHON_BIN}" \
bash "${SCRIPT_DIR}/run_import_to_stats.sh"

echo "[4/4] Completed rebuild"
echo "Merged input: ${MERGED_INPUT}"
echo "Database: ${PROJECT_ROOT}/${DB_PATH}"