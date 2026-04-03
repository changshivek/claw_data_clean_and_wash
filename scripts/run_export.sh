#!/usr/bin/env bash

set -euo pipefail

# -----------------------------
# Configuration
# -----------------------------
DB_PATH="data/pipeline.duckdb"
EXPORT_PATH="data/exported.jsonl"
REPORT_PATH="data/export_report.json"

# Leave empty to disable a filter.
RESPONSE_HELPFUL_RATE=">=0.7"
USER_SATISFIED_RATE=""
HAS_ERROR="false"
LIMIT=""
GENERATE_REPORT="true"

# Optional override for Python executable.
# Default uses the project's virtualenv.
PYTHON_BIN=""


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

DB_PATH="${PROJECT_ROOT}/${DB_PATH}"
EXPORT_PATH="${PROJECT_ROOT}/${EXPORT_PATH}"
REPORT_PATH="${PROJECT_ROOT}/${REPORT_PATH}"

if [[ ! -f "${DB_PATH}" ]]; then
  echo "Database file not found: ${DB_PATH}" >&2
  echo "Please update DB_PATH in the config section." >&2
  exit 1
fi

mkdir -p "$(dirname -- "${EXPORT_PATH}")"
mkdir -p "$(dirname -- "${REPORT_PATH}")"

args=(
  -m claw_data_filter.cli
  --db-path "${DB_PATH}"
  filter
  --export "${EXPORT_PATH}"
)

if [[ -n "${RESPONSE_HELPFUL_RATE}" ]]; then
  args+=(--response-helpful-rate "${RESPONSE_HELPFUL_RATE}")
fi

if [[ -n "${USER_SATISFIED_RATE}" ]]; then
  args+=(--user-satisfied-rate "${USER_SATISFIED_RATE}")
fi

if [[ -n "${HAS_ERROR}" ]]; then
  args+=(--has-error "${HAS_ERROR}")
fi

if [[ -n "${LIMIT}" ]]; then
  args+=(--limit "${LIMIT}")
fi

if [[ "${GENERATE_REPORT}" == "true" ]]; then
  args+=(--report "${REPORT_PATH}")
fi

echo "Exporting data from: ${DB_PATH}"
"${PYTHON_BIN}" "${args[@]}"

echo "Done. Export written to: ${EXPORT_PATH}"
if [[ "${GENERATE_REPORT}" == "true" ]]; then
  echo "Report written to: ${REPORT_PATH}"
fi