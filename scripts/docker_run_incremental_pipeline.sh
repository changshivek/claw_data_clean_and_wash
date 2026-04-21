#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
IMAGE_NAME="${IMAGE_NAME:-claw-incremental-pipeline}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
CONTAINER_NAME="${CONTAINER_NAME:-claw-incremental-pipeline}"
CONFIG_PATH="${CONFIG_PATH:-${PROJECT_ROOT}/configs/autoprocess.pipeline.toml}"
STREAMLIT_PORT="${STREAMLIT_PORT:-8501}"

if command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  echo "python3 or python is required to parse ${CONFIG_PATH}" >&2
  exit 1
fi

mapfile -t CONFIG_VALUES < <("${PYTHON_BIN}" - <<'PY' "${CONFIG_PATH}"
from pathlib import Path
import sys

try:
  import tomllib
except ModuleNotFoundError:
  import tomli as tomllib  # type: ignore

config_path = Path(sys.argv[1])
payload = tomllib.loads(config_path.read_text(encoding="utf-8"))
llm = payload.get("llm", {})
round_feedback = payload.get("round_feedback", {})
print("true" if round_feedback.get("enabled", True) else "false")
print(llm.get("endpoint", ""))
print(llm.get("api_key", ""))
print(llm.get("model_id", ""))
PY
)

ROUND_FEEDBACK_ENABLED="${CONFIG_VALUES[0]:-true}"
CONFIG_LLM_ENDPOINT="${CONFIG_VALUES[1]:-}"
CONFIG_LLM_API_KEY="${CONFIG_VALUES[2]:-}"
CONFIG_LLM_MODEL_ID="${CONFIG_VALUES[3]:-}"

EFFECTIVE_LLM_ENDPOINT="${LLM_ENDPOINT:-${CONFIG_LLM_ENDPOINT}}"
EFFECTIVE_LLM_API_KEY="${LLM_API_KEY:-${CONFIG_LLM_API_KEY}}"
EFFECTIVE_LLM_MODEL_ID="${LLM_MODEL_ID:-${CONFIG_LLM_MODEL_ID}}"

if docker ps -a --format '{{.Names}}' | grep -Fxq "${CONTAINER_NAME}"; then
  echo "Container already exists: ${CONTAINER_NAME}" >&2
  echo "Refusing to reuse, stop, restart, or remove existing containers automatically." >&2
  exit 1
fi

if [[ ! -f "${CONFIG_PATH}" ]]; then
  echo "Config file not found: ${CONFIG_PATH}" >&2
  exit 1
fi

if [[ "${ROUND_FEEDBACK_ENABLED}" == "true" ]]; then
  MISSING_FIELDS=()
  [[ -n "${EFFECTIVE_LLM_ENDPOINT}" ]] || MISSING_FIELDS+=("LLM_ENDPOINT or llm.endpoint")
  [[ -n "${EFFECTIVE_LLM_API_KEY}" ]] || MISSING_FIELDS+=("LLM_API_KEY or llm.api_key")
  [[ -n "${EFFECTIVE_LLM_MODEL_ID}" ]] || MISSING_FIELDS+=("LLM_MODEL_ID or llm.model_id")

  if (( ${#MISSING_FIELDS[@]} > 0 )); then
    echo "Missing LLM configuration for round feedback: ${MISSING_FIELDS[*]}" >&2
    exit 1
  fi
fi

cd "${PROJECT_ROOT}"
exec docker run -d \
  --name "${CONTAINER_NAME}" \
  -p "${STREAMLIT_PORT}:8501" \
  -e CONFIG_PATH="/app/configs/autoprocess.pipeline.toml" \
  -e LLM_ENDPOINT="${EFFECTIVE_LLM_ENDPOINT}" \
  -e LLM_API_KEY="${EFFECTIVE_LLM_API_KEY}" \
  -e LLM_MODEL_ID="${EFFECTIVE_LLM_MODEL_ID}" \
  -v "${CONFIG_PATH}:/app/configs/autoprocess.pipeline.toml:ro" \
  -v /kanas/nlp/liuchang/manydata/unirouter:/kanas/nlp/liuchang/manydata/unirouter:ro \
  -v /kanas/nlp/liuchang/manydata/unirouter_uncompress:/kanas/nlp/liuchang/manydata/unirouter_uncompress \
  -v /kanas/nlp/liuchang/manydata/unirouter_duckdb:/kanas/nlp/liuchang/manydata/unirouter_duckdb \
  -v /kanas/nlp/liuchang/manydata/unirouter_in_process:/kanas/nlp/liuchang/manydata/unirouter_in_process \
  -v /kanas/nlp/liuchang/manydata/unirouter_unisound_format:/kanas/nlp/liuchang/manydata/unirouter_unisound_format \
  "${IMAGE_NAME}:${IMAGE_TAG}"