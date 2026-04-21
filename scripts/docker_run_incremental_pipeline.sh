#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
IMAGE_NAME="${IMAGE_NAME:-claw-incremental-pipeline}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
CONTAINER_NAME="${CONTAINER_NAME:-claw-incremental-pipeline}"
CONFIG_PATH="${CONFIG_PATH:-${PROJECT_ROOT}/configs/autoprocess.pipeline.toml}"
STREAMLIT_PORT="${STREAMLIT_PORT:-8501}"

if docker ps -a --format '{{.Names}}' | grep -Fxq "${CONTAINER_NAME}"; then
  echo "Container already exists: ${CONTAINER_NAME}" >&2
  echo "Refusing to reuse, stop, restart, or remove existing containers automatically." >&2
  exit 1
fi

if [[ ! -f "${CONFIG_PATH}" ]]; then
  echo "Config file not found: ${CONFIG_PATH}" >&2
  exit 1
fi

cd "${PROJECT_ROOT}"
exec docker run -d \
  --name "${CONTAINER_NAME}" \
  -p "${STREAMLIT_PORT}:8501" \
  -e CONFIG_PATH="/app/configs/autoprocess.pipeline.toml" \
  -v "${CONFIG_PATH}:/app/configs/autoprocess.pipeline.toml:ro" \
  -v /kanas/nlp/liuchang/manydata/unirouter:/kanas/nlp/liuchang/manydata/unirouter:ro \
  -v /kanas/nlp/liuchang/manydata/unirouter_uncompress:/kanas/nlp/liuchang/manydata/unirouter_uncompress \
  -v /kanas/nlp/liuchang/manydata/unirouter_duckdb:/kanas/nlp/liuchang/manydata/unirouter_duckdb \
  -v /kanas/nlp/liuchang/manydata/unirouter_in_process:/kanas/nlp/liuchang/manydata/unirouter_in_process \
  -v /kanas/nlp/liuchang/manydata/unirouter_unisound_format:/kanas/nlp/liuchang/manydata/unirouter_unisound_format \
  "${IMAGE_NAME}:${IMAGE_TAG}"