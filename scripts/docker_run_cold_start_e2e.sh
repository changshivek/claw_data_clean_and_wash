#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

IMAGE_NAME="${IMAGE_NAME:-claw-incremental-pipeline}"
IMAGE_TAG="${IMAGE_TAG:-cold-start-e2e}"
CONFIG_PATH="${CONFIG_PATH:-${PROJECT_ROOT}/configs/autoprocess.pipeline.docker_cold_start_e2e.toml}"
SOURCE_DIR="${SOURCE_DIR:-${PROJECT_ROOT}/data/docker_cold_start_e2e/source}"
ARTIFACT_ROOT="${ARTIFACT_ROOT:-${PROJECT_ROOT}/data/docker_cold_start_e2e/artifacts}"
WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS:-900}"
LLM_ENDPOINT="${LLM_ENDPOINT:-http://120.220.102.24:31877/v1}"
LLM_API_KEY="${LLM_API_KEY:-dummy}"
LLM_MODEL_ID="${LLM_MODEL_ID:-qwen35}"

RUN_ID="$(date +%Y%m%d_%H%M%S)_${RANDOM}"
CONTAINER_NAME="${CONTAINER_NAME:-claw-cold-start-e2e-${RUN_ID}}"
VOLUME_NAME="${VOLUME_NAME:-claw-cold-start-e2e-${RUN_ID}}"
ARTIFACT_DIR="${ARTIFACT_ROOT}/${RUN_ID}"
CONTAINER_LOG="${ARTIFACT_DIR}/container.log"

if [[ ! -f "${CONFIG_PATH}" ]]; then
  echo "Config file not found: ${CONFIG_PATH}" >&2
  exit 1
fi

if [[ ! -d "${SOURCE_DIR}" ]]; then
  echo "Source dir not found: ${SOURCE_DIR}" >&2
  exit 1
fi

mkdir -p "${ARTIFACT_DIR}"

docker volume create "${VOLUME_NAME}" >/dev/null

docker run -d \
  --name "${CONTAINER_NAME}" \
  -e CONFIG_PATH="/app/configs/autoprocess.pipeline.docker_cold_start_e2e.toml" \
  -e RUN_ON_START="true" \
  -e CRON_SCHEDULE="59 23 31 12 *" \
  -e LLM_ENDPOINT="${LLM_ENDPOINT}" \
  -e LLM_API_KEY="${LLM_API_KEY}" \
  -e LLM_MODEL_ID="${LLM_MODEL_ID}" \
  -v "${CONFIG_PATH}:/app/configs/autoprocess.pipeline.docker_cold_start_e2e.toml:ro" \
  -v "${PROJECT_ROOT}/configs/unisound_export.autoprocess.json:/app/configs/unisound_export.autoprocess.json:ro" \
  -v "${SOURCE_DIR}:/app/test_source:ro" \
  -v "${VOLUME_NAME}:/app/runtime" \
  "${IMAGE_NAME}:${IMAGE_TAG}" >/dev/null

deadline=$(( $(date +%s) + WAIT_TIMEOUT_SECONDS ))
completed=0
failed=0

while (( $(date +%s) < deadline )); do
  docker logs "${CONTAINER_NAME}" > "${CONTAINER_LOG}" 2>&1 || true

  if grep -Fq "status: completed" "${CONTAINER_LOG}"; then
    completed=1
    break
  fi

  if grep -Fq "Incremental pipeline run failed" "${CONTAINER_LOG}"; then
    failed=1
    break
  fi

  sleep 5
done

if [[ "${completed}" -ne 1 ]]; then
  if [[ "${failed}" -eq 1 ]]; then
    echo "Cold-start pipeline run failed; see ${CONTAINER_LOG}" >&2
  else
    echo "Cold-start pipeline run did not reach completed status within timeout; see ${CONTAINER_LOG}" >&2
  fi
  exit 1
fi

docker exec "${CONTAINER_NAME}" test -f /app/runtime/docker_cold_start_e2e/db/incremental_pipeline.duckdb

docker cp "${CONTAINER_NAME}:/app/runtime/docker_cold_start_e2e" "${ARTIFACT_DIR}/runtime"

echo "container_name=${CONTAINER_NAME}"
echo "volume_name=${VOLUME_NAME}"
echo "artifact_dir=${ARTIFACT_DIR}"
find "${ARTIFACT_DIR}/runtime/export" -maxdepth 1 -type f | sort