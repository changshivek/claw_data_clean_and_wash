#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
RUNNER_SCRIPT="${RUNNER_SCRIPT:-${SCRIPT_DIR}/docker_run_incremental_pipeline.sh}"

IMAGE_NAME="${IMAGE_NAME:-claw-incremental-pipeline}"
IMAGE_TAG="${IMAGE_TAG:-}"
CONTAINER_NAME="${CONTAINER_NAME:-claw-incremental-pipeline-prod-$(date +%Y%m%d_%H%M%S)}"
MEMORY_LIMIT_GIB="${MEMORY_LIMIT_GIB:-256}"
CHECK_INTERVAL_SECONDS="${CHECK_INTERVAL_SECONDS:-10}"
LOG_DIR="${LOG_DIR:-${PROJECT_ROOT}/data/docker_monitor}"
MONITOR_LOG="${MONITOR_LOG:-${LOG_DIR}/${CONTAINER_NAME}.memory.log}"

mkdir -p "${LOG_DIR}"

if [[ ! -x "${RUNNER_SCRIPT}" ]]; then
  echo "Runner script is not executable: ${RUNNER_SCRIPT}" >&2
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "docker command not found" >&2
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1 && ! command -v python >/dev/null 2>&1; then
  echo "python3 or python is required" >&2
  exit 1
fi

if command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
else
  PYTHON_BIN="python"
fi

if ! [[ "${MEMORY_LIMIT_GIB}" =~ ^[0-9]+$ ]]; then
  echo "MEMORY_LIMIT_GIB must be an integer, got: ${MEMORY_LIMIT_GIB}" >&2
  exit 1
fi

if ! [[ "${CHECK_INTERVAL_SECONDS}" =~ ^[0-9]+$ ]] || (( CHECK_INTERVAL_SECONDS <= 0 )); then
  echo "CHECK_INTERVAL_SECONDS must be a positive integer, got: ${CHECK_INTERVAL_SECONDS}" >&2
  exit 1
fi

select_image_tag() {
  if [[ -n "${IMAGE_TAG}" ]]; then
    printf '%s\n' "${IMAGE_TAG}"
    return
  fi

  local discovered_tag
  discovered_tag="$({ docker images --format '{{.Repository}}:{{.Tag}}' || true; } | awk -F ':' -v repo="${IMAGE_NAME}" '$1 == repo && $2 ~ /^prod-/ { print $2; exit }')"
  if [[ -n "${discovered_tag}" ]]; then
    printf '%s\n' "${discovered_tag}"
    return
  fi

  if docker image inspect "${IMAGE_NAME}:latest" >/dev/null 2>&1; then
    printf '%s\n' "latest"
    return
  fi

  echo "No suitable image tag found for ${IMAGE_NAME}. Set IMAGE_TAG explicitly." >&2
  exit 1
}

parse_usage_bytes() {
  local stats_json="$1"
  "${PYTHON_BIN}" - <<'PY' "${stats_json}"
import json
import re
import sys

payload = json.loads(sys.argv[1])
mem_usage = payload.get("MemUsage", "")
usage = mem_usage.split("/", 1)[0].strip()
match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]+)", usage)
if not match:
    raise SystemExit(f"Unable to parse memory usage: {mem_usage!r}")

value = float(match.group(1))
unit = match.group(2)
factors = {
    "B": 1,
    "KB": 1000,
    "MB": 1000 ** 2,
    "GB": 1000 ** 3,
    "TB": 1000 ** 4,
    "KIB": 1024,
    "MIB": 1024 ** 2,
    "GIB": 1024 ** 3,
    "TIB": 1024 ** 4,
}
factor = factors.get(unit.upper())
if factor is None:
    raise SystemExit(f"Unsupported memory unit: {unit}")

print(int(value * factor))
print(usage)
PY
}

IMAGE_TAG="$(select_image_tag)"
MEMORY_LIMIT_BYTES=$(( MEMORY_LIMIT_GIB * 1024 * 1024 * 1024 ))

echo "[$(date '+%F %T')] starting container ${CONTAINER_NAME} with image ${IMAGE_NAME}:${IMAGE_TAG}" | tee -a "${MONITOR_LOG}"
echo "[$(date '+%F %T')] memory limit: ${MEMORY_LIMIT_GIB} GiB; check interval: ${CHECK_INTERVAL_SECONDS}s" | tee -a "${MONITOR_LOG}"

container_id="$({
  IMAGE_NAME="${IMAGE_NAME}" \
  IMAGE_TAG="${IMAGE_TAG}" \
  CONTAINER_NAME="${CONTAINER_NAME}" \
  "${RUNNER_SCRIPT}"
} 2>&1)" || {
  printf '%s\n' "${container_id:-}" | tee -a "${MONITOR_LOG}" >&2
  exit 1
}

printf '%s\n' "${container_id}" | tee -a "${MONITOR_LOG}"

while true; do
  running="$(docker inspect --format '{{.State.Running}}' "${CONTAINER_NAME}" 2>/dev/null || echo false)"
  if [[ "${running}" != "true" ]]; then
    exit_code="$(docker inspect --format '{{.State.ExitCode}}' "${CONTAINER_NAME}" 2>/dev/null || echo unknown)"
    echo "[$(date '+%F %T')] container ${CONTAINER_NAME} is no longer running; exit_code=${exit_code}" | tee -a "${MONITOR_LOG}"
    break
  fi

  stats_json="$(docker stats --no-stream --format '{{json .}}' "${CONTAINER_NAME}" | head -n 1)"
  if [[ -z "${stats_json}" ]]; then
    echo "[$(date '+%F %T')] docker stats returned no data for ${CONTAINER_NAME}" | tee -a "${MONITOR_LOG}"
    sleep "${CHECK_INTERVAL_SECONDS}"
    continue
  fi

  mapfile -t usage_info < <(parse_usage_bytes "${stats_json}")
  usage_bytes="${usage_info[0]}"
  usage_display="${usage_info[1]}"

  echo "[$(date '+%F %T')] mem_usage=${usage_display} (${usage_bytes} bytes)" | tee -a "${MONITOR_LOG}"

  if (( usage_bytes > MEMORY_LIMIT_BYTES )); then
    echo "[$(date '+%F %T')] memory threshold exceeded for ${CONTAINER_NAME}; killing container" | tee -a "${MONITOR_LOG}"
    docker kill "${CONTAINER_NAME}" >/dev/null 2>&1 || true
    exit 137
  fi

  sleep "${CHECK_INTERVAL_SECONDS}"
done