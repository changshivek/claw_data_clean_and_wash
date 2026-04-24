#!/usr/bin/env bash

set -euo pipefail

CONFIG_PATH="$1"
MIN_INTERVAL_HOURS="${2:-0}"
POLL_SECONDS="${3:-3600}"

if ! [[ "${POLL_SECONDS}" =~ ^[0-9]+$ ]] || (( POLL_SECONDS <= 0 )); then
  echo "Invalid SCHEDULER_POLL_SECONDS: ${POLL_SECONDS}" >&2
  exit 1
fi

while true; do
  bash /app/docker/run_pipeline_if_due.sh "${CONFIG_PATH}" "${MIN_INTERVAL_HOURS}"
  sleep "${POLL_SECONDS}"
done