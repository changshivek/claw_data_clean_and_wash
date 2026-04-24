#!/usr/bin/env bash

set -euo pipefail

CONFIG_PATH="$1"
MIN_INTERVAL_HOURS="${2:-0}"
STATE_FILE="${CRON_STATE_FILE:-/app/runtime/cron.last_run_at}"

if ! [[ "${MIN_INTERVAL_HOURS}" =~ ^[0-9]+$ ]]; then
  echo "Invalid CRON_MIN_INTERVAL_HOURS: ${MIN_INTERVAL_HOURS}" >&2
  exit 1
fi

if (( MIN_INTERVAL_HOURS > 0 )) && [[ -f "${STATE_FILE}" ]]; then
  last_run_at="$(cat "${STATE_FILE}")"
  now="$(date +%s)"
  min_interval_seconds=$(( MIN_INTERVAL_HOURS * 3600 ))
  elapsed=$(( now - last_run_at ))
  if (( elapsed < min_interval_seconds )); then
    remaining=$(( min_interval_seconds - elapsed ))
    echo "Skipping scheduled pipeline run: remaining_seconds=${remaining} min_interval_hours=${MIN_INTERVAL_HOURS}"
    exit 0
  fi
fi

python -m claw_data_filter.cli pipeline-run --config "${CONFIG_PATH}" && date +%s > "${STATE_FILE}"