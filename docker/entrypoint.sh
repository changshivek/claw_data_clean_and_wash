#!/usr/bin/env bash

set -euo pipefail

APP_ROOT="/app"
CONFIG_PATH="${CONFIG_PATH:-${APP_ROOT}/configs/autoprocess.pipeline.toml}"
CRON_SCHEDULE="${CRON_SCHEDULE:-}"
CRON_MIN_INTERVAL_HOURS="${CRON_MIN_INTERVAL_HOURS:-0}"
RUN_ON_START="${RUN_ON_START:-false}"
SCHEDULER_MODE="${SCHEDULER_MODE:-cron}"
SCHEDULER_POLL_SECONDS="${SCHEDULER_POLL_SECONDS:-3600}"
STREAMLIT_HOST="${STREAMLIT_HOST:-0.0.0.0}"
STREAMLIT_PORT="${STREAMLIT_PORT:-8501}"

if [[ ! -f "${CONFIG_PATH}" ]]; then
  echo "Config file not found: ${CONFIG_PATH}" >&2
  exit 1
fi

export CONFIG_PATH

mapfile -t CONFIG_VALUES < <(python - <<'PY'
from pathlib import Path
import os

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore

config_path = Path(os.environ["CONFIG_PATH"])
payload = tomllib.loads(config_path.read_text(encoding="utf-8"))
print(payload["paths"]["db_path"])
print(payload.get("schedule", {}).get("cron", "*/30 * * * *"))
print(Path(payload["paths"]["log_dir"]).expanduser() / "cron.log")
print(Path(payload["paths"]["work_dir"]).expanduser() / "cron.last_run_at")
PY
)

DB_PATH_VALUE="${CONFIG_VALUES[0]}"
CONFIG_CRON_SCHEDULE="${CONFIG_VALUES[1]:-*/30 * * * *}"
CRON_LOG_PATH="${CRON_LOG_PATH:-${CONFIG_VALUES[2]}}"
CRON_STATE_FILE="${CRON_STATE_FILE:-${CONFIG_VALUES[3]}}"

if [[ -z "${CRON_SCHEDULE}" ]]; then
  CRON_SCHEDULE="${CONFIG_CRON_SCHEDULE}"
fi

export CRON_SCHEDULE
export DB_PATH="${DB_PATH:-${DB_PATH_VALUE}}"
export CRON_MIN_INTERVAL_HOURS
export CRON_LOG_PATH
export CRON_STATE_FILE
export SCHEDULER_MODE
export SCHEDULER_POLL_SECONDS

mapfile -t PIPELINE_DIRS < <(python - <<'PY'
from pathlib import Path
import os

try:
  import tomllib
except ModuleNotFoundError:
  import tomli as tomllib  # type: ignore

config_path = Path(os.environ["CONFIG_PATH"])
payload = tomllib.loads(config_path.read_text(encoding="utf-8"))
paths = payload["paths"]
dir_candidates = [
  Path(paths["db_path"]).expanduser().parent,
  Path(paths["unpack_dir"]).expanduser(),
  Path(paths["work_dir"]).expanduser(),
  Path(paths["export_dir"]).expanduser(),
  Path(paths["log_dir"]).expanduser(),
]
for path in dir_candidates:
  print(path)
PY
)

mkdir -p /app/runtime "${PIPELINE_DIRS[@]}"

touch "${CRON_LOG_PATH}"

if [[ "${RUN_ON_START}" == "true" ]]; then
  cd /app
  python -m claw_data_filter.cli pipeline-run --config "${CONFIG_PATH}"
fi

if [[ "${SCHEDULER_MODE}" == "cron" ]]; then
  sed \
    -e "s|\${CRON_SCHEDULE}|${CRON_SCHEDULE}|g" \
    -e "s|\${CONFIG_PATH}|${CONFIG_PATH}|g" \
    -e "s|\${CRON_MIN_INTERVAL_HOURS}|${CRON_MIN_INTERVAL_HOURS}|g" \
    -e "s|\${CRON_LOG_PATH}|${CRON_LOG_PATH}|g" \
    "${APP_ROOT}/docker/pipeline.cron" > /etc/cron.d/claw-incremental-pipeline
  chmod 0644 /etc/cron.d/claw-incremental-pipeline
  crontab /etc/cron.d/claw-incremental-pipeline
  cron
else
  "${APP_ROOT}/docker/scheduler_loop.sh" "${CONFIG_PATH}" "${CRON_MIN_INTERVAL_HOURS}" "${SCHEDULER_POLL_SECONDS}" >> "${CRON_LOG_PATH}" 2>&1 &
fi

cd /app
exec streamlit run claw_data_filter/web/app.py \
  --server.address "${STREAMLIT_HOST}" \
  --server.port "${STREAMLIT_PORT}"