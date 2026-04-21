#!/usr/bin/env bash

set -euo pipefail

APP_ROOT="/app"
CONFIG_PATH="${CONFIG_PATH:-${APP_ROOT}/configs/autoprocess.pipeline.toml}"
CRON_SCHEDULE="${CRON_SCHEDULE:-*/30 * * * *}"
RUN_ON_START="${RUN_ON_START:-false}"
STREAMLIT_HOST="${STREAMLIT_HOST:-0.0.0.0}"
STREAMLIT_PORT="${STREAMLIT_PORT:-8501}"

if [[ ! -f "${CONFIG_PATH}" ]]; then
  echo "Config file not found: ${CONFIG_PATH}" >&2
  exit 1
fi

export CONFIG_PATH

DB_PATH_VALUE="$(python - <<'PY'
from pathlib import Path
import os

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore

config_path = Path(os.environ["CONFIG_PATH"])
payload = tomllib.loads(config_path.read_text(encoding="utf-8"))
print(payload["paths"]["db_path"])
PY
 )"

export CRON_SCHEDULE
export DB_PATH="${DB_PATH:-${DB_PATH_VALUE}}"

mkdir -p /app/runtime

sed \
  -e "s|\${CRON_SCHEDULE}|${CRON_SCHEDULE}|g" \
  -e "s|\${CONFIG_PATH}|${CONFIG_PATH}|g" \
  "${APP_ROOT}/docker/pipeline.cron" > /etc/cron.d/claw-incremental-pipeline
chmod 0644 /etc/cron.d/claw-incremental-pipeline
crontab /etc/cron.d/claw-incremental-pipeline

touch /app/runtime/cron.log

if [[ "${RUN_ON_START}" == "true" ]]; then
  cd /app
  python -m claw_data_filter.cli pipeline-run --config "${CONFIG_PATH}"
fi

cron

cd /app
exec streamlit run claw_data_filter/web/app.py \
  --server.address "${STREAMLIT_HOST}" \
  --server.port "${STREAMLIT_PORT}"