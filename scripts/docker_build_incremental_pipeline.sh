#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
IMAGE_NAME="${IMAGE_NAME:-claw-incremental-pipeline}"
IMAGE_TAG="${IMAGE_TAG:-latest}"

cd "${PROJECT_ROOT}"
exec docker build -t "${IMAGE_NAME}:${IMAGE_TAG}" .