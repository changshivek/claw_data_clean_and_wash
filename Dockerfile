FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    UV_SYSTEM_PYTHON=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends cron \
    && rm -rf /var/lib/apt/lists/*

RUN pip install uv

WORKDIR /app

COPY pyproject.toml README.md /app/
COPY claw_data_filter /app/claw_data_filter
COPY scripts /app/scripts
COPY configs /app/configs
COPY docker /app/docker

RUN uv pip install --system .

RUN mkdir -p /app/runtime

EXPOSE 8501

ENTRYPOINT ["/app/docker/entrypoint.sh"]