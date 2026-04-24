"""Centralized logging configuration shared across CLI, pipeline, and Web."""

from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# RotatingFileHandler defaults (applied only when log_file is supplied).
DEFAULT_MAX_BYTES = 50 * 1024 * 1024  # 50 MB
DEFAULT_BACKUP_COUNT = 5


def configure_logging(
    *,
    level: int = logging.INFO,
    log_file: Path | None = None,
    max_bytes: int = DEFAULT_MAX_BYTES,
    backup_count: int = DEFAULT_BACKUP_COUNT,
) -> None:
    """Idempotent root-logger setup.

    Always writes to stderr.  When *log_file* is given a rotating file
    handler is attached as well.
    """
    root = logging.getLogger()
    root.setLevel(level)

    _remove_existing_handlers(root)

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(level)
    stderr_handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))
    root.addHandler(stderr_handler)

    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            str(log_file),
            encoding="utf-8",
            maxBytes=max_bytes,
            backupCount=backup_count,
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))
        root.addHandler(file_handler)

    _quiet_third_party_loggers()


def make_file_handler(log_path: Path) -> logging.FileHandler:
    """Create a plain (non-rotating) file handler for per-run pipeline logs."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(str(log_path), encoding="utf-8")
    handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))
    return handler


def _remove_existing_handlers(root: logging.Logger) -> None:
    for handler in root.handlers[:]:
        root.removeHandler(handler)
        handler.close()


def _quiet_third_party_loggers() -> None:
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
