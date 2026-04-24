"""Database access helpers for the Streamlit web UI."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

import streamlit as st

from claw_data_filter.pipeline.config import PipelineConfig
from claw_data_filter.storage.duckdb_store import DuckDBStore
from claw_data_filter.web.components.page_shell import render_panel_note

DEFAULT_RETRY_DELAYS = (0.25, 0.5, 1.0, 2.0)
BUSY_ERROR_MARKERS = (
    "could not set lock on file",
    "conflicting lock is held",
    "database is locked",
    "resource temporarily unavailable",
)


@dataclass(slots=True)
class DatabaseWaitState:
    db_path: Path
    attempts: int
    retry_delays: tuple[float, ...]
    last_error: str
    db_exists: bool
    db_size_bytes: int | None
    db_mtime: datetime | None
    config_path: Path | None
    scheduler_mode: str | None
    latest_log_path: Path | None
    latest_log_lines: list[str]


def is_database_busy_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(marker in message for marker in BUSY_ERROR_MARKERS)


def open_read_only_store_with_backoff(
    db_path: Path,
    *,
    retry_delays: tuple[float, ...] = DEFAULT_RETRY_DELAYS,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> tuple[DuckDBStore | None, DatabaseWaitState | None]:
    attempts = 0
    last_error: Exception | None = None

    for attempt_index, delay in enumerate((*retry_delays, None), start=1):
        attempts = attempt_index
        try:
            return DuckDBStore(db_path, read_only=True), None
        except Exception as exc:
            if not is_database_busy_error(exc):
                raise
            last_error = exc
            if delay is None:
                break
            sleep_fn(delay)

    assert last_error is not None
    return None, _build_wait_state(db_path, attempts, retry_delays, str(last_error))


def open_read_only_store_or_render_waiting(
    db_path: Path,
    *,
    retry_delays: tuple[float, ...] = DEFAULT_RETRY_DELAYS,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> DuckDBStore | None:
    with st.spinner("数据库当前正被 pipeline 写入，正在退避重试..."):
        store, wait_state = open_read_only_store_with_backoff(
            db_path,
            retry_delays=retry_delays,
            sleep_fn=sleep_fn,
        )
    if store is not None:
        return store
    assert wait_state is not None
    render_database_waiting_page(wait_state)
    return None


def render_database_waiting_page(wait_state: DatabaseWaitState) -> None:
    render_panel_note(
        "检测到 pipeline 正在导入或处理当前数据库。Web 已暂停读取 DuckDB，避免与写入阶段争锁；请稍后重试。"
    )

    col1, col2, col3 = st.columns(3)
    col1.metric("重试次数", str(wait_state.attempts))
    col2.metric(
        "数据库文件",
        "已发现" if wait_state.db_exists else "未发现",
    )
    size_mb = "-"
    if wait_state.db_size_bytes is not None:
        size_mb = f"{wait_state.db_size_bytes / (1024 * 1024):.2f} MB"
    col3.metric("当前库大小", size_mb)

    st.caption(f"数据库路径: {wait_state.db_path}")
    if wait_state.db_mtime is not None:
        st.caption(f"最近修改时间: {wait_state.db_mtime.strftime('%Y-%m-%d %H:%M:%S')}")
    if wait_state.config_path is not None:
        st.caption(f"当前 CONFIG_PATH: {wait_state.config_path}")
    if wait_state.scheduler_mode:
        st.caption(f"调度模式: {wait_state.scheduler_mode}")

    st.warning(f"最近一次 DuckDB 锁冲突: {wait_state.last_error}")
    st.caption(
        "退避间隔: "
        + ", ".join(f"{delay:.2f}s" for delay in wait_state.retry_delays)
    )

    if wait_state.latest_log_path is not None:
        st.markdown(f"**最新 pipeline 日志:** {wait_state.latest_log_path}")
    if wait_state.latest_log_lines:
        with st.expander("查看最新 pipeline 日志片段", expanded=True):
            st.code("\n".join(wait_state.latest_log_lines), language="text")

    if st.button("立即重试", key="db_waiting.retry"):
        st.rerun()


def _build_wait_state(
    db_path: Path,
    attempts: int,
    retry_delays: tuple[float, ...],
    last_error: str,
) -> DatabaseWaitState:
    resolved_db_path = Path(db_path).expanduser().resolve()
    db_exists = resolved_db_path.exists()
    db_size_bytes = None
    db_mtime = None
    if db_exists:
        stats = resolved_db_path.stat()
        db_size_bytes = stats.st_size
        db_mtime = datetime.fromtimestamp(stats.st_mtime)

    config_path, latest_log_path, latest_log_lines = _load_runtime_log_snapshot(resolved_db_path)
    return DatabaseWaitState(
        db_path=resolved_db_path,
        attempts=attempts,
        retry_delays=retry_delays,
        last_error=last_error,
        db_exists=db_exists,
        db_size_bytes=db_size_bytes,
        db_mtime=db_mtime,
        config_path=config_path,
        scheduler_mode=os.getenv("SCHEDULER_MODE"),
        latest_log_path=latest_log_path,
        latest_log_lines=latest_log_lines,
    )


def _load_runtime_log_snapshot(db_path: Path) -> tuple[Path | None, Path | None, list[str]]:
    raw_config_path = os.getenv("CONFIG_PATH")
    if not raw_config_path:
        return None, None, []

    config_path = Path(raw_config_path).expanduser().resolve()
    if not config_path.exists():
        return config_path, None, []

    try:
        config = PipelineConfig.from_toml(config_path)
    except Exception:
        return config_path, None, []

    if config.paths.db_path.resolve() != db_path:
        return config_path, None, []

    log_dir = config.paths.log_dir
    if not log_dir.exists():
        return config_path, None, []

    latest_log_path = _pick_latest_log_file(log_dir)
    if latest_log_path is None:
        return config_path, None, []

    return config_path, latest_log_path, _tail_lines(latest_log_path)


def _pick_latest_log_file(log_dir: Path) -> Path | None:
    candidates = [path for path in log_dir.glob("*.log") if path.is_file()]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _tail_lines(path: Path, limit: int = 20) -> list[str]:
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return lines[-limit:]