"""Tests for Streamlit web database access helpers."""

from pathlib import Path

from claw_data_filter.web.services import database_access


def test_open_read_only_store_with_backoff_retries_busy_lock_and_succeeds(monkeypatch, tmp_path):
    attempts: list[tuple[Path, bool]] = []
    delays: list[float] = []
    sentinel = object()

    class FakeStore:
        def __new__(cls, db_path, read_only=False):
            attempts.append((Path(db_path), read_only))
            if len(attempts) < 3:
                raise RuntimeError("IO Error: Could not set lock on file 'sample.duckdb': Conflicting lock is held")
            return sentinel

    monkeypatch.setattr(database_access, "DuckDBStore", FakeStore)

    store, wait_state = database_access.open_read_only_store_with_backoff(
        tmp_path / "sample.duckdb",
        retry_delays=(0.1, 0.2, 0.4),
        sleep_fn=delays.append,
    )

    assert store is sentinel
    assert wait_state is None
    assert attempts == [
        (tmp_path / "sample.duckdb", True),
        (tmp_path / "sample.duckdb", True),
        (tmp_path / "sample.duckdb", True),
    ]
    assert delays == [0.1, 0.2]


def test_open_read_only_store_with_backoff_returns_wait_state_when_busy_persists(monkeypatch, tmp_path):
    db_path = tmp_path / "sample.duckdb"
    db_path.write_bytes(b"duckdb")
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    (log_dir / "pipeline.log").write_text("line1\nline2\nline3\n", encoding="utf-8")
    config_path = tmp_path / "pipeline.toml"
    config_path.write_text(
        """
[paths]
source_dir = "/tmp/source"
unpack_dir = "/tmp/unpack"
work_dir = "/tmp/work"
db_path = "sample.duckdb"
export_dir = "/tmp/export"
log_dir = "logs"

[export]
unisound_config_path = "/tmp/unisound.json"
""".strip(),
        encoding="utf-8",
    )

    delays: list[float] = []

    class BusyStore:
        def __new__(cls, db_path, read_only=False):
            raise RuntimeError("IO Error: Could not set lock on file 'sample.duckdb': Conflicting lock is held")

    monkeypatch.setattr(database_access, "DuckDBStore", BusyStore)
    monkeypatch.setenv("CONFIG_PATH", str(config_path))
    monkeypatch.setenv("SCHEDULER_MODE", "loop")

    store, wait_state = database_access.open_read_only_store_with_backoff(
        db_path,
        retry_delays=(0.1, 0.2),
        sleep_fn=delays.append,
    )

    assert store is None
    assert wait_state is not None
    assert wait_state.db_path == db_path.resolve()
    assert wait_state.attempts == 3
    assert wait_state.retry_delays == (0.1, 0.2)
    assert wait_state.db_exists is True
    assert wait_state.db_size_bytes == len(b"duckdb")
    assert wait_state.config_path == config_path.resolve()
    assert wait_state.scheduler_mode == "loop"
    assert wait_state.latest_log_path == (log_dir / "pipeline.log").resolve()
    assert wait_state.latest_log_lines == ["line1", "line2", "line3"]
    assert "Conflicting lock is held" in wait_state.last_error
    assert delays == [0.1, 0.2]