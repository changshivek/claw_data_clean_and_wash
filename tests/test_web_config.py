"""Tests for Streamlit web database path state helpers."""

from pathlib import Path

from claw_data_filter.web.config import (
    ACTIVE_DB_PATH_INPUT_KEY,
    ACTIVE_DB_PATH_KEY,
    apply_active_db_path,
    ensure_db_path_state,
    get_active_db_path,
)


def test_ensure_db_path_state_initializes_defaults(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    session_state = {}

    active_path = ensure_db_path_state(session_state)

    assert session_state[ACTIVE_DB_PATH_KEY] == str(active_path)
    assert session_state[ACTIVE_DB_PATH_INPUT_KEY] == str(active_path)
    assert active_path == get_active_db_path(session_state)


def test_apply_active_db_path_accepts_existing_relative_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    db_file = Path("data") / "sample.duckdb"
    db_file.parent.mkdir(parents=True, exist_ok=True)
    db_file.write_text("", encoding="utf-8")
    session_state = {}

    ok, error_message, selected_path = apply_active_db_path(session_state, str(db_file))

    assert ok is True
    assert error_message is None
    assert selected_path == db_file.resolve()
    assert session_state[ACTIVE_DB_PATH_KEY] == str(db_file.resolve())
    assert get_active_db_path(session_state) == db_file.resolve()


def test_apply_active_db_path_rejects_missing_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    session_state = {}

    ok, error_message, selected_path = apply_active_db_path(session_state, "missing.duckdb")

    assert ok is False
    assert selected_path is None
    assert error_message is not None
