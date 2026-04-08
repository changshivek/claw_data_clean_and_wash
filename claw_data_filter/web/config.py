"""Web app configuration."""
import os
from pathlib import Path
from typing import Any, MutableMapping


DB_PATH = Path(os.environ.get("DB_PATH", "data.duckdb"))
ACTIVE_DB_PATH_KEY = "app.active_db_path"
ACTIVE_DB_PATH_INPUT_KEY = "app.active_db_path_input"


def _normalize_db_path(path_like: str | Path) -> Path:
	path = Path(path_like).expanduser()
	if not path.is_absolute():
		path = Path.cwd() / path
	return path.resolve()


def get_default_db_path() -> Path:
	"""Return the default database path configured at process start."""
	return _normalize_db_path(DB_PATH)


def ensure_db_path_state(session_state: MutableMapping[str, Any]) -> Path:
	"""Ensure Streamlit session state has an active database path."""
	if not session_state.get(ACTIVE_DB_PATH_KEY):
		default_path = get_default_db_path()
		session_state[ACTIVE_DB_PATH_KEY] = str(default_path)
		session_state[ACTIVE_DB_PATH_INPUT_KEY] = str(default_path)
	elif not session_state.get(ACTIVE_DB_PATH_INPUT_KEY):
		session_state[ACTIVE_DB_PATH_INPUT_KEY] = str(session_state[ACTIVE_DB_PATH_KEY])
	return Path(str(session_state[ACTIVE_DB_PATH_KEY]))


def get_active_db_path(session_state: MutableMapping[str, Any]) -> Path:
	"""Return the current active database path for this Streamlit session."""
	ensure_db_path_state(session_state)
	return _normalize_db_path(str(session_state[ACTIVE_DB_PATH_KEY]))


def apply_active_db_path(session_state: MutableMapping[str, Any], raw_path: str) -> tuple[bool, str | None, Path | None]:
	"""Validate and persist a new active database path."""
	candidate = raw_path.strip()
	if not candidate:
		return False, "数据库路径不能为空", None

	path = _normalize_db_path(candidate)
	if not path.exists():
		return False, f"数据库文件不存在: {path}", None
	if not path.is_file():
		return False, f"目标不是文件: {path}", None

	session_state[ACTIVE_DB_PATH_KEY] = str(path)
	session_state[ACTIVE_DB_PATH_INPUT_KEY] = str(path)
	return True, None, path