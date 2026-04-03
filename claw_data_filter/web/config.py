"""Web app configuration."""
import os
from pathlib import Path

DB_PATH = Path(os.environ.get("DB_PATH", "data.duckdb"))