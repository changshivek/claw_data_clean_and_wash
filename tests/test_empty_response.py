"""Tests for empty_response backfill helpers."""

import tempfile
from pathlib import Path

from claw_data_filter.empty_response import backfill_empty_response, has_empty_response
from claw_data_filter.models.sample import Sample
from claw_data_filter.storage.duckdb_store import DuckDBStore


def test_detect_empty_response_returns_true_for_user_only_payload():
    messages = [{"role": "user", "content": "只有用户消息"}]
    assert has_empty_response(messages) is True


def test_backfill_empty_response_updates_existing_rows():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "backfill_empty_response.duckdb"
        store = DuckDBStore(db_path)
        store.insert_sample(Sample.from_dict({
            "messages": [
                {"role": "user", "content": "hello"},
            ]
        }))
        store.insert_sample(Sample.from_dict({
            "messages": [
                {"role": "user", "content": "hello"},
                {"role": "assistant", "content": "hi"},
            ]
        }))
        store.conn.execute("UPDATE samples SET empty_response = FALSE")
        store.close()

        summary = backfill_empty_response(db_path)

        assert summary == {
            "total_samples": 2,
            "empty_response_count": 1,
            "updated_count": 1,
        }

        store = DuckDBStore(db_path, read_only=True)
        rows = store.conn.execute("SELECT id, empty_response FROM samples ORDER BY id").fetchall()
        assert rows == [(1, True), (2, False)]
        store.close()