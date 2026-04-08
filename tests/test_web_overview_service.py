"""Tests for overview-page helper metrics."""
import tempfile
from pathlib import Path

from claw_data_filter.models.sample import Sample
from claw_data_filter.storage.duckdb_store import DuckDBStore
from claw_data_filter.web.services.overview_service import get_processing_status_counts, get_session_merge_counts


def test_get_processing_status_counts_returns_all_buckets():
    with tempfile.TemporaryDirectory() as tmpdir:
        store = DuckDBStore(Path(tmpdir) / "overview.duckdb")
        first_id = store.insert_sample(
            Sample.from_dict(
                {
                    "messages": [
                        {"role": "user", "content": "hello"},
                        {"role": "assistant", "content": "hi"},
                    ]
                }
            )
        )
        second_id = store.insert_sample(
            Sample.from_dict(
                {
                    "messages": [
                        {"role": "user", "content": "question"},
                        {"role": "assistant", "content": "answer"},
                    ]
                }
            )
        )
        third_id = store.insert_sample(
            Sample.from_dict(
                {
                    "messages": [
                        {"role": "user", "content": "again"},
                        {"role": "assistant", "content": "reply"},
                    ]
                }
            )
        )

        store.conn.execute(
            "UPDATE samples SET processing_status = 'processing' WHERE id = ?",
            [first_id],
        )
        store.conn.execute(
            "UPDATE samples SET processing_status = 'completed' WHERE id = ?",
            [second_id],
        )
        store.conn.execute(
            "UPDATE samples SET processing_status = 'failed' WHERE id = ?",
            [third_id],
        )

        counts = get_processing_status_counts(store)

        assert counts == {
            "pending": 0,
            "processing": 1,
            "completed": 1,
            "failed": 1,
        }
        store.close()


def test_get_session_merge_counts_returns_merge_buckets():
    with tempfile.TemporaryDirectory() as tmpdir:
        store = DuckDBStore(Path(tmpdir) / "overview_merge.duckdb")
        keep_id = store.insert_sample(
            Sample.from_dict(
                {
                    "messages": [
                        {"role": "user", "content": "hello"},
                        {"role": "assistant", "content": "hi"},
                    ]
                }
            )
        )
        merged_id = store.insert_sample(
            Sample.from_dict(
                {
                    "messages": [
                        {"role": "user", "content": "question"},
                        {"role": "assistant", "content": "answer"},
                    ]
                }
            )
        )
        store.conn.execute("UPDATE samples SET session_merge_status = 'keep', session_merge_keep = TRUE WHERE id = ?", [keep_id])
        store.conn.execute("UPDATE samples SET session_merge_status = 'merged', session_merge_keep = FALSE WHERE id = ?", [merged_id])
        store.conn.execute("UPDATE samples SET empty_response = TRUE WHERE id = ?", [merged_id])

        counts = get_session_merge_counts(store)

        assert counts == {"total": 2, "keep": 1, "merged": 1, "skipped": 0, "unmarked": 0, "empty_response": 1}
        store.close()