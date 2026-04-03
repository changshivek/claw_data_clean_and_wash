"""Tests for table preview pagination helpers."""

import tempfile
from pathlib import Path

from claw_data_filter.models.sample import Sample
from claw_data_filter.storage.duckdb_store import DuckDBStore
from claw_data_filter.web.services.sample_query_service import get_samples_preview_page, get_table_preview


def test_get_samples_preview_page_returns_only_requested_page():
    with tempfile.TemporaryDirectory() as tmpdir:
        store = DuckDBStore(Path(tmpdir) / "tables.duckdb")
        for idx in range(25):
            store.insert_sample(
                Sample.from_dict(
                    {
                        "messages": [
                            {"role": "user", "content": f"question {idx}"},
                            {"role": "assistant", "content": "answer"},
                        ]
                    }
                )
            )

        rows, total = get_samples_preview_page(store, page_index=2, page_size=10)

        assert total == 25
        assert len(rows) == 10
        assert rows[0]["id"] == 11
        assert rows[-1]["id"] == 20
        store.close()


def test_get_table_preview_returns_rows_and_total_with_offset():
    with tempfile.TemporaryDirectory() as tmpdir:
        store = DuckDBStore(Path(tmpdir) / "turns.duckdb")
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
        store.replace_round_feedback_results(
            first_id,
            1,
            [],
            {"response_helpful_rate": 0.0, "user_satisfied_rate": 0.0, "total_turns": 0, "has_error": False},
        )
        store.insert_turn_judgment(
            __import__("claw_data_filter.models.round_judgment", fromlist=["RoundJudgment"]).RoundJudgment(
                sample_id=second_id,
                turn_index=0,
                response_helpful="yes",
                user_satisfied="yes",
            )
        )

        columns, rows, total = get_table_preview(store, "samples", limit=10, offset=5)
        assert "id" in columns
        assert total == 2
        assert rows == []

        turn_columns, turn_rows, turn_total = get_table_preview(store, "turn_judgments", limit=1, offset=0)
        assert "sample_id" in turn_columns
        assert turn_total == 1
        assert len(turn_rows) == 1
        store.close()