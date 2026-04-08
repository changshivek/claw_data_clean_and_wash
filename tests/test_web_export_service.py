"""Tests for export service helpers used by the Streamlit UI."""
import tempfile
from pathlib import Path

from claw_data_filter.models.sample import Sample
from claw_data_filter.storage.duckdb_store import DuckDBStore
from claw_data_filter.web.services.export_service import (
    build_export_where_clause,
    fetch_export_rows,
    preview_export,
)
from claw_data_filter.web.view_models.filter_list_view import FilterCriteria


def test_build_export_where_clause_includes_dates_and_tool_stats():
    criteria = FilterCriteria(
        helpful_op=">=",
        helpful_val=0.8,
        satisfied_op=">=",
        satisfied_val=0.5,
        negative_feedback_op=">=",
        negative_feedback_val=0.2,
        empty_response_scope="non_empty_only",
        session_merge_scope="keep",
        session_merge_status="keep",
        date_from="2026-04-01",
        date_to="2026-04-03",
    )

    where_clause, params = build_export_where_clause(criteria)

    assert "tool_stats IS NOT NULL" in where_clause
    assert "empty_response = ?" in where_clause
    assert "COALESCE(session_merge_keep, TRUE) = TRUE" in where_clause
    assert "session_merge_status = ?" in where_clause
    assert "imported_at >= ?" in where_clause
    assert "imported_at <= ?" in where_clause
    assert params == [0.8, 0.5, 0.2, False, "keep", "2026-04-01", "2026-04-03"]


def test_preview_export_returns_count_and_size_estimate():
    with tempfile.TemporaryDirectory() as tmpdir:
        store = DuckDBStore(Path(tmpdir) / "preview.duckdb")

        for idx in range(2):
            sample_id = store.insert_sample(
                Sample.from_dict(
                    {
                        "messages": [
                            {"role": "user", "content": f"Question {idx}"},
                            {"role": "assistant", "content": "Answer"},
                        ]
                    }
                )
            )
            store.update_sample_tool_stats(
                sample_id,
                {
                    "response_helpful_rate": 0.9,
                    "user_satisfied_rate": 0.8,
                    "total_turns": 1,
                    "has_error": False,
                },
            )

        preview = preview_export(store, FilterCriteria())

        assert preview["count"] == 2
        assert int(preview["estimated_bytes"]) > 0
        store.close()


def test_fetch_export_rows_respects_shared_criteria():
    with tempfile.TemporaryDirectory() as tmpdir:
        store = DuckDBStore(Path(tmpdir) / "rows.duckdb")

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
                        {"role": "user", "content": "world"},
                        {"role": "assistant", "content": "ok"},
                    ]
                }
            )
        )
        store.update_sample_tool_stats(
            first_id,
            {
                "response_helpful_rate": 0.9,
                "user_satisfied_rate": 0.8,
                "total_turns": 1,
                "has_error": False,
            },
        )
        store.update_sample_tool_stats(
            second_id,
            {
                "response_helpful_rate": 0.2,
                "user_satisfied_rate": 0.3,
                "total_turns": 1,
                "has_error": False,
            },
        )

        rows = fetch_export_rows(
            store,
            FilterCriteria(helpful_op=">=", helpful_val=0.8, satisfied_op=">=", satisfied_val=0.5),
            ["id", "raw_json"],
        )

        assert len(rows) == 1
        assert rows[0][0] == first_id
        store.close()


def test_fetch_export_rows_can_filter_empty_response():
    with tempfile.TemporaryDirectory() as tmpdir:
        store = DuckDBStore(Path(tmpdir) / "rows_empty_response.duckdb")

        empty_id = store.insert_sample(
            Sample.from_dict(
                {
                    "messages": [
                        {"role": "user", "content": "hello"},
                    ]
                }
            )
        )
        store.update_sample_tool_stats(
            empty_id,
            {
                "response_helpful_rate": 0.0,
                "user_satisfied_rate": 0.0,
                "total_turns": 0,
                "has_error": False,
            },
        )
        normal_id = store.insert_sample(
            Sample.from_dict(
                {
                    "messages": [
                        {"role": "user", "content": "world"},
                        {"role": "assistant", "content": "ok"},
                    ]
                }
            )
        )
        store.update_sample_tool_stats(
            normal_id,
            {
                "response_helpful_rate": 1.0,
                "user_satisfied_rate": 1.0,
                "total_turns": 1,
                "has_error": False,
            },
        )

        rows = fetch_export_rows(
            store,
            FilterCriteria(helpful_val=None, satisfied_val=None, empty_response_scope="empty_only"),
            ["id", "empty_response"],
        )

        assert rows == [(empty_id, True)]
        store.close()