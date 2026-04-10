"""Tests for table preview pagination and shared sample query helpers."""

import tempfile
from pathlib import Path
from unittest.mock import Mock

from claw_data_filter.models.round_judgment import AssistantResponseJudgment, FeedbackKind, UserEpisodeJudgment
from claw_data_filter.models.sample import Sample
from claw_data_filter.storage.duckdb_store import DuckDBStore
from claw_data_filter.web.services.sample_query_service import get_filtered_samples, get_samples_preview_page, get_table_preview
from claw_data_filter.web.view_models.filter_list_view import FilterCriteria


def test_get_filtered_samples_uses_current_filter_keyword_names():
    store = Mock()
    store.filter_samples.return_value = ([], 0)

    get_filtered_samples(
        store,
        FilterCriteria(
            progress_op="<=",
            progress_val=0.4,
            satisfied_op="!=",
            satisfied_val=0.8,
            negative_feedback_op=">=",
            negative_feedback_val=0.2,
        ),
        page_index=2,
        page_size=25,
    )

    _, kwargs = store.filter_samples.call_args
    assert kwargs["progress_op"] == "<="
    assert kwargs["progress_val"] == 0.4
    assert kwargs["satisfied_op"] == "!="
    assert kwargs["satisfied_val"] == 0.8
    assert kwargs["negative_feedback_op"] == ">="
    assert kwargs["negative_feedback_val"] == 0.2
    assert kwargs["limit"] == 25
    assert kwargs["offset"] == 25


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
        first_uid = store.get_sample_by_id(first_id)["sample_uid"]
        second_uid = store.get_sample_by_id(second_id)["sample_uid"]
        store.insert_assistant_response_judgment(
            AssistantResponseJudgment(
                sample_uid=first_uid,
                response_index=0,
                episode_index=0,
                assistant_message_index=1,
                feedback_kind=FeedbackKind.NONE,
                response_progress="uncertain",
            )
        )
        store.insert_assistant_response_judgment(
            AssistantResponseJudgment(
                sample_uid=second_uid,
                response_index=0,
                episode_index=0,
                assistant_message_index=1,
                feedback_kind=FeedbackKind.USER,
                feedback_message_start_index=2,
                feedback_message_end_index=2,
                feedback_payload=["good"],
                response_progress="yes",
            )
        )
        store.insert_user_episode_judgment(
            UserEpisodeJudgment(
                sample_uid=second_uid,
                episode_index=0,
                start_user_message_index=0,
                end_before_user_message_index=1,
                signal_from_users=["good"],
                user_satisfied="yes",
            )
        )

        columns, rows, total = get_table_preview(store, "samples", limit=10, offset=5)
        assert "id" in columns
        assert "sample_uid" in columns
        assert "expected_response_judgment_count" in columns
        assert "expected_episode_judgment_count" in columns
        assert total == 2
        assert rows == []

        response_columns, response_rows, response_total = get_table_preview(store, "assistant_response_judgments", limit=5, offset=0)
        assert "sample_uid" in response_columns
        assert "response_index" in response_columns
        assert response_total == 2
        assert len(response_rows) == 2

        episode_columns, episode_rows, episode_total = get_table_preview(store, "user_episode_judgments", limit=5, offset=0)
        assert "sample_uid" in episode_columns
        assert "episode_index" in episode_columns
        assert episode_total == 1
        assert len(episode_rows) == 1
        store.close()


def test_get_filtered_samples_supports_session_merge_filters():
    with tempfile.TemporaryDirectory() as tmpdir:
        store = DuckDBStore(Path(tmpdir) / "filtered.duckdb")
        keep_id = store.insert_sample(
            Sample.from_dict(
                {
                    "messages": [
                        {"role": "user", "content": "keep me"},
                        {"role": "assistant", "content": "answer"},
                    ]
                }
            )
        )
        merged_id = store.insert_sample(
            Sample.from_dict(
                {
                    "messages": [
                        {"role": "user", "content": "merge me"},
                        {"role": "assistant", "content": "answer"},
                    ]
                }
            )
        )
        store.conn.execute("UPDATE samples SET session_merge_status = 'keep', session_merge_keep = TRUE WHERE id = ?", [keep_id])
        store.conn.execute("UPDATE samples SET session_merge_status = 'merged', session_merge_keep = FALSE WHERE id = ?", [merged_id])

        rows, total = get_filtered_samples(
            store,
            FilterCriteria(progress_val=None, satisfied_val=None, session_merge_scope="merged", session_merge_status="merged"),
            page_index=1,
            page_size=20,
        )

        assert total == 1
        assert len(rows) == 1
        assert rows[0]["id"] == merged_id
        store.close()


def test_get_filtered_samples_supports_empty_response_scope():
    with tempfile.TemporaryDirectory() as tmpdir:
        store = DuckDBStore(Path(tmpdir) / "filtered_empty_response.duckdb")
        empty_id = store.insert_sample(
            Sample.from_dict(
                {
                    "messages": [
                        {"role": "user", "content": "keep me out"},
                    ]
                }
            )
        )
        normal_id = store.insert_sample(
            Sample.from_dict(
                {
                    "messages": [
                        {"role": "user", "content": "keep me in"},
                        {"role": "assistant", "content": "answer"},
                    ]
                }
            )
        )

        empty_rows, empty_total = get_filtered_samples(
            store,
            FilterCriteria(progress_val=None, satisfied_val=None, empty_response_scope="empty_only"),
            page_index=1,
            page_size=20,
        )
        normal_rows, normal_total = get_filtered_samples(
            store,
            FilterCriteria(progress_val=None, satisfied_val=None, empty_response_scope="non_empty_only"),
            page_index=1,
            page_size=20,
        )

        assert empty_total == 1
        assert empty_rows[0]["id"] == empty_id
        assert normal_total == 1
        assert normal_rows[0]["id"] == normal_id
        store.close()