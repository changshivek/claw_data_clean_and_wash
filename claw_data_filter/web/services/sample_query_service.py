"""Thin query services used by Streamlit pages."""
from typing import Any

from claw_data_filter.storage.duckdb_store import DuckDBStore
from claw_data_filter.web.view_models.filter_list_view import FilterCriteria


def get_filtered_samples(
    store: DuckDBStore,
    criteria: FilterCriteria,
    page_index: int,
    page_size: int,
) -> tuple[list[dict[str, Any]], int]:
    """Fetch a single page of filtered sample records."""
    session_merge_keep: bool | None = None
    session_merge_status: str | None = None
    empty_response: bool | None = None

    if criteria.empty_response_scope == "empty_only":
        empty_response = True
    elif criteria.empty_response_scope == "non_empty_only":
        empty_response = False

    if criteria.session_merge_scope == "keep":
        session_merge_keep = True
    elif criteria.session_merge_scope == "merged":
        session_merge_keep = False
    if criteria.session_merge_status != "all":
        session_merge_status = criteria.session_merge_status

    return store.filter_samples(
        helpful_rate_op=criteria.helpful_op,
        helpful_rate_val=criteria.helpful_val,
        satisfied_rate_op=criteria.satisfied_op,
        satisfied_rate_val=criteria.satisfied_val,
        negative_feedback_rate_op=criteria.negative_feedback_op,
        negative_feedback_rate_val=criteria.negative_feedback_val,
        empty_response=empty_response,
        session_merge_keep=session_merge_keep,
        session_merge_status=session_merge_status,
        num_turns_min=criteria.num_turns_min,
        num_turns_max=criteria.num_turns_max,
        date_from=criteria.date_from,
        date_to=criteria.date_to,
        limit=page_size,
        offset=max(0, page_index - 1) * page_size,
    )


def get_samples_preview(store: DuckDBStore, limit: int = 20) -> list[dict[str, Any]]:
    """Fetch a lightweight samples preview for the tables page."""
    rows, _ = store.filter_samples(limit=limit, offset=0)
    return rows


def get_samples_preview_page(
    store: DuckDBStore,
    page_index: int,
    page_size: int = 20,
) -> tuple[list[dict[str, Any]], int]:
    """Fetch a single preview page for the samples table."""
    return store.filter_samples(limit=page_size, offset=max(0, page_index - 1) * page_size)


def get_table_preview(
    store: DuckDBStore,
    table_name: str,
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[str], list[tuple[Any, ...]], int]:
    """Fetch preview rows for a table."""
    if table_name == "samples":
        query = """
                        SELECT id, sample_uid, num_turns, expected_judgment_count,
                                     expected_response_judgment_count, expected_episode_judgment_count, num_tool_calls,
                 empty_response, processing_status, session_merge_status, session_merge_keep,
                   session_merge_reason, imported_at,
                   response_helpful_rate AS helpful_rate,
                   user_satisfied_rate AS satisfied_rate
            FROM samples
            ORDER BY id
            LIMIT ? OFFSET ?
        """
        rows = store.conn.execute(query, [limit, offset]).fetchall()
        total = store.conn.execute("SELECT COUNT(*) FROM samples").fetchone()[0]
        columns = [
            "id",
            "sample_uid",
            "num_turns",
            "expected_judgment_count",
            "expected_response_judgment_count",
            "expected_episode_judgment_count",
            "num_tool_calls",
            "empty_response",
            "processing_status",
            "session_merge_status",
            "session_merge_keep",
            "session_merge_reason",
            "imported_at",
            "helpful_rate",
            "satisfied_rate",
        ]
        return columns, rows, total
    if table_name == "assistant_response_judgments":
        query = """
            SELECT judgment_uid, sample_uid, response_index, episode_index,
                   assistant_message_index, feedback_kind,
                   feedback_message_start_index, feedback_message_end_index,
                   response_helpful, llm_error, created_at
            FROM assistant_response_judgments
            ORDER BY sample_uid, response_index
            LIMIT ? OFFSET ?
        """
        rows = store.conn.execute(query, [limit, offset]).fetchall()
        total = store.conn.execute("SELECT COUNT(*) FROM assistant_response_judgments").fetchone()[0]
        columns = [
            "judgment_uid",
            "sample_uid",
            "response_index",
            "episode_index",
            "assistant_message_index",
            "feedback_kind",
            "feedback_message_start_index",
            "feedback_message_end_index",
            "response_helpful",
            "llm_error",
            "created_at",
        ]
        return columns, rows, total
    if table_name == "user_episode_judgments":
        query = """
            SELECT judgment_uid, sample_uid, episode_index,
                   start_user_message_index, end_before_user_message_index,
                   user_satisfied, llm_error, created_at
            FROM user_episode_judgments
            ORDER BY sample_uid, episode_index
            LIMIT ? OFFSET ?
        """
        rows = store.conn.execute(query, [limit, offset]).fetchall()
        total = store.conn.execute("SELECT COUNT(*) FROM user_episode_judgments").fetchone()[0]
        columns = [
            "judgment_uid",
            "sample_uid",
            "episode_index",
            "start_user_message_index",
            "end_before_user_message_index",
            "user_satisfied",
            "llm_error",
            "created_at",
        ]
        return columns, rows, total

    query = f"SELECT * FROM {table_name} LIMIT ? OFFSET ?"
    rows = store.conn.execute(query, [limit, offset]).fetchall()
    total = store.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    columns = [desc[0] for desc in store.conn.description]
    return columns, rows, total
