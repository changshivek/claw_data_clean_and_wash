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
    return store.filter_samples(
        helpful_rate_op=criteria.helpful_op,
        helpful_rate_val=criteria.helpful_val,
        satisfied_rate_op=criteria.satisfied_op,
        satisfied_rate_val=criteria.satisfied_val,
        negative_feedback_rate_op=criteria.negative_feedback_op,
        negative_feedback_rate_val=criteria.negative_feedback_val,
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
            SELECT id, num_turns, expected_judgment_count, num_tool_calls,
                   processing_status, imported_at,
                   json_extract(tool_stats, '$.response_helpful_rate') AS helpful_rate,
                   json_extract(tool_stats, '$.user_satisfied_rate') AS satisfied_rate
            FROM samples
            ORDER BY id
            LIMIT ? OFFSET ?
        """
        rows = store.conn.execute(query, [limit, offset]).fetchall()
        total = store.conn.execute("SELECT COUNT(*) FROM samples").fetchone()[0]
        columns = [
            "id",
            "num_turns",
            "expected_judgment_count",
            "num_tool_calls",
            "processing_status",
            "imported_at",
            "helpful_rate",
            "satisfied_rate",
        ]
        return columns, rows, total
    if table_name == "turn_judgments":
        query = """
            SELECT id, sample_id, turn_index, response_helpful, user_satisfied,
                   llm_error, created_at
            FROM turn_judgments
            ORDER BY sample_id, turn_index
            LIMIT ? OFFSET ?
        """
        rows = store.conn.execute(query, [limit, offset]).fetchall()
        total = store.conn.execute("SELECT COUNT(*) FROM turn_judgments").fetchone()[0]
        columns = [
            "id",
            "sample_id",
            "turn_index",
            "response_helpful",
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
