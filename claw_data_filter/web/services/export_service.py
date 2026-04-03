"""Export-related helpers for the Streamlit web UI."""
from typing import Any

from claw_data_filter.storage.duckdb_store import DuckDBStore
from claw_data_filter.web.view_models.filter_list_view import FilterCriteria


def build_export_where_clause(criteria: FilterCriteria) -> tuple[str, list[Any]]:
    """Build WHERE clause for export and preview queries."""
    from claw_data_filter.filters.query import ComparisonOp, FilterQueryBuilder

    builder = FilterQueryBuilder()
    if criteria.helpful_val is not None:
        builder.add_condition("response_helpful_rate", ComparisonOp(criteria.helpful_op), criteria.helpful_val)
    if criteria.satisfied_val is not None:
        builder.add_condition("user_satisfied_rate", ComparisonOp(criteria.satisfied_op), criteria.satisfied_val)
    if criteria.num_turns_min is not None and criteria.num_turns_min > 0:
        builder.add_condition("num_turns", ComparisonOp.GTE, criteria.num_turns_min)
    if criteria.num_turns_max is not None and criteria.num_turns_max < 100:
        builder.add_condition("num_turns", ComparisonOp.LTE, criteria.num_turns_max)

    where_clause, params = builder.build_parameterized_where_clause("samples")
    if where_clause == "1=1":
        where_clause = "tool_stats IS NOT NULL"
    else:
        where_clause = f"tool_stats IS NOT NULL AND {where_clause}"

    if criteria.date_from:
        where_clause += " AND imported_at >= ?"
        params.append(criteria.date_from)
    if criteria.date_to:
        where_clause += " AND imported_at <= ?"
        params.append(criteria.date_to)

    return where_clause, params


def preview_export(store: DuckDBStore, criteria: FilterCriteria) -> dict[str, float | int]:
    """Return export preview metadata."""
    where_clause, params = build_export_where_clause(criteria)
    query = f"""
        SELECT COUNT(*), COALESCE(AVG(length(CAST(raw_json AS VARCHAR))), 0)
        FROM samples
        WHERE {where_clause}
    """
    count, avg_chars = store.conn.execute(query, params).fetchone()
    estimated_bytes = int((avg_chars or 0) * (count or 0))
    return {
        "count": int(count or 0),
        "estimated_bytes": estimated_bytes,
    }


def fetch_export_rows(
    store: DuckDBStore,
    criteria: FilterCriteria,
    columns: list[str],
) -> list[tuple[Any, ...]]:
    """Fetch export rows with parameterized filtering."""
    where_clause, params = build_export_where_clause(criteria)
    query = f"SELECT {', '.join(columns)} FROM samples WHERE {where_clause}"
    return store.conn.execute(query, params).fetchall()
