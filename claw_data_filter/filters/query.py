"""Filter query builder for selecting samples by evaluation criteria."""
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional


ALLOWED_FIELDS = frozenset([
    "response_progress_rate",
    "response_regress_rate",
    "user_satisfied_rate",
    "user_negative_feedback_rate",
    "empty_response",
    "session_merge_keep",
    "session_merge_status",
    "session_merge_reason",
    "session_merge_group_size",
    "has_error",
    "num_turns",
    "num_tool_calls",
])


class ComparisonOp(Enum):
    """SQL comparison operators."""
    EQ = "="
    NE = "!="
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="


# JSON fields stored in tool_stats column
JSON_FIELDS = frozenset([
    "has_error",
    "total_turns",
])


def _field_sql_expression(field: str, table_name: str = "samples") -> str:
    """Return SQL expression for a supported filter field."""
    if field not in ALLOWED_FIELDS:
        raise ValueError(f"Invalid field name: {field}")

    if field == "response_progress_rate":
        return f"{table_name}.response_progress_rate"
    if field == "response_regress_rate":
        return f"{table_name}.response_regress_rate"
    if field == "user_satisfied_rate":
        return f"{table_name}.user_satisfied_rate"
    if field == "user_negative_feedback_rate":
        return f"{table_name}.user_negative_feedback_rate"
    if field == "empty_response":
        return f"{table_name}.empty_response"
    if field == "session_merge_keep":
        return f"{table_name}.session_merge_keep"
    if field == "session_merge_status":
        return f"{table_name}.session_merge_status"
    if field == "session_merge_reason":
        return f"{table_name}.session_merge_reason"
    if field == "session_merge_group_size":
        return f"{table_name}.session_merge_group_size"
    if field == "has_error":
        return f"CAST(json_extract({table_name}.tool_stats, '$.has_error') AS BOOLEAN)"
    return f"{table_name}.{field}"


@dataclass
class FilterCondition:
    """A single filter condition."""
    field: str
    op: ComparisonOp
    value: float | int | str

    def to_sql_clause(self, table_name: str = "samples") -> tuple[str, list[Any]]:
        """Convert to parameterized SQL fragment and params."""
        field_ref = _field_sql_expression(self.field, table_name)
        return f"{field_ref} {self.op.value} ?", [self.value]

    def to_sql(self, table_name: str = "samples") -> str:
        """Convert to SQL WHERE clause fragment."""
        field_ref = _field_sql_expression(self.field, table_name)
        if isinstance(self.value, str):
            escaped = self.value.replace("'", "''")  # SQL escape single quotes
            return f"{field_ref} {self.op.value} '{escaped}'"
        return f"{field_ref} {self.op.value} {self.value}"


class FilterQueryBuilder:
    """Build SQL WHERE clauses from filter conditions."""

    def __init__(self):
        self.conditions: list[FilterCondition] = []

    def add_condition(self, field: str, op: ComparisonOp, value: float | int | str) -> "FilterQueryBuilder":
        """Add a filter condition.

        Args:
            field: Field name (e.g., 'progress_score')
            op: Comparison operator
            value: Value to compare against

        Returns:
            self for chaining
        """
        self.conditions.append(FilterCondition(field, op, value))
        return self

    def build_where_clause(self, table_name: str = "samples") -> str:
        """Build WHERE clause SQL fragment.

        Args:
            table_name: Table name for JSON field references

        Returns:
            SQL WHERE clause string (e.g., "progress_score >= 4 AND num_turns >= 2")
        """
        parts = []

        for cond in self.conditions:
            parts.append(cond.to_sql(table_name))

        return " AND ".join(parts) if parts else "1=1"

    def build_parameterized_where_clause(self, table_name: str = "samples") -> tuple[str, list[Any]]:
        """Build a parameterized WHERE clause and corresponding params."""
        parts: list[str] = []
        params: list[Any] = []

        for cond in self.conditions:
            clause, clause_params = cond.to_sql_clause(table_name)
            parts.append(clause)
            params.extend(clause_params)

        return (" AND ".join(parts) if parts else "1=1", params)

    def get_filtered_samples_query(self, limit: Optional[int] = None) -> str:
        """Build complete SELECT query with filters.

        For tool_stats fields, extracts from JSON using json_extract.
        """
        where = self.build_where_clause(table_name="s")
        limit_str = f"LIMIT {limit}" if limit else ""

        return f"""
            SELECT s.id, s.sample_uid, s.tool_stats
            FROM samples s
            WHERE {where}
            {limit_str}
        """

    def get_parameterized_query(self, limit: Optional[int] = None) -> tuple[str, list[Any]]:
        """Build complete SELECT query with placeholders and params."""
        where, params = self.build_parameterized_where_clause(table_name="s")
        query = """
            SELECT s.id, s.sample_uid, s.tool_stats
            FROM samples s
            WHERE {where}
        """.format(where=where)
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        return query, params