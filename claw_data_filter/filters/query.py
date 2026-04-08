"""Filter query builder for selecting samples by evaluation criteria."""
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional


ALLOWED_FIELDS = frozenset([
    "response_helpful_rate",
    "response_unhelpful_rate",
    "user_satisfied_rate",
    "user_negative_feedback_rate",
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

    if field == "response_helpful_rate":
        return f"{table_name}.response_helpful_rate"
    if field == "response_unhelpful_rate":
        return f"{table_name}.response_unhelpful_rate"
    if field == "user_satisfied_rate":
        return f"{table_name}.user_satisfied_rate"
    if field == "user_negative_feedback_rate":
        return f"{table_name}.user_negative_feedback_rate"
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
    """Build SQL WHERE clauses from filter conditions.

    Supports expressions like ">=4", "<5", "=coding" etc.
    """

    # Pattern for expressions with explicit field: "progress_score >= 4"
    OPERATOR_PATTERN = re.compile(r"^(\w+)\s*(>=|<=|>|<|!=|=)\s*(.+)$")
    # Pattern for shorthand expressions: ">=4", "<5"
    SHORTHAND_PATTERN = re.compile(r"^(>=|<=|>|<|!=|=)\s*(.+)$")

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

    def add_progress_score_filter(self, expr: str) -> "FilterQueryBuilder":
        """Parse expression like '>=4' and add progress_score filter.

        Args:
            expr: Filter expression like '>=4', '<5', '=4', 'progress_score >= 4'

        Returns:
            self for chaining

        Raises:
            ValueError: If expression is invalid
        """
        expr = expr.strip()

        # Try shorthand pattern first (no field name, e.g., ">=4")
        shorthand_match = self.SHORTHAND_PATTERN.match(expr)
        if shorthand_match:
            op_str, value_str = shorthand_match.groups()
            value = float(value_str) if "." in value_str else int(value_str)
            return self.add_condition("progress_score", ComparisonOp(op_str), value)

        # Try full pattern (with field name, e.g., "progress_score >= 4")
        match = self.OPERATOR_PATTERN.match(expr)
        if not match:
            raise ValueError(f"Invalid filter expression: {expr}")

        field, op_str, value_str = match.groups()
        if field not in ALLOWED_FIELDS:
            raise ValueError(f"Invalid field name: {field}")
        value = float(value_str) if "." in value_str else int(value_str)

        return self.add_condition(field, ComparisonOp(op_str), value)

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
            SELECT s.id, s.raw_json, s.tool_stats
            FROM samples s
            WHERE {where}
            {limit_str}
        """

    def get_parameterized_query(self, limit: Optional[int] = None) -> tuple[str, list[Any]]:
        """Build complete SELECT query with placeholders and params."""
        where, params = self.build_parameterized_where_clause(table_name="s")
        query = """
            SELECT s.id, s.raw_json, s.tool_stats
            FROM samples s
            WHERE {where}
        """.format(where=where)
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        return query, params