"""Filter query builder for selecting samples by evaluation criteria."""
import re
from dataclasses import dataclass
from enum import Enum
from typing import Optional


ALLOWED_FIELDS = frozenset(["progress_score", "overall_score", "tool_quality_score", "tool_success_rate", "task_type"])

ALLOWED_TASK_TYPES = frozenset(["information_retrieval", "data_processing", "coding", "reasoning", "creative", "general"])


class ComparisonOp(Enum):
    """SQL comparison operators."""
    EQ = "="
    NE = "!="
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="


@dataclass
class FilterCondition:
    """A single filter condition."""
    field: str
    op: ComparisonOp
    value: float | int | str

    def to_sql(self) -> str:
        """Convert to SQL WHERE clause fragment."""
        if self.field not in ALLOWED_FIELDS:
            raise ValueError(f"Invalid field name: {self.field}")
        if isinstance(self.value, str):
            escaped = self.value.replace("'", "''")  # SQL escape single quotes
            return f"{self.field} {self.op.value} '{escaped}'"
        return f"{self.field} {self.op.value} {self.value}"


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
        self.task_types: list[str] = []

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

    def add_task_type_filter(self, task_types: list[str]) -> "FilterQueryBuilder":
        """Filter by task type(s).

        Args:
            task_types: List of task types to include

        Returns:
            self for chaining
        """
        self.task_types.extend(task_types)
        return self

    def build_where_clause(self) -> str:
        """Build WHERE clause SQL fragment.

        Returns:
            SQL WHERE clause string (e.g., "progress_score >= 4 AND task_type IN ('coding')")
        """
        parts = []

        for cond in self.conditions:
            parts.append(cond.to_sql())

        if self.task_types:
            for tt in self.task_types:
                if tt not in ALLOWED_TASK_TYPES:
                    raise ValueError(f"Invalid task type: {tt}")
            types_str = ", ".join(f"'{t.replace('\'', '\'\'')}'" for t in self.task_types)
            parts.append(f"task_type IN ({types_str})")

        return " AND ".join(parts) if parts else "1=1"

    def get_filtered_samples_query(self, limit: Optional[int] = None) -> str:
        """Build complete SELECT query with filters.

        Args:
            limit: Optional row limit

        Returns:
            Complete SQL query string
        """
        where = self.build_where_clause()
        limit_str = f"LIMIT {limit}" if limit else ""

        return f"""
            SELECT s.id, s.raw_json, e.*
            FROM samples s
            JOIN evaluations e ON s.id = e.sample_id
            WHERE {where}
            {limit_str}
        """