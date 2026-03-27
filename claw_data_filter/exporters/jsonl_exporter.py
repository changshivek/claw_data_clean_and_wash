"""JSONL export functionality."""
import json
import logging
import re
from pathlib import Path

from claw_data_filter.storage.duckdb_store import DuckDBStore

logger = logging.getLogger(__name__)

# Pattern for allowed SQL WHERE clause fragments (safe subset)
ALLOWED_WHERE_PATTERN = re.compile(
    r"^[\w\s><=!.\-\(\),\']+$"  # Only allow word chars, spaces, operators, parens
)


def _validate_filter_query(query: str) -> None:
    """Validate filter query is safe for use in WHERE clause."""
    if not ALLOWED_WHERE_PATTERN.match(query):
        # Also check for dangerous patterns
        dangerous = [
            ";", "--", "/*", "*/", "DROP", "DELETE", "INSERT", "UPDATE", "UNION", "EXEC", "EXECUTE"
        ]
        upper_query = query.upper()
        for pattern in dangerous:
            if pattern in upper_query:
                raise ValueError(f"Filter query contains disallowed pattern: {pattern}")


class JSONLExporter:
    """Export filtered samples to JSONL file."""

    def __init__(self, store: DuckDBStore):
        self.store = store

    def export(
        self,
        output_path: Path,
        filter_query: str | None = None,
        limit: int | None = None,
    ) -> int:
        """Export filtered samples to JSONL file.

        Args:
            output_path: Path to output JSONL file
            filter_query: Optional WHERE clause to filter samples
            limit: Optional maximum number of records to export

        Returns:
            Number of records exported
        """
        count = 0

        if filter_query:
            _validate_filter_query(filter_query)
            query = f"""
                SELECT s.raw_json
                FROM samples s
                JOIN evaluations e ON s.id = e.sample_id
                WHERE {filter_query}
            """
            if limit:
                query += f" LIMIT {limit}"
            rows = self.store.conn.execute(query).fetchall()
        else:
            query = "SELECT raw_json FROM samples"
            if limit:
                query += f" LIMIT {limit}"
            rows = self.store.conn.execute(query).fetchall()

        with open(output_path, "w", encoding="utf-8") as f:
            for row in rows:
                raw_json = json.loads(row[0])
                f.write(json.dumps(raw_json, ensure_ascii=False) + "\n")
                count += 1

        logger.info(f"Exported {count} samples to {output_path}")
        return count