"""Overview-page helpers for status-aware sample metrics."""

from claw_data_filter.storage.duckdb_store import DuckDBStore


def get_processing_status_counts(store: DuckDBStore) -> dict[str, int]:
    """Return counts grouped by processing status."""
    rows = store.conn.execute(
        """
        SELECT COALESCE(processing_status, 'pending') AS status, COUNT(*)
        FROM samples
        GROUP BY 1
        """
    ).fetchall()
    counts = {"pending": 0, "processing": 0, "completed": 0, "failed": 0}
    for status, count in rows:
        counts[str(status)] = int(count)
    return counts
