"""Statistical report generation."""
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from claw_data_filter.storage.duckdb_store import DuckDBStore

logger = logging.getLogger(__name__)

# Allowed directories for I/O (relative to project root)
ALLOWED_IO_DIRS = ["data", "."]


def _validate_output_path(path: Path) -> None:
    """Validate output path is within allowed directories.

    Args:
        path: Path to validate

    Raises:
        ValueError: If path is outside allowed directories
    """
    path = path.resolve()
    allowed = [Path.cwd() / d for d in ALLOWED_IO_DIRS]
    for allowed_dir in allowed:
        try:
            path.relative_to(allowed_dir.resolve())
            return
        except ValueError:
            continue
    raise ValueError(f"Output path must be within allowed directories: {ALLOWED_IO_DIRS}")


class ReportExporter:
    """Generate and export statistical reports from evaluations."""

    def __init__(self, store: DuckDBStore):
        self.store = store

    def generate_report(self) -> dict[str, Any]:
        """Generate statistical report from samples and turn judgments.

        Returns:
            Dictionary containing summary statistics
        """
        stats = self.store.get_stats()

        report: dict[str, Any] = {
            "summary": {
                "total_samples": stats["total_samples"],
                "avg_response_helpful_rate": round(stats["avg_response_helpful_rate"], 2),
                "avg_user_satisfied_rate": round(stats["avg_user_satisfied_rate"], 2),
                "error_count": stats["error_count"],
            },
        }

        return report

    def export_report(self, output_path: Path) -> dict:
        """Export statistical report."""
        _validate_output_path(output_path)
        stats = self.store.get_stats()

        report = {
            "total_samples": stats["total_samples"],
            "avg_response_helpful_rate": stats["avg_response_helpful_rate"],
            "avg_user_satisfied_rate": stats["avg_user_satisfied_rate"],
            "error_count": stats["error_count"],
            "generated_at": datetime.now().isoformat(),
        }

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

        logger.info(f"Report saved to {output_path}")
        return report