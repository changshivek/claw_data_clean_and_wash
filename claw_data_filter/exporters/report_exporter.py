"""Statistical report generation."""
import json
import logging
from pathlib import Path
from typing import Any

from claw_data_filter.storage.duckdb_store import DuckDBStore

logger = logging.getLogger(__name__)


class ReportExporter:
    """Generate and export statistical reports from evaluations."""

    def __init__(self, store: DuckDBStore):
        self.store = store

    def generate_report(self) -> dict[str, Any]:
        """Generate statistical report from evaluations.

        Returns:
            Dictionary containing summary statistics, distributions, and percentiles
        """
        stats = self.store.get_stats()

        # Distribution of progress scores
        progress_dist = self.store.conn.execute("""
            SELECT progress_score, COUNT(*)
            FROM evaluations
            GROUP BY progress_score
            ORDER BY progress_score
        """).fetchall()

        # Task type distribution
        task_dist = self.store.conn.execute("""
            SELECT task_type, COUNT(*)
            FROM evaluations
            GROUP BY task_type
        """).fetchall()

        # Score percentiles
        percentiles = self.store.conn.execute("""
            SELECT
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY overall_score) as p25,
                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY overall_score) as p50,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY overall_score) as p75
            FROM evaluations
        """).fetchone()

        report: dict[str, Any] = {
            "summary": {
                "total_samples": stats["total_samples"],
                "total_evaluations": stats["total_evaluations"],
                "evaluation_rate": (
                    stats["total_evaluations"] / stats["total_samples"]
                    if stats["total_samples"] > 0 else 0
                ),
            },
            "averages": {
                "progress_score": round(stats["avg_progress_score"], 2),
                "tool_quality": round(stats["avg_tool_quality"], 2),
                "tool_success_rate": round(stats["avg_tool_success_rate"], 2),
                "overall_score": round(stats["avg_overall_score"], 2),
            },
            "progress_score_distribution": {
                str(row[0]): row[1] for row in progress_dist
            },
            "task_type_distribution": {
                row[0]: row[1] for row in task_dist
            },
            "overall_score_percentiles": {
                "p25": round(percentiles[0], 2) if percentiles[0] else 0,
                "p50": round(percentiles[1], 2) if percentiles[1] else 0,
                "p75": round(percentiles[2], 2) if percentiles[2] else 0,
            },
        }

        return report

    def export_report(self, output_path: Path) -> None:
        """Generate and save report to JSON file.

        Args:
            output_path: Path to output JSON report file
        """
        report = self.generate_report()
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        logger.info(f"Report exported to {output_path}")