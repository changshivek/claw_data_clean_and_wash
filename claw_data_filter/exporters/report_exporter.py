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
    """Generate and export dual-level statistical reports from samples."""

    def __init__(self, store: DuckDBStore):
        self.store = store

    def _collect_judgment_totals(self) -> dict[str, int]:
        rows = self.store.conn.execute(
            """
            SELECT tool_stats
            FROM samples
            WHERE tool_stats IS NOT NULL
            """
        ).fetchall()

        totals = {
            "processed_samples": 0,
            "assistant_response_count": 0,
            "user_episode_count": 0,
            "response_progress_scored_steps": 0,
            "user_feedback_scored_episodes": 0,
        }
        for (tool_stats_raw,) in rows:
            tool_stats = json.loads(tool_stats_raw) if isinstance(tool_stats_raw, str) else (tool_stats_raw or {})
            if not tool_stats:
                continue
            totals["processed_samples"] += 1
            totals["assistant_response_count"] += int(tool_stats.get("assistant_response_count", 0) or 0)
            totals["user_episode_count"] += int(tool_stats.get("user_episode_count", 0) or 0)
            totals["response_progress_scored_steps"] += int(tool_stats.get("response_progress_scored_steps", 0) or 0)
            totals["user_feedback_scored_episodes"] += int(tool_stats.get("user_feedback_scored_episodes", 0) or 0)
        return totals

    def _build_report_payload(self) -> dict[str, Any]:
        stats = self.store.get_stats()
        judgment_totals = self._collect_judgment_totals()

        return {
            "summary": {
                "total_samples": stats["total_samples"],
                "processed_samples": judgment_totals["processed_samples"],
                "avg_response_progress_rate": round(stats["avg_response_progress_rate"], 2),
                "avg_response_regress_rate": round(stats["avg_response_regress_rate"], 2),
                "avg_user_satisfied_rate": round(stats["avg_user_satisfied_rate"], 2),
                "avg_user_negative_feedback_rate": round(stats["avg_user_negative_feedback_rate"], 2),
                "error_count": stats["error_count"],
            },
            "judgment_totals": {
                "assistant_response_count": judgment_totals["assistant_response_count"],
                "user_episode_count": judgment_totals["user_episode_count"],
                "response_progress_scored_steps": judgment_totals["response_progress_scored_steps"],
                "user_feedback_scored_episodes": judgment_totals["user_feedback_scored_episodes"],
            },
            "semantics": {
                "num_turns": "samples.num_turns currently tracks user episode count",
                "response_progress_rate": "Computed from assistant response judgments with yes/no as scored denominator",
                "user_satisfied_rate": "Computed from user episode judgments with yes/no/neutral as scored denominator",
            },
        }

    def generate_report(self) -> dict[str, Any]:
        """Generate a dual-level statistical report.

        Returns:
            Dictionary containing summary statistics
        """
        return self._build_report_payload()

    def export_report(self, output_path: Path) -> dict:
        """Export statistical report."""
        _validate_output_path(output_path)
        report = self._build_report_payload()
        report["generated_at"] = datetime.now().isoformat()

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

        logger.info(f"Report saved to {output_path}")
        return report