"""DuckDB storage layer for samples and evaluations."""
import json
from pathlib import Path
from typing import Optional
import duckdb
from datetime import datetime

from claw_data_filter.models.sample import Sample
from claw_data_filter.models.evaluation import Evaluation


class DuckDBStore:
    """DuckDB-backed storage for samples and evaluations."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.conn = duckdb.connect(str(self.db_path))
        self._init_schema()

    def _init_schema(self):
        """Create tables and sequences if not exist."""
        # Samples table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS samples (
                id INTEGER PRIMARY KEY,
                raw_json JSON,
                user_query TEXT,
                assistant_response TEXT,
                num_turns INTEGER,
                num_tool_calls INTEGER,
                has_error BOOLEAN,
                imported_at TIMESTAMP
            )
        """)
        # Evaluations table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS evaluations (
                id INTEGER PRIMARY KEY,
                sample_id INTEGER REFERENCES samples(id),
                task_type TEXT,
                progress_score INTEGER,
                tool_quality_score DOUBLE,
                tool_success_rate DOUBLE,
                overall_score DOUBLE,
                reasoning TEXT,
                evaluated_at TIMESTAMP
            )
        """)
        # Sequences for auto-increment
        self.conn.execute("CREATE SEQUENCE IF NOT EXISTS sample_id_seq")
        self.conn.execute("CREATE SEQUENCE IF NOT EXISTS eval_id_seq")

    def insert_sample(self, sample: Sample) -> int:
        """Insert sample, return auto-generated id."""
        result = self.conn.execute("SELECT nextval('sample_id_seq')").fetchone()
        sample_id = result[0]

        self.conn.execute(
            """
            INSERT INTO samples (id, raw_json, user_query, assistant_response, num_turns, num_tool_calls, has_error, imported_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                sample_id,
                json.dumps(sample.raw_json),
                sample.user_query,
                sample.assistant_response,
                sample.num_turns,
                sample.num_tool_calls,
                sample.has_error,
                datetime.now(),
            ],
        )
        return sample_id

    def get_samples(self, limit: int = 100, offset: int = 0, evaluated_only: bool = False) -> list[Sample]:
        """Get samples with optional evaluation filter."""
        query = "SELECT raw_json FROM samples"
        if evaluated_only:
            query += " WHERE id IN (SELECT sample_id FROM evaluations)"
        query += f" LIMIT {limit} OFFSET {offset}"

        rows = self.conn.execute(query).fetchall()
        return [Sample.from_dict(json.loads(row[0])) for row in rows]

    def get_unevaluated_samples(self, limit: int = 100) -> list[tuple[int, Sample]]:
        """Get samples that haven't been evaluated yet. Returns (id, sample) tuples."""
        rows = self.conn.execute(
            """
            SELECT s.id, s.raw_json
            FROM samples s
            LEFT JOIN evaluations e ON s.id = e.sample_id
            WHERE e.id IS NULL
            LIMIT ?
            """,
            [limit],
        ).fetchall()
        return [(row[0], Sample.from_dict(json.loads(row[1]))) for row in rows]

    def insert_evaluation(self, evaluation: Evaluation) -> int:
        """Insert evaluation, return auto-generated id."""
        result = self.conn.execute("SELECT nextval('eval_id_seq')").fetchone()
        eval_id = result[0]

        self.conn.execute(
            """
            INSERT INTO evaluations (id, sample_id, task_type, progress_score, tool_quality_score, tool_success_rate, overall_score, reasoning, evaluated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                eval_id,
                evaluation.sample_id,
                evaluation.task_type,
                evaluation.progress_score,
                evaluation.tool_quality_score,
                evaluation.tool_success_rate,
                evaluation.overall_score,
                evaluation.reasoning,
                datetime.now(),
            ],
        )
        return eval_id

    def get_sample_count(self) -> int:
        """Get total sample count."""
        result = self.conn.execute("SELECT COUNT(*) FROM samples").fetchone()
        return result[0] if result else 0

    def get_evaluation_count(self) -> int:
        """Get total evaluation count."""
        result = self.conn.execute("SELECT COUNT(*) FROM evaluations").fetchone()
        return result[0] if result else 0

    def get_stats(self) -> dict:
        """Get statistics about samples and evaluations."""
        sample_count = self.get_sample_count()
        eval_count = self.get_evaluation_count()

        progress_stats = self.conn.execute(
            "SELECT AVG(progress_score), AVG(tool_quality_score), AVG(tool_success_rate), AVG(overall_score) FROM evaluations"
        ).fetchone()

        return {
            "total_samples": sample_count,
            "total_evaluations": eval_count,
            "avg_progress_score": progress_stats[0] if progress_stats[0] is not None else 0,
            "avg_tool_quality": progress_stats[1] if progress_stats[1] is not None else 0,
            "avg_tool_success_rate": progress_stats[2] if progress_stats[2] is not None else 0,
            "avg_overall_score": progress_stats[3] if progress_stats[3] is not None else 0,
        }

    def close(self):
        """Close connection."""
        self.conn.close()