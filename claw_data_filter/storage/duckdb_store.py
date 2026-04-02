"""DuckDB storage layer for samples and evaluations."""
import json
from pathlib import Path
from typing import Optional
import duckdb
from datetime import datetime

from claw_data_filter.models.sample import Sample
from claw_data_filter.models.evaluation import Evaluation
from claw_data_filter.models.round_judgment import RoundJudgment


class DuckDBStore:
    """DuckDB-backed storage for samples and evaluations."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.conn = duckdb.connect(str(self.db_path))
        self.init_schema()

    def init_schema(self):
        """Create tables and sequences if not exist."""
        # Samples table - includes task_type column
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS samples (
                id INTEGER PRIMARY KEY,
                raw_json JSON,
                user_query TEXT,
                assistant_response TEXT,
                num_turns INTEGER,
                num_tool_calls INTEGER,
                has_error BOOLEAN,
                imported_at TIMESTAMP,
                tool_stats JSON,
                task_type TEXT
            )
        """)

        # Migration: add columns if they don't exist
        try:
            self.conn.execute("ALTER TABLE samples ADD COLUMN tool_stats JSON")
        except:
            pass  # Column may already exist (ignore error)
        try:
            self.conn.execute("ALTER TABLE samples ADD COLUMN task_type TEXT")
        except:
            pass  # Column may already exist (ignore error)

        # Drop evaluations table completely
        self.conn.execute("DROP TABLE IF EXISTS evaluations")

        # Turn judgments table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS turn_judgments (
                id INTEGER PRIMARY KEY,
                sample_id INTEGER,
                turn_index INTEGER,
                need_tool TEXT,
                tool_correct TEXT,
                response_helpful TEXT,
                user_satisfied TEXT,
                signal_from_users JSON,
                llm_error BOOLEAN,
                created_at TIMESTAMP
            )
        """)

        # Sequences for auto-increment
        self.conn.execute("CREATE SEQUENCE IF NOT EXISTS sample_id_seq")
        self.conn.execute("CREATE SEQUENCE IF NOT EXISTS eval_id_seq")
        self.conn.execute("CREATE SEQUENCE IF NOT EXISTS turn_judgment_id_seq")

        # Create index
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_turn_judgments_sample
            ON turn_judgments(sample_id)
        """)

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
        query += " LIMIT ? OFFSET ?"

        rows = self.conn.execute(query, [limit, offset]).fetchall()
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
        """Get statistics about samples and turn judgments."""
        sample_count = self.get_sample_count()

        # Aggregate from samples.tool_stats
        stats = self.conn.execute("""
            SELECT
                COUNT(*) as total,
                AVG(CAST(json_extract(tool_stats, '$.response_helpful_rate') AS DOUBLE)) as avg_helpful,
                AVG(CAST(json_extract(tool_stats, '$.user_satisfied_rate') AS DOUBLE)) as avg_satisfied,
                SUM(CASE WHEN CAST(json_extract(tool_stats, '$.has_error') AS BOOLEAN) = true THEN 1 ELSE 0 END) as error_count
            FROM samples
            WHERE tool_stats IS NOT NULL
        """).fetchone()

        return {
            "total_samples": sample_count,
            "avg_response_helpful_rate": stats[1] or 0,
            "avg_user_satisfied_rate": stats[2] or 0,
            "error_count": stats[3] or 0,
        }

    def insert_turn_judgment(self, judgment: RoundJudgment) -> int:
        """Insert turn judgment, return auto-generated id."""
        result = self.conn.execute("SELECT nextval('turn_judgment_id_seq')").fetchone()
        j_id = result[0]

        self.conn.execute(
            """
            INSERT INTO turn_judgments (id, sample_id, turn_index, need_tool, tool_correct, response_helpful, user_satisfied, signal_from_users, llm_error, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                j_id,
                judgment.sample_id,
                judgment.turn_index,
                judgment.need_tool,
                judgment.tool_correct,
                judgment.response_helpful,
                judgment.user_satisfied,
                json.dumps(judgment.signal_from_users),
                judgment.llm_error,
                datetime.now(),
            ],
        )
        return j_id

    def get_turn_judgments(self, sample_id: int) -> list[RoundJudgment]:
        """Get all turn judgments for a sample."""
        rows = self.conn.execute(
            "SELECT sample_id, turn_index, need_tool, tool_correct, response_helpful, user_satisfied, signal_from_users, llm_error, created_at FROM turn_judgments WHERE sample_id = ? ORDER BY turn_index",
            [sample_id],
        ).fetchall()
        return [
            RoundJudgment(
                sample_id=row[0],
                turn_index=row[1],
                need_tool=row[2],
                tool_correct=row[3],
                response_helpful=row[4],
                user_satisfied=row[5],
                signal_from_users=json.loads(row[6]) if row[6] else [],
                llm_error=row[7],
                created_at=row[8],
            )
            for row in rows
        ]

    def update_sample_tool_stats(self, sample_id: int, tool_stats: dict) -> None:
        """Update tool_stats for a sample."""
        self.conn.execute(
            "UPDATE samples SET tool_stats = ? WHERE id = ?",
            [json.dumps(tool_stats), sample_id],
        )

    def get_unprocessed_samples(self, limit: int = 100) -> list[tuple[int, dict]]:
        """Get samples that haven't been processed for round judgments."""
        rows = self.conn.execute(
            """
            SELECT s.id, s.raw_json
            FROM samples s
            LEFT JOIN turn_judgments tj ON s.id = tj.sample_id
            WHERE tj.id IS NULL
            LIMIT ?
            """,
            [limit],
        ).fetchall()
        return [(row[0], json.loads(row[1])) for row in rows]

    def close(self):
        """Close connection."""
        self.conn.close()