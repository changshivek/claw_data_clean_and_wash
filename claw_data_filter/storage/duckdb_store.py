"""DuckDB storage layer for samples and evaluations."""
import json
from pathlib import Path
from typing import Any, Optional
import duckdb
from datetime import datetime

from claw_data_filter.filters.query import ComparisonOp, FilterQueryBuilder
from claw_data_filter.models.sample import Sample, generate_sample_uid
from claw_data_filter.models.round_judgment import RoundJudgment


class DuckDBStore:
    """DuckDB-backed storage for samples and evaluations."""

    def __init__(self, db_path: Path, read_only: bool = False):
        self.db_path = Path(db_path)
        self.read_only = read_only
        self.conn = duckdb.connect(str(self.db_path), read_only=read_only)
        if not self.read_only:
            self.init_schema()

    def init_schema(self):
        """Create tables and sequences if not exist."""
        # Samples table keeps only actively maintained sample-level fields.
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS samples (
                id INTEGER PRIMARY KEY,
                sample_uid TEXT,
                raw_json JSON,
                user_query TEXT,
                assistant_response TEXT,
                num_turns INTEGER,
                expected_judgment_count INTEGER,
                num_tool_calls INTEGER,
                response_helpful_rate DOUBLE,
                response_unhelpful_rate DOUBLE,
                user_satisfied_rate DOUBLE,
                user_negative_feedback_rate DOUBLE,
                imported_at TIMESTAMP,
                tool_stats JSON,
                session_merge_status TEXT,
                session_merge_keep BOOLEAN,
                session_merge_group_id TEXT,
                session_merge_group_size INTEGER,
                session_merge_representative_id INTEGER,
                session_merge_reason TEXT,
                session_merge_updated_at TIMESTAMP,
                processing_status TEXT,
                processing_updated_at TIMESTAMP
            )
        """)

        # Migration: add columns if they don't exist
        try:
            self.conn.execute("ALTER TABLE samples ADD COLUMN tool_stats JSON")
        except:
            pass  # Column may already exist (ignore error)
        try:
            self.conn.execute("ALTER TABLE samples ADD COLUMN sample_uid TEXT")
        except:
            pass  # Column may already exist (ignore error)
        try:
            self.conn.execute("ALTER TABLE samples ADD COLUMN expected_judgment_count INTEGER")
        except:
            pass  # Column may already exist (ignore error)
        try:
            self.conn.execute("ALTER TABLE samples ADD COLUMN processing_status TEXT")
        except:
            pass  # Column may already exist (ignore error)
        try:
            self.conn.execute("ALTER TABLE samples ADD COLUMN processing_updated_at TIMESTAMP")
        except:
            pass  # Column may already exist (ignore error)
        try:
            self.conn.execute("ALTER TABLE samples ADD COLUMN response_helpful_rate DOUBLE")
        except:
            pass
        try:
            self.conn.execute("ALTER TABLE samples ADD COLUMN response_unhelpful_rate DOUBLE")
        except:
            pass
        try:
            self.conn.execute("ALTER TABLE samples ADD COLUMN user_satisfied_rate DOUBLE")
        except:
            pass
        try:
            self.conn.execute("ALTER TABLE samples ADD COLUMN user_negative_feedback_rate DOUBLE")
        except:
            pass
        try:
            self.conn.execute("ALTER TABLE samples ADD COLUMN session_merge_status TEXT")
        except:
            pass
        try:
            self.conn.execute("ALTER TABLE samples ADD COLUMN session_merge_keep BOOLEAN")
        except:
            pass
        try:
            self.conn.execute("ALTER TABLE samples ADD COLUMN session_merge_group_id TEXT")
        except:
            pass
        try:
            self.conn.execute("ALTER TABLE samples ADD COLUMN session_merge_group_size INTEGER")
        except:
            pass
        try:
            self.conn.execute("ALTER TABLE samples ADD COLUMN session_merge_representative_id INTEGER")
        except:
            pass
        try:
            self.conn.execute("ALTER TABLE samples ADD COLUMN session_merge_reason TEXT")
        except:
            pass
        try:
            self.conn.execute("ALTER TABLE samples ADD COLUMN session_merge_updated_at TIMESTAMP")
        except:
            pass

        self.conn.execute(
            "UPDATE samples SET processing_status = COALESCE(processing_status, 'pending'), processing_updated_at = COALESCE(processing_updated_at, imported_at, CURRENT_TIMESTAMP)"
        )
        self.conn.execute("UPDATE samples SET sample_uid = COALESCE(sample_uid, sha256(CAST(raw_json AS VARCHAR)))")
        self.conn.execute("UPDATE samples SET num_turns = COALESCE(expected_judgment_count, num_turns)")
        self.conn.execute(
            """
            UPDATE samples
            SET response_helpful_rate = COALESCE(response_helpful_rate, CAST(json_extract(tool_stats, '$.response_helpful_rate') AS DOUBLE)),
                response_unhelpful_rate = COALESCE(response_unhelpful_rate, CAST(json_extract(tool_stats, '$.response_unhelpful_rate') AS DOUBLE)),
                user_satisfied_rate = COALESCE(user_satisfied_rate, CAST(json_extract(tool_stats, '$.user_satisfied_rate') AS DOUBLE)),
                user_negative_feedback_rate = COALESCE(user_negative_feedback_rate, CAST(json_extract(tool_stats, '$.user_negative_feedback_rate') AS DOUBLE))
            WHERE tool_stats IS NOT NULL
            """
        )

        try:
            self.conn.execute("ALTER TABLE samples DROP COLUMN task_type")
        except:
            pass
        try:
            self.conn.execute("ALTER TABLE samples DROP COLUMN has_error")
        except:
            pass

        # Drop evaluations table completely
        self.conn.execute("DROP TABLE IF EXISTS evaluations")

        # Turn judgments table (simplified: only response_helpful and user_satisfied)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS turn_judgments (
                id INTEGER PRIMARY KEY,
                sample_id INTEGER,
                turn_index INTEGER,
                response_helpful TEXT,
                user_satisfied TEXT,
                signal_from_users JSON,
                llm_error BOOLEAN,
                created_at TIMESTAMP
            )
        """)

        # Sequences for auto-increment
        self.conn.execute("CREATE SEQUENCE IF NOT EXISTS sample_id_seq")
        self.conn.execute("CREATE SEQUENCE IF NOT EXISTS turn_judgment_id_seq")

        self.conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_samples_uid ON samples(sample_uid)")

        # Create index
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_turn_judgments_sample
            ON turn_judgments(sample_id)
        """)

        self._refresh_tool_stats_from_turn_judgments()

    def _refresh_tool_stats_from_turn_judgments(self) -> None:
        """Backfill tool_stats metrics using current turn_judgments semantics.

        This keeps historical databases aligned when rate definitions change.
        Only samples with persisted turn_judgments are recalculated.
        """
        rows = self.conn.execute(
            """
            SELECT sample_id, response_helpful, user_satisfied, llm_error
            FROM turn_judgments
            ORDER BY sample_id, turn_index
            """
        ).fetchall()
        if not rows:
            return

        aggregated: dict[int, dict[str, Any]] = {}
        for sample_id, response_helpful, user_satisfied, llm_error in rows:
            stats = aggregated.setdefault(
                sample_id,
                {
                    "total_turns": 0,
                    "helpful_yes": 0,
                    "helpful_no": 0,
                    "satisfied_yes": 0,
                    "satisfied_no": 0,
                    "satisfied_neutral": 0,
                    "has_error": False,
                },
            )
            stats["total_turns"] += 1
            stats["helpful_yes"] += 1 if response_helpful == "yes" else 0
            stats["helpful_no"] += 1 if response_helpful == "no" else 0
            stats["satisfied_yes"] += 1 if user_satisfied == "yes" else 0
            stats["satisfied_no"] += 1 if user_satisfied == "no" else 0
            stats["satisfied_neutral"] += 1 if user_satisfied == "neutral" else 0
            stats["has_error"] = stats["has_error"] or bool(llm_error)

        for sample_id, counters in aggregated.items():
            response_helpful_scored_turns = counters["helpful_yes"] + counters["helpful_no"]
            user_feedback_scored_turns = (
                counters["satisfied_yes"] + counters["satisfied_no"] + counters["satisfied_neutral"]
            )
            existing = self.conn.execute("SELECT tool_stats FROM samples WHERE id = ?", [sample_id]).fetchone()
            tool_stats = json.loads(existing[0]) if existing and existing[0] else {}
            tool_stats.update(
                {
                    "response_helpful_rate": counters["helpful_yes"] / response_helpful_scored_turns if response_helpful_scored_turns else 0.0,
                    "response_unhelpful_rate": counters["helpful_no"] / response_helpful_scored_turns if response_helpful_scored_turns else 0.0,
                    "user_satisfied_rate": counters["satisfied_yes"] / user_feedback_scored_turns if user_feedback_scored_turns else 0.0,
                    "user_negative_feedback_rate": counters["satisfied_no"] / user_feedback_scored_turns if user_feedback_scored_turns else 0.0,
                    "response_helpful_scored_turns": response_helpful_scored_turns,
                    "user_feedback_scored_turns": user_feedback_scored_turns,
                    "total_turns": counters["total_turns"],
                    "has_error": counters["has_error"],
                }
            )
            self.conn.execute(
                "UPDATE samples SET tool_stats = ?, num_turns = ?, expected_judgment_count = ?, response_helpful_rate = ?, response_unhelpful_rate = ?, user_satisfied_rate = ?, user_negative_feedback_rate = ?, processing_updated_at = COALESCE(processing_updated_at, CURRENT_TIMESTAMP) WHERE id = ?",
                [
                    json.dumps(tool_stats),
                    counters["total_turns"],
                    counters["total_turns"],
                    tool_stats["response_helpful_rate"],
                    tool_stats["response_unhelpful_rate"],
                    tool_stats["user_satisfied_rate"],
                    tool_stats["user_negative_feedback_rate"],
                    sample_id,
                ],
            )

    def insert_sample(self, sample: Sample) -> int:
        """Insert sample, return auto-generated id."""
        sample_uid = sample.sample_uid or generate_sample_uid(sample.raw_json)
        existing = self.conn.execute("SELECT id FROM samples WHERE sample_uid = ?", [sample_uid]).fetchone()
        if existing:
            return existing[0]

        result = self.conn.execute("SELECT nextval('sample_id_seq')").fetchone()
        sample_id = result[0]

        self.conn.execute(
            """
            INSERT INTO samples (id, sample_uid, raw_json, user_query, assistant_response, num_turns, expected_judgment_count, num_tool_calls, imported_at, processing_status, processing_updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                sample_id,
                sample_uid,
                json.dumps(sample.raw_json),
                sample.user_query,
                sample.assistant_response,
                sample.num_turns,
                sample.expected_judgment_count,
                sample.num_tool_calls,
                datetime.now(),
                "pending",
                datetime.now(),
            ],
        )
        return sample_id

    def _build_sample_record(self, row: tuple[Any, ...]) -> dict[str, Any]:
        tool_stats = json.loads(row[12]) if row[12] else None
        return {
            "id": row[0],
            "sample_uid": row[1],
            "raw_json": json.loads(row[2]) if row[2] else {},
            "user_query": row[3],
            "assistant_response": row[4],
            "num_turns": row[5] or 0,
            "expected_judgment_count": row[6] or 0,
            "num_tool_calls": row[7] or 0,
            "has_error": (tool_stats or {}).get("has_error", False),
            "tool_stats": tool_stats,
            "processing_status": row[13] or "pending",
            "processing_updated_at": row[14],
            "helpful_rate": row[8] if row[8] is not None else (tool_stats or {}).get("response_helpful_rate", 0),
            "unhelpful_rate": row[9] if row[9] is not None else (tool_stats or {}).get("response_unhelpful_rate", 0),
            "satisfied_rate": row[10] if row[10] is not None else (tool_stats or {}).get("user_satisfied_rate", 0),
            "negative_feedback_rate": row[11] if row[11] is not None else (tool_stats or {}).get("user_negative_feedback_rate", 0),
        }

    def get_samples(self, limit: int = 100, offset: int = 0) -> list[Sample]:
        """Get samples."""
        rows = self.conn.execute(
            "SELECT raw_json FROM samples LIMIT ? OFFSET ?",
            [limit, offset]
        ).fetchall()
        return [Sample.from_dict(json.loads(row[0])) for row in rows]

    def get_sample_count(self) -> int:
        """Get total sample count."""
        result = self.conn.execute("SELECT COUNT(*) FROM samples").fetchone()
        return result[0] if result else 0

    def get_stats(self) -> dict:
        """Get statistics about samples and turn judgments."""
        sample_count = self.get_sample_count()

        # Aggregate from samples.tool_stats
        stats = self.conn.execute("""
            SELECT
                COUNT(*) as total,
                AVG(response_helpful_rate) as avg_helpful,
                AVG(response_unhelpful_rate) as avg_unhelpful,
                AVG(user_satisfied_rate) as avg_satisfied,
                AVG(user_negative_feedback_rate) as avg_negative_feedback,
                SUM(CASE WHEN CAST(json_extract(tool_stats, '$.has_error') AS BOOLEAN) = true THEN 1 ELSE 0 END) as error_count
            FROM samples
            WHERE tool_stats IS NOT NULL
        """).fetchone()

        return {
            "total_samples": sample_count,
            "avg_response_helpful_rate": stats[1] or 0,
            "avg_response_unhelpful_rate": stats[2] or 0,
            "avg_user_satisfied_rate": stats[3] or 0,
            "avg_user_negative_feedback_rate": stats[4] or 0,
            "error_count": stats[5] or 0,
        }

    def get_processed_count(self) -> int:
        """Count samples that finished round feedback processing."""
        result = self.conn.execute(
            "SELECT COUNT(*) FROM samples WHERE COALESCE(processing_status, 'pending') = 'completed'"
        ).fetchone()
        return result[0] if result else 0

    def insert_turn_judgment(self, judgment: RoundJudgment) -> int:
        """Insert turn judgment, return auto-generated id."""
        result = self.conn.execute("SELECT nextval('turn_judgment_id_seq')").fetchone()
        j_id = result[0]

        self.conn.execute(
            """
            INSERT INTO turn_judgments (id, sample_id, turn_index, response_helpful, user_satisfied, signal_from_users, llm_error, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                j_id,
                judgment.sample_id,
                judgment.turn_index,
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
            "SELECT sample_id, turn_index, response_helpful, user_satisfied, signal_from_users, llm_error, created_at FROM turn_judgments WHERE sample_id = ? ORDER BY turn_index",
            [sample_id],
        ).fetchall()
        return [
            RoundJudgment(
                sample_id=row[0],
                turn_index=row[1],
                response_helpful=row[2],
                user_satisfied=row[3],
                signal_from_users=json.loads(row[4]) if row[4] else [],
                llm_error=row[5],
                created_at=row[6],
            )
            for row in rows
        ]

    def update_sample_tool_stats(self, sample_id: int, tool_stats: dict) -> None:
        """Update tool_stats for a sample."""
        self.conn.execute(
            "UPDATE samples SET tool_stats = ?, response_helpful_rate = ?, response_unhelpful_rate = ?, user_satisfied_rate = ?, user_negative_feedback_rate = ?, processing_updated_at = ? WHERE id = ?",
            [
                json.dumps(tool_stats),
                tool_stats.get("response_helpful_rate"),
                tool_stats.get("response_unhelpful_rate"),
                tool_stats.get("user_satisfied_rate"),
                tool_stats.get("user_negative_feedback_rate"),
                datetime.now(),
                sample_id,
            ],
        )

    def mark_sample_processing_failed(self, sample_id: int, error_reason: str | None = None) -> None:
        """Mark sample as failed and persist error reason in tool_stats."""
        existing = self.conn.execute("SELECT tool_stats FROM samples WHERE id = ?", [sample_id]).fetchone()
        tool_stats = json.loads(existing[0]) if existing and existing[0] else {}
        tool_stats["has_error"] = True
        if error_reason:
            tool_stats["error_reason"] = error_reason
        self.conn.execute(
            "UPDATE samples SET tool_stats = ?, processing_status = 'failed', processing_updated_at = ? WHERE id = ?",
            [json.dumps(tool_stats), datetime.now(), sample_id],
        )

    def replace_round_feedback_results(
        self,
        sample_id: int,
        expected_judgment_count: int,
        judgments: list[RoundJudgment],
        tool_stats: dict,
    ) -> None:
        """Atomically replace a sample's round feedback results."""
        self.conn.execute("BEGIN TRANSACTION")
        try:
            self.conn.execute("DELETE FROM turn_judgments WHERE sample_id = ?", [sample_id])
            self.conn.execute(
                "UPDATE samples SET tool_stats = ?, num_turns = ?, expected_judgment_count = ?, response_helpful_rate = ?, response_unhelpful_rate = ?, user_satisfied_rate = ?, user_negative_feedback_rate = ?, processing_status = 'completed', processing_updated_at = ? WHERE id = ?",
                [
                    json.dumps(tool_stats),
                    expected_judgment_count,
                    expected_judgment_count,
                    tool_stats.get("response_helpful_rate"),
                    tool_stats.get("response_unhelpful_rate"),
                    tool_stats.get("user_satisfied_rate"),
                    tool_stats.get("user_negative_feedback_rate"),
                    datetime.now(),
                    sample_id,
                ],
            )
            for judgment in judgments:
                self.insert_turn_judgment(judgment)
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    def claim_unprocessed_samples(self, limit: int = 100) -> list[tuple[int, dict]]:
        """Claim pending or failed samples for round feedback processing."""
        self.conn.execute("BEGIN TRANSACTION")
        try:
            rows = self.conn.execute(
                """
                SELECT id, raw_json
                FROM samples
                WHERE COALESCE(processing_status, 'pending') IN ('pending', 'failed')
                                    AND COALESCE(session_merge_keep, TRUE) = TRUE
                ORDER BY id
                LIMIT ?
                """,
                [limit],
            ).fetchall()
            sample_ids = [row[0] for row in rows]
            if sample_ids:
                placeholders = ", ".join(["?"] * len(sample_ids))
                params = [datetime.now(), *sample_ids]
                self.conn.execute(
                    f"UPDATE samples SET processing_status = 'processing', processing_updated_at = ? WHERE id IN ({placeholders})",
                    params,
                )
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

        return [(row[0], json.loads(row[1])) for row in rows]

    def get_unprocessed_samples(self, limit: int = 100) -> list[tuple[int, dict]]:
        """Get samples that haven't been processed for round judgments."""
        rows = self.conn.execute(
            """
            SELECT s.id, s.raw_json
            FROM samples s
            WHERE COALESCE(s.processing_status, 'pending') IN ('pending', 'failed')
                            AND COALESCE(s.session_merge_keep, TRUE) = TRUE
            ORDER BY s.id
            LIMIT ?
            """,
            [limit],
        ).fetchall()
        return [(row[0], json.loads(row[1])) for row in rows]

    def get_sample_by_id(self, sample_id: int) -> dict[str, Any] | None:
        """Get a sample record with parsed JSON fields."""
        row = self.conn.execute(
            """
             SELECT id, sample_uid, raw_json, user_query, assistant_response, num_turns, expected_judgment_count,
                 num_tool_calls, response_helpful_rate, response_unhelpful_rate, user_satisfied_rate,
                 user_negative_feedback_rate, tool_stats, processing_status, processing_updated_at
            FROM samples
            WHERE id = ?
            """,
            [sample_id],
        ).fetchone()
        return self._build_sample_record(row) if row else None

    def filter_samples(
        self,
        helpful_rate_op: str = ">=",
        helpful_rate_val: float | None = None,
        satisfied_rate_op: str = ">=",
        satisfied_rate_val: float | None = None,
        negative_feedback_rate_op: str = ">=",
        negative_feedback_rate_val: float | None = None,
        has_error: bool | None = None,
        num_turns_min: int | None = None,
        num_turns_max: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        """Filter samples with parameterized query building."""
        builder = FilterQueryBuilder()

        if helpful_rate_val is not None:
            builder.add_condition("response_helpful_rate", ComparisonOp(helpful_rate_op), helpful_rate_val)
        if satisfied_rate_val is not None:
            builder.add_condition("user_satisfied_rate", ComparisonOp(satisfied_rate_op), satisfied_rate_val)
        if negative_feedback_rate_val is not None:
            builder.add_condition("user_negative_feedback_rate", ComparisonOp(negative_feedback_rate_op), negative_feedback_rate_val)
        if has_error is not None:
            builder.add_condition("has_error", ComparisonOp.EQ, has_error)
        if num_turns_min is not None:
            builder.add_condition("num_turns", ComparisonOp.GTE, num_turns_min)
        if num_turns_max is not None:
            builder.add_condition("num_turns", ComparisonOp.LTE, num_turns_max)

        where_clause, params = builder.build_parameterized_where_clause("s")
        extra_clauses: list[str] = []
        if date_from:
            extra_clauses.append("s.imported_at >= ?")
            params.append(date_from)
        if date_to:
            extra_clauses.append("s.imported_at <= ?")
            params.append(date_to)

        combined_where = where_clause
        if extra_clauses:
            combined_where = " AND ".join([where_clause, *extra_clauses]) if where_clause != "1=1" else " AND ".join(extra_clauses)

        count_query = f"SELECT COUNT(*) FROM samples s WHERE {combined_where}"
        total_row = self.conn.execute(count_query, params).fetchone()
        total = total_row[0] if total_row else 0

        query = f"""
             SELECT id, sample_uid, raw_json, user_query, assistant_response, num_turns, expected_judgment_count,
                 num_tool_calls, response_helpful_rate, response_unhelpful_rate, user_satisfied_rate,
                 user_negative_feedback_rate, tool_stats, processing_status, processing_updated_at
            FROM samples s
            WHERE {combined_where}
            ORDER BY id
            LIMIT ? OFFSET ?
        """
        rows = self.conn.execute(query, [*params, limit, offset]).fetchall()
        return [self._build_sample_record(row) for row in rows], total

    def get_table_list(self) -> list[str]:
        """List available tables."""
        rows = self.conn.execute("SHOW TABLES").fetchall()
        return [row[0] for row in rows]

    def get_table_schema(self, table_name: str) -> list[dict[str, Any]]:
        """Get schema for a table."""
        rows = self.conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        return [{"name": row[1], "type": row[2]} for row in rows]

    def close(self):
        """Close connection."""
        self.conn.close()