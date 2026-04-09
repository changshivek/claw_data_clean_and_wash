"""DuckDB storage layer for samples and evaluations."""
import json
from pathlib import Path
from typing import Any, Optional
import duckdb
from datetime import datetime

from claw_data_filter.filters.query import ComparisonOp, FilterQueryBuilder
from claw_data_filter.models.sample import Sample, generate_sample_uid
from claw_data_filter.models.round_judgment import (
    AssistantResponseJudgment,
    UserEpisodeJudgment,
)


class DuckDBStore:
    """DuckDB-backed storage for samples and evaluations."""

    def __init__(self, db_path: Path, read_only: bool = False):
        self.db_path = Path(db_path)
        self.read_only = read_only
        self.conn = duckdb.connect(str(self.db_path), read_only=read_only)
        if not self.read_only:
            self.init_schema()

    def _create_samples_table(self, table_name: str) -> None:
        self.conn.execute(
            f"""
            CREATE TABLE {table_name} (
                sample_uid TEXT PRIMARY KEY,
                id INTEGER UNIQUE,
                raw_json JSON,
                user_query TEXT,
                assistant_response TEXT,
                empty_response BOOLEAN,
                num_turns INTEGER,
                expected_judgment_count INTEGER,
                expected_response_judgment_count INTEGER,
                expected_episode_judgment_count INTEGER,
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
            """
        )

    def _ensure_samples_table(self) -> None:
        existing_tables = {row[0] for row in self.conn.execute("SHOW TABLES").fetchall()}
        if "samples" not in existing_tables:
            self._create_samples_table("samples")
            return

        columns = self.conn.execute("PRAGMA table_info('samples')").fetchall()
        column_names = {row[1] for row in columns}
        primary_keys = [row[1] for row in columns if row[5]]

        needs_rebuild = primary_keys != ["sample_uid"] or "id" not in column_names
        if not needs_rebuild:
            return

        def existing(name: str, fallback_sql: str) -> str:
            return name if name in column_names else fallback_sql

        self._create_samples_table("samples_v2")
        self.conn.execute(
            f"""
            INSERT INTO samples_v2 (
                sample_uid, id, raw_json, user_query, assistant_response, empty_response,
                num_turns, expected_judgment_count, expected_response_judgment_count,
                expected_episode_judgment_count, num_tool_calls, response_helpful_rate,
                response_unhelpful_rate, user_satisfied_rate, user_negative_feedback_rate,
                imported_at, tool_stats, session_merge_status, session_merge_keep,
                session_merge_group_id, session_merge_group_size, session_merge_representative_id,
                session_merge_reason, session_merge_updated_at, processing_status, processing_updated_at
            )
            SELECT
                COALESCE(sample_uid, sha256(CAST(raw_json AS VARCHAR))),
                id,
                raw_json,
                user_query,
                assistant_response,
                COALESCE({existing('empty_response', 'FALSE')}, FALSE),
                COALESCE({existing('num_turns', '0')}, 0),
                COALESCE({existing('expected_judgment_count', 'num_turns')}, {existing('num_turns', '0')}, 0),
                {existing('expected_response_judgment_count', 'NULL')},
                {existing('expected_episode_judgment_count', 'NULL')},
                COALESCE({existing('num_tool_calls', '0')}, 0),
                {existing('response_helpful_rate', 'NULL')},
                {existing('response_unhelpful_rate', 'NULL')},
                {existing('user_satisfied_rate', 'NULL')},
                {existing('user_negative_feedback_rate', 'NULL')},
                {existing('imported_at', 'CURRENT_TIMESTAMP')},
                {existing('tool_stats', 'NULL')},
                {existing('session_merge_status', 'NULL')},
                {existing('session_merge_keep', 'NULL')},
                {existing('session_merge_group_id', 'NULL')},
                {existing('session_merge_group_size', 'NULL')},
                {existing('session_merge_representative_id', 'NULL')},
                {existing('session_merge_reason', 'NULL')},
                {existing('session_merge_updated_at', 'NULL')},
                {existing('processing_status', "'pending'")},
                {existing('processing_updated_at', 'CURRENT_TIMESTAMP')}
            FROM samples
            """
        )
        self.conn.execute("DROP TABLE samples")
        self.conn.execute("ALTER TABLE samples_v2 RENAME TO samples")

    def init_schema(self):
        """Create tables and sequences if not exist."""
        self._ensure_samples_table()

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
            self.conn.execute("ALTER TABLE samples ADD COLUMN empty_response BOOLEAN")
        except:
            pass
        try:
            self.conn.execute("ALTER TABLE samples ADD COLUMN expected_judgment_count INTEGER")
        except:
            pass  # Column may already exist (ignore error)
        try:
            self.conn.execute("ALTER TABLE samples ADD COLUMN expected_response_judgment_count INTEGER")
        except:
            pass
        try:
            self.conn.execute("ALTER TABLE samples ADD COLUMN expected_episode_judgment_count INTEGER")
        except:
            pass
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
        self.conn.execute("UPDATE samples SET empty_response = COALESCE(empty_response, FALSE)")
        self.conn.execute(
            "UPDATE samples SET num_turns = COALESCE(expected_episode_judgment_count, expected_judgment_count, num_turns, 0)"
        )
        self.conn.execute(
            "UPDATE samples SET expected_judgment_count = COALESCE(expected_judgment_count, num_turns, 0), expected_response_judgment_count = COALESCE(expected_response_judgment_count, 0), expected_episode_judgment_count = COALESCE(expected_episode_judgment_count, num_turns, 0)"
        )
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

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS assistant_response_judgments (
                judgment_uid TEXT PRIMARY KEY,
                sample_uid TEXT,
                response_index INTEGER,
                episode_index INTEGER,
                assistant_message_index INTEGER,
                feedback_kind TEXT,
                feedback_message_start_index INTEGER,
                feedback_message_end_index INTEGER,
                feedback_payload JSON,
                response_helpful TEXT,
                llm_error BOOLEAN,
                created_at TIMESTAMP
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_episode_judgments (
                judgment_uid TEXT PRIMARY KEY,
                sample_uid TEXT,
                episode_index INTEGER,
                start_user_message_index INTEGER,
                end_before_user_message_index INTEGER,
                signal_from_users JSON,
                user_satisfied TEXT,
                llm_error BOOLEAN,
                created_at TIMESTAMP
            )
            """
        )

        # Sequences for auto-increment
        self.conn.execute("CREATE SEQUENCE IF NOT EXISTS sample_id_seq")
        self._sync_sequence_to_table_max("sample_id_seq", "samples", "id")

        self.conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_samples_uid ON samples(sample_uid)")
        self.conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_samples_id ON samples(id)")

        self.conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_assistant_response_sample_response ON assistant_response_judgments(sample_uid, response_index)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_assistant_response_sample ON assistant_response_judgments(sample_uid)"
        )
        self.conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_user_episode_sample_episode ON user_episode_judgments(sample_uid, episode_index)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_user_episode_sample ON user_episode_judgments(sample_uid)"
        )

        if self._tool_stats_refresh_needed():
            self._refresh_tool_stats_from_judgments()

    def _sync_sequence_to_table_max(self, sequence_name: str, table_name: str, id_column: str) -> None:
        """Recreate a sequence so its next value is greater than current max(id)."""
        max_id = self.conn.execute(
            f"SELECT COALESCE(MAX({id_column}), 0) FROM {table_name}"
        ).fetchone()[0]
        next_value = int(max_id) + 1
        self.conn.execute(f"DROP SEQUENCE IF EXISTS {sequence_name}")
        self.conn.execute(f"CREATE SEQUENCE {sequence_name} START {next_value}")

    def _tool_stats_refresh_needed(self) -> bool:
        """Refresh tool_stats only when dual-level judgment rows exist and samples are missing aggregates."""
        has_judgments = self.conn.execute("SELECT 1 FROM assistant_response_judgments LIMIT 1").fetchone() or self.conn.execute(
            "SELECT 1 FROM user_episode_judgments LIMIT 1"
        ).fetchone()
        if not has_judgments:
            return False

        needs_refresh = self.conn.execute(
            """
            SELECT 1
            FROM samples
            WHERE tool_stats IS NULL
               OR response_helpful_rate IS NULL
               OR response_unhelpful_rate IS NULL
               OR user_satisfied_rate IS NULL
               OR user_negative_feedback_rate IS NULL
               OR expected_judgment_count IS NULL
            LIMIT 1
            """
        ).fetchone()
        return needs_refresh is not None

    def _refresh_tool_stats_from_judgments(self) -> None:
        """Backfill aggregates from the dual-level judgment tables."""
        sample_uids = {
            row[0]
            for row in self.conn.execute("SELECT sample_uid FROM assistant_response_judgments").fetchall()
        }
        sample_uids.update(
            row[0] for row in self.conn.execute("SELECT sample_uid FROM user_episode_judgments").fetchall()
        )
        for sample_uid in sample_uids:
            assistant_rows = self.get_assistant_response_judgments(sample_uid)
            episode_rows = self.get_user_episode_judgments(sample_uid)
            tool_stats = self._build_tool_stats(assistant_rows, episode_rows)
            self.conn.execute(
                """
                UPDATE samples
                SET tool_stats = ?,
                    num_turns = ?,
                    expected_judgment_count = ?,
                    expected_response_judgment_count = ?,
                    expected_episode_judgment_count = ?,
                    response_helpful_rate = ?,
                    response_unhelpful_rate = ?,
                    user_satisfied_rate = ?,
                    user_negative_feedback_rate = ?,
                    processing_updated_at = COALESCE(processing_updated_at, CURRENT_TIMESTAMP)
                WHERE sample_uid = ?
                """,
                [
                    json.dumps(tool_stats),
                    tool_stats["user_episode_count"],
                    tool_stats["assistant_response_count"] + tool_stats["user_episode_count"],
                    tool_stats["assistant_response_count"],
                    tool_stats["user_episode_count"],
                    tool_stats["response_helpful_rate"],
                    tool_stats["response_unhelpful_rate"],
                    tool_stats["user_satisfied_rate"],
                    tool_stats["user_negative_feedback_rate"],
                    sample_uid,
                ],
            )

    def insert_sample(self, sample: Sample) -> int:
        """Insert sample, return auto-generated id."""
        sample_uid = sample.sample_uid or generate_sample_uid(sample.raw_json)
        existing = self.conn.execute("SELECT id FROM samples WHERE sample_uid = ?", [sample_uid]).fetchone()
        if existing:
            return existing[0]

        insert_params = [
            sample_uid,
            json.dumps(sample.raw_json),
            sample.user_query,
            sample.assistant_response,
            sample.empty_response,
            sample.num_turns,
            sample.expected_judgment_count,
            sample.expected_response_judgment_count,
            sample.expected_episode_judgment_count,
            sample.num_tool_calls,
            datetime.now(),
            "pending",
            datetime.now(),
        ]

        for attempt in range(2):
            result = self.conn.execute("SELECT nextval('sample_id_seq')").fetchone()
            sample_id = result[0]
            try:
                self.conn.execute(
                    """
                    INSERT INTO samples (
                        id, sample_uid, raw_json, user_query, assistant_response, empty_response,
                        num_turns, expected_judgment_count, expected_response_judgment_count,
                        expected_episode_judgment_count, num_tool_calls, imported_at,
                        processing_status, processing_updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [sample_id, *insert_params],
                )
                return sample_id
            except Exception as exc:
                existing = self.conn.execute("SELECT id FROM samples WHERE sample_uid = ?", [sample_uid]).fetchone()
                if existing:
                    return existing[0]
                if attempt == 0 and "duplicate key" in str(exc).lower():
                    self._sync_sequence_to_table_max("sample_id_seq", "samples", "id")
                    continue
                raise

        raise RuntimeError("Failed to insert sample after retrying sequence synchronization")

    def _build_sample_record(self, row: tuple[Any, ...]) -> dict[str, Any]:
        tool_stats = json.loads(row[15]) if row[15] else None
        return {
            "id": row[0],
            "sample_uid": row[1],
            "raw_json": json.loads(row[2]) if row[2] else {},
            "user_query": row[3],
            "assistant_response": row[4],
            "empty_response": bool(row[5]),
            "num_turns": row[6] or 0,
            "expected_judgment_count": row[7] or 0,
            "expected_response_judgment_count": row[8] or 0,
            "expected_episode_judgment_count": row[9] or 0,
            "num_tool_calls": row[10] or 0,
            "has_error": (tool_stats or {}).get("has_error", False),
            "tool_stats": tool_stats,
            "session_merge_status": row[16],
            "session_merge_keep": row[17],
            "session_merge_group_id": row[18],
            "session_merge_group_size": row[19],
            "session_merge_representative_id": row[20],
            "session_merge_reason": row[21],
            "session_merge_updated_at": row[22],
            "processing_status": row[23] or "pending",
            "processing_updated_at": row[24],
            "helpful_rate": row[11] if row[11] is not None else (tool_stats or {}).get("response_helpful_rate", 0),
            "unhelpful_rate": row[12] if row[12] is not None else (tool_stats or {}).get("response_unhelpful_rate", 0),
            "satisfied_rate": row[13] if row[13] is not None else (tool_stats or {}).get("user_satisfied_rate", 0),
            "negative_feedback_rate": row[14] if row[14] is not None else (tool_stats or {}).get("user_negative_feedback_rate", 0),
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

    def _sample_lookup_column(self, sample_ref: int | str) -> str:
        return "sample_uid" if isinstance(sample_ref, str) else "id"

    def _build_tool_stats(
        self,
        response_judgments: list[AssistantResponseJudgment],
        episode_judgments: list[UserEpisodeJudgment],
    ) -> dict[str, Any]:
        helpful_yes = sum(1 for row in response_judgments if row.response_helpful == "yes")
        helpful_no = sum(1 for row in response_judgments if row.response_helpful == "no")
        helpful_uncertain = sum(1 for row in response_judgments if row.response_helpful == "uncertain")
        satisfied_yes = sum(1 for row in episode_judgments if row.user_satisfied == "yes")
        satisfied_no = sum(1 for row in episode_judgments if row.user_satisfied == "no")
        satisfied_neutral = sum(1 for row in episode_judgments if row.user_satisfied == "neutral")
        satisfied_uncertain = sum(1 for row in episode_judgments if row.user_satisfied == "uncertain")
        helpful_scored = helpful_yes + helpful_no
        satisfied_scored = satisfied_yes + satisfied_no + satisfied_neutral

        return {
            "response_helpful": {
                "yes": helpful_yes,
                "no": helpful_no,
                "uncertain": helpful_uncertain,
                "rate": helpful_yes / helpful_scored if helpful_scored else 0.0,
            },
            "user_satisfied": {
                "yes": satisfied_yes,
                "no": satisfied_no,
                "neutral": satisfied_neutral,
                "uncertain": satisfied_uncertain,
                "rate": satisfied_yes / satisfied_scored if satisfied_scored else 0.0,
            },
            "response_helpful_rate": helpful_yes / helpful_scored if helpful_scored else 0.0,
            "response_unhelpful_rate": helpful_no / helpful_scored if helpful_scored else 0.0,
            "user_satisfied_rate": satisfied_yes / satisfied_scored if satisfied_scored else 0.0,
            "user_negative_feedback_rate": satisfied_no / satisfied_scored if satisfied_scored else 0.0,
            "response_helpful_scored_steps": helpful_scored,
            "user_feedback_scored_episodes": satisfied_scored,
            "assistant_response_count": len(response_judgments),
            "user_episode_count": len(episode_judgments),
            "has_error": any(row.llm_error for row in response_judgments) or any(row.llm_error for row in episode_judgments),
        }

    def insert_assistant_response_judgment(self, judgment: AssistantResponseJudgment) -> str:
        self.conn.execute("DELETE FROM assistant_response_judgments WHERE judgment_uid = ?", [judgment.judgment_uid])
        self.conn.execute(
            """
            INSERT INTO assistant_response_judgments (
                judgment_uid, sample_uid, response_index, episode_index,
                assistant_message_index, feedback_kind, feedback_message_start_index,
                feedback_message_end_index, feedback_payload, response_helpful,
                llm_error, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                judgment.judgment_uid,
                judgment.sample_uid,
                judgment.response_index,
                judgment.episode_index,
                judgment.assistant_message_index,
                judgment.feedback_kind.value,
                judgment.feedback_message_start_index,
                judgment.feedback_message_end_index,
                json.dumps(judgment.feedback_payload),
                judgment.response_helpful,
                judgment.llm_error,
                judgment.created_at,
            ],
        )
        return judgment.judgment_uid

    def insert_user_episode_judgment(self, judgment: UserEpisodeJudgment) -> str:
        self.conn.execute("DELETE FROM user_episode_judgments WHERE judgment_uid = ?", [judgment.judgment_uid])
        self.conn.execute(
            """
            INSERT INTO user_episode_judgments (
                judgment_uid, sample_uid, episode_index, start_user_message_index,
                end_before_user_message_index, signal_from_users, user_satisfied,
                llm_error, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                judgment.judgment_uid,
                judgment.sample_uid,
                judgment.episode_index,
                judgment.start_user_message_index,
                judgment.end_before_user_message_index,
                json.dumps(judgment.signal_from_users),
                judgment.user_satisfied,
                judgment.llm_error,
                judgment.created_at,
            ],
        )
        return judgment.judgment_uid

    def get_assistant_response_judgments(self, sample_uid: str) -> list[AssistantResponseJudgment]:
        rows = self.conn.execute(
            """
            SELECT sample_uid, response_index, episode_index, assistant_message_index,
                   feedback_kind, feedback_message_start_index, feedback_message_end_index,
                   feedback_payload, response_helpful, llm_error, created_at, judgment_uid
            FROM assistant_response_judgments
            WHERE sample_uid = ?
            ORDER BY response_index
            """,
            [sample_uid],
        ).fetchall()
        return [
            AssistantResponseJudgment(
                sample_uid=row[0],
                response_index=row[1],
                episode_index=row[2],
                assistant_message_index=row[3],
                feedback_kind=row[4],
                feedback_message_start_index=row[5],
                feedback_message_end_index=row[6],
                feedback_payload=json.loads(row[7]) if row[7] else [],
                response_helpful=row[8],
                llm_error=row[9],
                created_at=row[10],
                judgment_uid=row[11],
            )
            for row in rows
        ]

    def get_user_episode_judgments(self, sample_uid: str) -> list[UserEpisodeJudgment]:
        rows = self.conn.execute(
            """
            SELECT sample_uid, episode_index, start_user_message_index,
                   end_before_user_message_index, signal_from_users, user_satisfied,
                   llm_error, created_at, judgment_uid
            FROM user_episode_judgments
            WHERE sample_uid = ?
            ORDER BY episode_index
            """,
            [sample_uid],
        ).fetchall()
        return [
            UserEpisodeJudgment(
                sample_uid=row[0],
                episode_index=row[1],
                start_user_message_index=row[2],
                end_before_user_message_index=row[3],
                signal_from_users=json.loads(row[4]) if row[4] else [],
                user_satisfied=row[5],
                llm_error=row[6],
                created_at=row[7],
                judgment_uid=row[8],
            )
            for row in rows
        ]

    def update_sample_tool_stats(self, sample_ref: int | str, tool_stats: dict) -> None:
        """Update tool_stats for a sample."""
        sample_column = self._sample_lookup_column(sample_ref)
        self.conn.execute(
            f"UPDATE samples SET tool_stats = ?, response_helpful_rate = ?, response_unhelpful_rate = ?, user_satisfied_rate = ?, user_negative_feedback_rate = ?, processing_updated_at = ? WHERE {sample_column} = ?",
            [
                json.dumps(tool_stats),
                tool_stats.get("response_helpful_rate"),
                tool_stats.get("response_unhelpful_rate"),
                tool_stats.get("user_satisfied_rate"),
                tool_stats.get("user_negative_feedback_rate"),
                datetime.now(),
                sample_ref,
            ],
        )

    def mark_sample_processing_failed(self, sample_ref: int | str, error_reason: str | None = None) -> None:
        """Mark sample as failed and persist error reason in tool_stats."""
        sample_column = self._sample_lookup_column(sample_ref)
        existing = self.conn.execute(f"SELECT tool_stats FROM samples WHERE {sample_column} = ?", [sample_ref]).fetchone()
        tool_stats = json.loads(existing[0]) if existing and existing[0] else {}
        tool_stats["has_error"] = True
        if error_reason:
            tool_stats["error_reason"] = error_reason
        self.conn.execute(
            f"UPDATE samples SET tool_stats = ?, processing_status = 'failed', processing_updated_at = ? WHERE {sample_column} = ?",
            [json.dumps(tool_stats), datetime.now(), sample_ref],
        )

    def replace_round_feedback_results(
        self,
        sample_uid: str | int,
        expected_response_judgment_count: int,
        expected_episode_judgment_count: int,
        response_judgments: list[AssistantResponseJudgment],
        episode_judgments: list[UserEpisodeJudgment],
        tool_stats: dict,
    ) -> None:
        """Atomically replace a sample's round feedback results."""
        if isinstance(sample_uid, int):
            sample = self.get_sample_by_id(sample_uid)
            if not sample or not sample.get("sample_uid"):
                raise ValueError(f"Unknown sample id: {sample_uid}")
            resolved_sample_uid = sample["sample_uid"]
        else:
            resolved_sample_uid = sample_uid
        self.conn.execute("BEGIN TRANSACTION")
        try:
            self.conn.execute("DELETE FROM assistant_response_judgments WHERE sample_uid = ?", [resolved_sample_uid])
            self.conn.execute("DELETE FROM user_episode_judgments WHERE sample_uid = ?", [resolved_sample_uid])
            self.conn.execute(
                """
                UPDATE samples
                SET tool_stats = ?,
                    num_turns = ?,
                    expected_judgment_count = ?,
                    expected_response_judgment_count = ?,
                    expected_episode_judgment_count = ?,
                    response_helpful_rate = ?,
                    response_unhelpful_rate = ?,
                    user_satisfied_rate = ?,
                    user_negative_feedback_rate = ?,
                    processing_status = 'completed',
                    processing_updated_at = ?
                WHERE sample_uid = ?
                """,
                [
                    json.dumps(tool_stats),
                    expected_episode_judgment_count,
                    expected_response_judgment_count + expected_episode_judgment_count,
                    expected_response_judgment_count,
                    expected_episode_judgment_count,
                    tool_stats.get("response_helpful_rate"),
                    tool_stats.get("response_unhelpful_rate"),
                    tool_stats.get("user_satisfied_rate"),
                    tool_stats.get("user_negative_feedback_rate"),
                    datetime.now(),
                    resolved_sample_uid,
                ],
            )
            for judgment in response_judgments:
                self.insert_assistant_response_judgment(judgment)
            for judgment in episode_judgments:
                self.insert_user_episode_judgment(judgment)
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    def claim_unprocessed_samples(self, limit: int = 100) -> list[tuple[str, dict]]:
        """Claim pending or failed samples for round feedback processing."""
        self.conn.execute("BEGIN TRANSACTION")
        try:
            rows = self.conn.execute(
                """
                SELECT sample_uid, raw_json
                FROM samples
                WHERE COALESCE(processing_status, 'pending') IN ('pending', 'failed')
                                    AND session_merge_keep = TRUE
                ORDER BY id
                LIMIT ?
                """,
                [limit],
            ).fetchall()
            sample_uids = [row[0] for row in rows]
            if sample_uids:
                placeholders = ", ".join(["?"] * len(sample_uids))
                params = [datetime.now(), *sample_uids]
                self.conn.execute(
                    f"UPDATE samples SET processing_status = 'processing', processing_updated_at = ? WHERE sample_uid IN ({placeholders})",
                    params,
                )
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

        return [(row[0], json.loads(row[1])) for row in rows]

    def get_unprocessed_samples(self, limit: int = 100) -> list[tuple[str, dict]]:
        """Get samples that haven't been processed for round judgments."""
        rows = self.conn.execute(
            """
            SELECT s.sample_uid, s.raw_json
            FROM samples s
            WHERE COALESCE(s.processing_status, 'pending') IN ('pending', 'failed')
                            AND s.session_merge_keep = TRUE
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
             SELECT id, sample_uid, raw_json, user_query, assistant_response, empty_response, num_turns, expected_judgment_count,
                 expected_response_judgment_count, expected_episode_judgment_count, num_tool_calls,
                 response_helpful_rate, response_unhelpful_rate, user_satisfied_rate,
                 user_negative_feedback_rate, tool_stats, session_merge_status, session_merge_keep,
                 session_merge_group_id, session_merge_group_size, session_merge_representative_id,
                 session_merge_reason, session_merge_updated_at, processing_status, processing_updated_at
            FROM samples
            WHERE id = ?
            """,
            [sample_id],
        ).fetchone()
        return self._build_sample_record(row) if row else None

    def get_sample_by_uid(self, sample_uid: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            """
             SELECT id, sample_uid, raw_json, user_query, assistant_response, empty_response, num_turns, expected_judgment_count,
                 expected_response_judgment_count, expected_episode_judgment_count, num_tool_calls,
                 response_helpful_rate, response_unhelpful_rate, user_satisfied_rate,
                 user_negative_feedback_rate, tool_stats, session_merge_status, session_merge_keep,
                 session_merge_group_id, session_merge_group_size, session_merge_representative_id,
                 session_merge_reason, session_merge_updated_at, processing_status, processing_updated_at
            FROM samples
            WHERE sample_uid = ?
            """,
            [sample_uid],
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
        session_merge_keep: bool | None = None,
        session_merge_status: str | None = None,
        empty_response: bool | None = None,
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
        if session_merge_status and session_merge_status != "unmarked":
            builder.add_condition("session_merge_status", ComparisonOp.EQ, session_merge_status)
        if empty_response is not None:
            builder.add_condition("empty_response", ComparisonOp.EQ, empty_response)
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
        if session_merge_keep is True:
            extra_clauses.append("COALESCE(s.session_merge_keep, TRUE) = TRUE")
        elif session_merge_keep is False:
            extra_clauses.append("s.session_merge_keep = FALSE")
        if session_merge_status == "unmarked":
            extra_clauses.append("s.session_merge_status IS NULL")

        combined_where = where_clause
        if extra_clauses:
            combined_where = " AND ".join([where_clause, *extra_clauses]) if where_clause != "1=1" else " AND ".join(extra_clauses)

        count_query = f"SELECT COUNT(*) FROM samples s WHERE {combined_where}"
        total_row = self.conn.execute(count_query, params).fetchone()
        total = total_row[0] if total_row else 0

        query = f"""
             SELECT id, sample_uid, raw_json, user_query, assistant_response, empty_response, num_turns, expected_judgment_count,
                 expected_response_judgment_count, expected_episode_judgment_count, num_tool_calls,
                 response_helpful_rate, response_unhelpful_rate, user_satisfied_rate,
                 user_negative_feedback_rate, tool_stats, session_merge_status, session_merge_keep,
                 session_merge_group_id, session_merge_group_size, session_merge_representative_id,
                 session_merge_reason, session_merge_updated_at, processing_status, processing_updated_at
            FROM samples s
            WHERE {combined_where}
            ORDER BY id
            LIMIT ? OFFSET ?
        """
        rows = self.conn.execute(query, [*params, limit, offset]).fetchall()
        return [self._build_sample_record(row) for row in rows], total

    def get_session_merge_counts(self) -> dict[str, int]:
        """Return session merge marker counts for overview and validation."""
        row = self.conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN COALESCE(session_merge_keep, TRUE) THEN 1 ELSE 0 END) AS keep_count,
                SUM(CASE WHEN session_merge_keep = FALSE THEN 1 ELSE 0 END) AS merged_count,
                SUM(CASE WHEN session_merge_status = 'skipped' THEN 1 ELSE 0 END) AS skipped_count,
                SUM(CASE WHEN session_merge_status IS NULL THEN 1 ELSE 0 END) AS unmarked_count,
                SUM(CASE WHEN empty_response = TRUE THEN 1 ELSE 0 END) AS empty_response_count
            FROM samples
            """
        ).fetchone()
        return {
            "total": int(row[0] or 0),
            "keep": int(row[1] or 0),
            "merged": int(row[2] or 0),
            "skipped": int(row[3] or 0),
            "unmarked": int(row[4] or 0),
            "empty_response": int(row[5] or 0),
        }

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