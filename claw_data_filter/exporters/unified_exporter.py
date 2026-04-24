"""Unified export service shared by CLI and Streamlit."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from claw_data_filter.filters.query import ComparisonOp, FilterQueryBuilder
from claw_data_filter.processors.round_feedback import TurnContextBuilder
from claw_data_filter.storage.duckdb_store import DuckDBStore

OPENAI_ROUND_FEEDBACK = "openai_round_feedback"
SUPPORTED_EXPORT_FORMATS = (OPENAI_ROUND_FEEDBACK,)
ALLOWED_IO_DIRS = ["data", "."]


@dataclass(slots=True)
class ExportFilterSpec:
    """Structured export filters shared by CLI and Web."""

    progress_op: str = ">="
    progress_val: float | None = None
    satisfied_op: str = ">="
    satisfied_val: float | None = None
    negative_feedback_op: str = ">="
    negative_feedback_val: float | None = None
    empty_response: bool | None = None
    session_merge_keep: bool | None = None
    session_merge_status: str | None = None
    has_error: bool | None = None
    num_turns_min: int | None = None
    num_turns_max: int | None = None
    date_from: str | None = None
    date_to: str | None = None
    selected_ids: list[int] = field(default_factory=list)
    selected_sample_uids: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ExportRequest:
    """Full export request."""

    output_path: Path
    export_format: str = OPENAI_ROUND_FEEDBACK
    filter_spec: ExportFilterSpec = field(default_factory=ExportFilterSpec)
    limit: int | None = None
    allowed_output_dirs: list[Path] = field(default_factory=list)


def _validate_output_path(path: Path, extra_allowed_dirs: list[Path] | None = None) -> None:
    """Validate output path is within allowed directories."""
    resolved_path = path.resolve()
    allowed_dirs = [Path.cwd() / item for item in ALLOWED_IO_DIRS]
    if extra_allowed_dirs:
        allowed_dirs.extend(extra_allowed_dirs)
    for allowed_dir in allowed_dirs:
        try:
            resolved_path.relative_to(allowed_dir.resolve())
            return
        except ValueError:
            continue
    allowed_display = [str(path) for path in allowed_dirs]
    raise ValueError(f"Output path must be within allowed directories: {allowed_display}")


class UnifiedExporter:
    """Shared exporter for OpenAI-compatible feedback JSONL."""

    def __init__(self, store: DuckDBStore):
        self.store = store

    def preview(self, filter_spec: ExportFilterSpec | None = None) -> dict[str, int]:
        """Return a lightweight preview for the current export filter."""
        where_clause, params = self._build_where_clause(filter_spec or ExportFilterSpec(), table_name="samples")
        row = self.store.conn.execute(
            f"""
            SELECT COUNT(*),
                   COALESCE(
                       AVG(
                           length(COALESCE(CAST(normalized_messages_json AS VARCHAR), ''))
                           + length(COALESCE(CAST(normalized_tools_json AS VARCHAR), ''))
                           + length(COALESCE(CAST(source_metadata_json AS VARCHAR), ''))
                       ),
                       0
                   )
            FROM samples
            WHERE {where_clause}
            """,
            params,
        ).fetchone()
        if row is None:
            row = (0, 0)
        count = int(row[0] or 0)
        avg_chars = int(row[1] or 0)
        return {
            "count": count,
            "estimated_bytes": count * avg_chars,
        }

    def export(self, request: ExportRequest) -> int:
        """Export records according to the request."""
        if request.export_format not in SUPPORTED_EXPORT_FORMATS:
            raise ValueError(f"Unsupported export format: {request.export_format}")

        _validate_output_path(request.output_path, request.allowed_output_dirs)
        rows = self._fetch_sample_rows(request.filter_spec, request.limit)
        request.output_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path: Path | None = None
        count = 0

        try:
            with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=request.output_path.parent, delete=False) as temp_file:
                temp_path = Path(temp_file.name)
                for row in rows:
                    payload = self._build_openai_round_feedback_record(row)
                    temp_file.write(json.dumps(payload, ensure_ascii=False) + "\n")
                    count += 1

            os.replace(temp_path, request.output_path)
        except Exception:
            if temp_path and temp_path.exists():
                temp_path.unlink()
            raise

        return count

    def _fetch_sample_rows(self, filter_spec: ExportFilterSpec, limit: int | None) -> list[dict[str, Any]]:
        where_clause, params = self._build_where_clause(filter_spec, table_name="samples")
        query = f"""
                        SELECT id, sample_uid, normalized_messages_json, normalized_tools_json, source_metadata_json, imported_at, empty_response,
                     num_turns, expected_judgment_count, expected_response_judgment_count,
                     expected_episode_judgment_count, num_tool_calls,
                   response_progress_rate, response_regress_rate,
                   user_satisfied_rate, user_negative_feedback_rate,
                   tool_stats, session_merge_status, session_merge_keep,
                   session_merge_reason, processing_status
            FROM samples
            WHERE {where_clause}
            ORDER BY id
        """
        if limit is not None:
            query += " LIMIT ?"
            params = [*params, limit]
        rows = self.store.conn.execute(query, params).fetchall()
        return [
            {
                "id": row[0],
                "sample_uid": row[1],
                "normalized_messages": json.loads(row[2]) if isinstance(row[2], str) else (row[2] or []),
                "normalized_tools": json.loads(row[3]) if isinstance(row[3], str) else (row[3] or []),
                "source_metadata": json.loads(row[4]) if isinstance(row[4], str) else (row[4] or {}),
                "imported_at": row[5],
                "empty_response": bool(row[6]),
                "num_turns": row[7] or 0,
                "expected_judgment_count": row[8] or 0,
                "expected_response_judgment_count": row[9] or 0,
                "expected_episode_judgment_count": row[10] or 0,
                "num_tool_calls": row[11] or 0,
                "response_progress_rate": row[12],
                "response_regress_rate": row[13],
                "user_satisfied_rate": row[14],
                "user_negative_feedback_rate": row[15],
                "tool_stats": json.loads(row[16]) if row[16] else {},
                "session_merge_status": row[17],
                "session_merge_keep": row[18],
                "session_merge_reason": row[19],
                "processing_status": row[20],
            }
            for row in rows
        ]

    def _build_where_clause(self, filter_spec: ExportFilterSpec, table_name: str) -> tuple[str, list[Any]]:
        builder = FilterQueryBuilder()

        if filter_spec.progress_val is not None:
            builder.add_condition("response_progress_rate", ComparisonOp(filter_spec.progress_op), filter_spec.progress_val)
        if filter_spec.satisfied_val is not None:
            builder.add_condition("user_satisfied_rate", ComparisonOp(filter_spec.satisfied_op), filter_spec.satisfied_val)
        if filter_spec.negative_feedback_val is not None:
            builder.add_condition("user_negative_feedback_rate", ComparisonOp(filter_spec.negative_feedback_op), filter_spec.negative_feedback_val)
        if filter_spec.empty_response is not None:
            builder.add_condition("empty_response", ComparisonOp.EQ, filter_spec.empty_response)
        if filter_spec.has_error is not None:
            builder.add_condition("has_error", ComparisonOp.EQ, filter_spec.has_error)
        if filter_spec.session_merge_status and filter_spec.session_merge_status != "unmarked":
            builder.add_condition("session_merge_status", ComparisonOp.EQ, filter_spec.session_merge_status)
        if filter_spec.num_turns_min is not None:
            builder.add_condition("num_turns", ComparisonOp.GTE, filter_spec.num_turns_min)
        if filter_spec.num_turns_max is not None:
            builder.add_condition("num_turns", ComparisonOp.LTE, filter_spec.num_turns_max)

        where_clause, params = builder.build_parameterized_where_clause(table_name)
        extra_clauses: list[str] = []

        if filter_spec.date_from:
            extra_clauses.append(f"{table_name}.imported_at >= ?")
            params.append(filter_spec.date_from)
        if filter_spec.date_to:
            extra_clauses.append(f"{table_name}.imported_at <= ?")
            params.append(filter_spec.date_to)
        if filter_spec.session_merge_keep is True:
            extra_clauses.append(f"COALESCE({table_name}.session_merge_keep, TRUE) = TRUE")
        elif filter_spec.session_merge_keep is False:
            extra_clauses.append(f"{table_name}.session_merge_keep = FALSE")
        if filter_spec.session_merge_status == "unmarked":
            extra_clauses.append(f"{table_name}.session_merge_status IS NULL")
        if filter_spec.selected_ids:
            placeholders = ", ".join(["?"] * len(filter_spec.selected_ids))
            extra_clauses.append(f"{table_name}.id IN ({placeholders})")
            params.extend(filter_spec.selected_ids)
        if filter_spec.selected_sample_uids:
            placeholders = ", ".join(["?"] * len(filter_spec.selected_sample_uids))
            extra_clauses.append(f"{table_name}.sample_uid IN ({placeholders})")
            params.extend(filter_spec.selected_sample_uids)

        if extra_clauses:
            return (
                " AND ".join([where_clause, *extra_clauses]) if where_clause != "1=1" else " AND ".join(extra_clauses),
                params,
            )
        return where_clause, params

    def _build_openai_round_feedback_record(self, sample_row: dict[str, Any]) -> dict[str, Any]:
        conversation = {"messages": sample_row["normalized_messages"]}
        if sample_row["normalized_tools"]:
            conversation["tools"] = sample_row["normalized_tools"]
        messages = conversation["messages"]
        builder = TurnContextBuilder()
        response_contexts = builder.extract_response_contexts(sample_row["sample_uid"], messages)
        episode_contexts = builder.extract_episode_contexts(sample_row["sample_uid"], messages)
        response_judgments = {
            judgment.response_index: judgment
            for judgment in self.store.get_assistant_response_judgments(sample_row["sample_uid"])
        }
        episode_judgments = {
            judgment.episode_index: judgment
            for judgment in self.store.get_user_episode_judgments(sample_row["sample_uid"])
        }

        response_steps = []
        for context in response_contexts:
            judgment = response_judgments.get(context.response_index)
            response_steps.append(
                {
                    "response_index": context.response_index,
                    "episode_index": context.episode_index,
                    "assistant_message_index": context.assistant_message_index,
                    "feedback_kind": context.feedback_kind.value,
                    "feedback_message_start_index": context.feedback_message_start_index,
                    "feedback_message_end_index": context.feedback_message_end_index,
                    "feedback_payload": judgment.feedback_payload if judgment else context.feedback_payload,
                    "response_progress": None if judgment is None else judgment.response_progress,
                    "llm_error": False if judgment is None else judgment.llm_error,
                }
            )

        episode_records = []
        for context in episode_contexts:
            judgment = episode_judgments.get(context.episode_index)
            episode_records.append(
                {
                    "episode_index": context.episode_index,
                    "message_start_index": context.start_user_message_index,
                    "message_end_index": context.end_before_user_message_index,
                    "signal_from_users": judgment.signal_from_users if judgment else context.signal_from_users,
                    "user_satisfied": None if judgment is None else judgment.user_satisfied,
                    "llm_error": False if judgment is None else judgment.llm_error,
                }
            )

        return {
            "schema": "openai_round_feedback_v2",
            "metadata": self._build_metadata(sample_row),
            "source_metadata": self._build_source_metadata(sample_row),
            "conversation": conversation,
            "round_feedback": {
                "response_progress_steps": response_steps,
                "user_satisfied_episodes": episode_records,
            },
        }

    def _build_metadata(self, sample_row: dict[str, Any]) -> dict[str, Any]:
        imported_at = sample_row.get("imported_at")
        tool_stats = sample_row.get("tool_stats") or {}
        return {
            "sample_uid": sample_row.get("sample_uid"),
            "local_sample_id": sample_row["id"],
            "imported_at": imported_at.isoformat() if isinstance(imported_at, datetime) else imported_at,
            "processing_status": sample_row.get("processing_status"),
            "empty_response": sample_row.get("empty_response", False),
            "session_merge_status": sample_row.get("session_merge_status"),
            "session_merge_keep": sample_row.get("session_merge_keep"),
            "session_merge_reason": sample_row.get("session_merge_reason"),
            "num_turns": sample_row.get("num_turns"),
            "expected_judgment_count": sample_row.get("expected_judgment_count"),
            "expected_response_judgment_count": sample_row.get("expected_response_judgment_count"),
            "expected_episode_judgment_count": sample_row.get("expected_episode_judgment_count"),
            "num_tool_calls": sample_row.get("num_tool_calls"),
            "response_progress_rate": sample_row.get("response_progress_rate"),
            "response_regress_rate": sample_row.get("response_regress_rate"),
            "user_satisfied_rate": sample_row.get("user_satisfied_rate"),
            "user_negative_feedback_rate": sample_row.get("user_negative_feedback_rate"),
            "has_error": tool_stats.get("has_error", False),
        }

    def _build_source_metadata(self, sample_row: dict[str, Any]) -> dict[str, Any]:
        source_metadata = dict(sample_row.get("source_metadata") or {})
        messages = sample_row.get("normalized_messages") or []
        source_metadata.setdefault(
            "source_format",
            "anthropic" if any(isinstance(message.get("content"), list) for message in messages) else "openai",
        )
        return source_metadata

    def _extract_text_content(self, content: Any) -> str:
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts: list[str] = []
            for part in content:
                if isinstance(part, dict) and part.get("type") == "text":
                    parts.append(part.get("text", ""))
            return "".join(parts)
        return str(content) if content else ""

    def _first_non_empty(self, *values: Any) -> Any:
        for value in values:
            if value not in (None, "", {}):
                return value
        return None