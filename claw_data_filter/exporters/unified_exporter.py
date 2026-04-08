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
from claw_data_filter.models.sample import (
    extract_messages_from_payload,
    extract_normalized_conversation_from_payload,
)
from claw_data_filter.storage.duckdb_store import DuckDBStore

RAW_JSONL = "raw_jsonl"
OPENAI_ROUND_FEEDBACK = "openai_round_feedback"
SUPPORTED_EXPORT_FORMATS = (RAW_JSONL, OPENAI_ROUND_FEEDBACK)
ALLOWED_IO_DIRS = ["data", "."]


@dataclass(slots=True)
class ExportFilterSpec:
    """Structured export filters shared by CLI and Web."""

    helpful_op: str = ">="
    helpful_val: float | None = None
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


@dataclass(slots=True)
class ExportRequest:
    """Full export request."""

    output_path: Path
    export_format: str = RAW_JSONL
    filter_spec: ExportFilterSpec = field(default_factory=ExportFilterSpec)
    limit: int | None = None


def _validate_output_path(path: Path) -> None:
    """Validate output path is within allowed directories."""
    resolved_path = path.resolve()
    for allowed_dir in [Path.cwd() / item for item in ALLOWED_IO_DIRS]:
        try:
            resolved_path.relative_to(allowed_dir.resolve())
            return
        except ValueError:
            continue
    raise ValueError(f"Output path must be within allowed directories: {ALLOWED_IO_DIRS}")


class UnifiedExporter:
    """Shared exporter for raw payload JSONL and OpenAI-compatible feedback JSONL."""

    def __init__(self, store: DuckDBStore):
        self.store = store

    def preview(self, filter_spec: ExportFilterSpec | None = None) -> dict[str, int]:
        """Return a lightweight preview for the current export filter."""
        where_clause, params = self._build_where_clause(filter_spec or ExportFilterSpec(), table_name="samples")
        row = self.store.conn.execute(
            f"SELECT COUNT(*), COALESCE(AVG(length(CAST(raw_json AS VARCHAR))), 0) FROM samples WHERE {where_clause}",
            params,
        ).fetchone()
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

        _validate_output_path(request.output_path)
        rows = self._fetch_sample_rows(request.filter_spec, request.limit)
        request.output_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path: Path | None = None
        count = 0

        try:
            with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=request.output_path.parent, delete=False) as temp_file:
                temp_path = Path(temp_file.name)
                for row in rows:
                    if request.export_format == RAW_JSONL:
                        payload = row["raw_json"]
                    else:
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
            SELECT id, sample_uid, raw_json, imported_at, empty_response,
                   num_turns, expected_judgment_count, num_tool_calls,
                   response_helpful_rate, response_unhelpful_rate,
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
                "raw_json": json.loads(row[2]) if isinstance(row[2], str) else (row[2] or {}),
                "imported_at": row[3],
                "empty_response": bool(row[4]),
                "num_turns": row[5] or 0,
                "expected_judgment_count": row[6] or 0,
                "num_tool_calls": row[7] or 0,
                "response_helpful_rate": row[8],
                "response_unhelpful_rate": row[9],
                "user_satisfied_rate": row[10],
                "user_negative_feedback_rate": row[11],
                "tool_stats": json.loads(row[12]) if row[12] else {},
                "session_merge_status": row[13],
                "session_merge_keep": row[14],
                "session_merge_reason": row[15],
                "processing_status": row[16],
            }
            for row in rows
        ]

    def _build_where_clause(self, filter_spec: ExportFilterSpec, table_name: str) -> tuple[str, list[Any]]:
        builder = FilterQueryBuilder()

        if filter_spec.helpful_val is not None:
            builder.add_condition("response_helpful_rate", ComparisonOp(filter_spec.helpful_op), filter_spec.helpful_val)
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

        if extra_clauses:
            return (
                " AND ".join([where_clause, *extra_clauses]) if where_clause != "1=1" else " AND ".join(extra_clauses),
                params,
            )
        return where_clause, params

    def _build_openai_round_feedback_record(self, sample_row: dict[str, Any]) -> dict[str, Any]:
        raw_json = sample_row["raw_json"]
        conversation = extract_normalized_conversation_from_payload(raw_json)
        messages = conversation["messages"]
        turn_ranges = self._build_turn_ranges(messages)
        judgments = {judgment.turn_index: judgment for judgment in self.store.get_turn_judgments(sample_row["id"])}
        turn_count = max(len(turn_ranges), max(judgments.keys(), default=-1) + 1)

        feedback_turns = []
        for turn_index in range(turn_count):
            turn_range = turn_ranges[turn_index] if turn_index < len(turn_ranges) else None
            judgment = judgments.get(turn_index)
            feedback_turns.append(
                {
                    "turn_index": turn_index,
                    "message_start_index": None if turn_range is None else turn_range["message_start_index"],
                    "message_end_index": None if turn_range is None else turn_range["message_end_index"],
                    "response_helpful": None if judgment is None else judgment.response_helpful,
                    "user_satisfied": None if judgment is None else judgment.user_satisfied,
                    "signal_from_users": [] if judgment is None else judgment.signal_from_users,
                    "llm_error": False if judgment is None else judgment.llm_error,
                }
            )

        return {
            "schema": "openai_round_feedback_v1",
            "metadata": self._build_metadata(sample_row),
            "source_metadata": self._build_source_metadata(raw_json),
            "conversation": conversation,
            "round_feedback": {
                "turns": feedback_turns,
            },
        }

    def _build_metadata(self, sample_row: dict[str, Any]) -> dict[str, Any]:
        imported_at = sample_row.get("imported_at")
        tool_stats = sample_row.get("tool_stats") or {}
        return {
            "sample_id": sample_row["id"],
            "sample_uid": sample_row.get("sample_uid"),
            "imported_at": imported_at.isoformat() if isinstance(imported_at, datetime) else imported_at,
            "processing_status": sample_row.get("processing_status"),
            "empty_response": sample_row.get("empty_response", False),
            "session_merge_status": sample_row.get("session_merge_status"),
            "session_merge_keep": sample_row.get("session_merge_keep"),
            "session_merge_reason": sample_row.get("session_merge_reason"),
            "num_turns": sample_row.get("num_turns"),
            "expected_judgment_count": sample_row.get("expected_judgment_count"),
            "num_tool_calls": sample_row.get("num_tool_calls"),
            "response_helpful_rate": sample_row.get("response_helpful_rate"),
            "response_unhelpful_rate": sample_row.get("response_unhelpful_rate"),
            "user_satisfied_rate": sample_row.get("user_satisfied_rate"),
            "user_negative_feedback_rate": sample_row.get("user_negative_feedback_rate"),
            "has_error": tool_stats.get("has_error", False),
        }

    def _build_source_metadata(self, raw_json: dict[str, Any]) -> dict[str, Any]:
        request = raw_json.get("request") if isinstance(raw_json.get("request"), dict) else {}
        headers = request.get("headers") if isinstance(request.get("headers"), dict) else {}
        body_json = request.get("bodyJson") if isinstance(request.get("bodyJson"), dict) else {}
        messages = extract_messages_from_payload(raw_json)
        source_metadata = raw_json.get("metadata")

        return {
            "timestamp": self._first_non_empty(
                raw_json.get("timestamp"),
                raw_json.get("created_at"),
                request.get("timestamp"),
                request.get("createdAt"),
            ),
            "model_requested": self._first_non_empty(body_json.get("model"), raw_json.get("model")),
            "user_agent": self._first_non_empty(
                headers.get("user-agent"),
                headers.get("User-Agent"),
                raw_json.get("user_agent"),
            ),
            "request_id": self._first_non_empty(
                raw_json.get("request_id"),
                raw_json.get("requestId"),
                request.get("request_id"),
                headers.get("x-request-id"),
            ),
            "trace_id": self._first_non_empty(
                raw_json.get("trace_id"),
                raw_json.get("traceId"),
                headers.get("x-trace-id"),
            ),
            "source_format": "anthropic" if any(isinstance(message.get("content"), list) for message in messages) else "openai",
            "metadata": source_metadata,
        }

    def _build_turn_ranges(self, messages: list[dict[str, Any]]) -> list[dict[str, int]]:
        turn_ranges: list[dict[str, int]] = []
        current_user_index: int | None = None
        last_response_index: int | None = None
        current_user_active = False
        current_has_response = False

        for index, message in enumerate(messages):
            role = message.get("role")
            if role == "system":
                continue

            if role == "user":
                user_text = self._extract_text_content(message.get("content"))
                if user_text:
                    if current_user_active and current_has_response and current_user_index is not None and last_response_index is not None:
                        turn_ranges.append(
                            {
                                "message_start_index": current_user_index,
                                "message_end_index": last_response_index,
                            }
                        )
                    current_user_index = index
                    current_has_response = False
                    last_response_index = None
                    current_user_active = True
                elif current_user_active:
                    current_has_response = True
                    last_response_index = index
                continue

            if role in {"assistant", "tool"} and current_user_active:
                current_has_response = True
                last_response_index = index

        if current_user_active and current_has_response and current_user_index is not None and last_response_index is not None:
            turn_ranges.append(
                {
                    "message_start_index": current_user_index,
                    "message_end_index": last_response_index,
                }
            )

        return turn_ranges

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