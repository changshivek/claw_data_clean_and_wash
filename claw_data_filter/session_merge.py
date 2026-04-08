"""Standalone session snapshot merge flow for imported DuckDB samples.

This module marks intermediate session snapshots in the samples table without
changing the existing import or round-feedback pipeline. The default strategy
is conservative:

- Prefer stable session identifiers such as clientTokenId or sessionId.
- Only collapse samples when one sample's normalized user-turn sequence is a
  strict prefix of another sample in the same session partition.
- Keep all leaf samples so diverging branches of a conversation are preserved.

It can be executed directly:

    python -m claw_data_filter.session_merge --db-path data/example.duckdb
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Iterator

import duckdb

from claw_data_filter.models.sample import extract_messages_from_payload

SENDER_WRAPPER_RE = re.compile(r"^Sender \(untrusted metadata\):\s*", re.IGNORECASE)
TIMESTAMP_PREFIX_RE = re.compile(r"^\[[^\]]+GMT[+-]\d+\]\s*")
SESSION_RESET_PREFIX_RE = re.compile(r"^A new session was started via /new or /reset\.\s*", re.IGNORECASE)
WHITESPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class SessionMergeCandidate:
    sample_id: int
    session_partition_key: str | None
    grouping_key: str | None
    grouping_source: str
    user_turns: tuple[str, ...]
    request_started_at: str
    message_count: int
    num_turns: int


@dataclass(frozen=True)
class SessionMergeDecision:
    sample_id: int
    status: str
    keep: bool
    group_id: str | None
    group_size: int
    representative_id: int
    reason: str


def normalize_user_text(text: str) -> str:
    """Normalize real user text for stable prefix comparison."""
    cleaned_lines: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        line = SENDER_WRAPPER_RE.sub("", line)
        line = TIMESTAMP_PREFIX_RE.sub("", line)
        line = SESSION_RESET_PREFIX_RE.sub("", line)
        if not line:
            continue
        line = WHITESPACE_RE.sub(" ", line).strip()
        if line:
            cleaned_lines.append(line)
    return "\n".join(cleaned_lines).strip()


def _extract_text_parts(content: Any) -> str:
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return str(content) if content else ""

    text_parts: list[str] = []
    for part in content:
        if not isinstance(part, dict):
            continue
        if part.get("type") == "text":
            text_parts.append(part.get("text", ""))
    return "\n".join(text_parts)


def extract_real_user_turns(payload: dict[str, Any]) -> tuple[str, ...]:
    """Extract normalized real user turns while skipping tool_result-only blocks."""
    messages = extract_messages_from_payload(payload)
    turns: list[str] = []
    for message in messages:
        if message.get("role") != "user":
            continue
        normalized = normalize_user_text(_extract_text_parts(message.get("content")))
        if normalized:
            turns.append(normalized)
    return tuple(turns)


def _extract_session_partition_key(payload: dict[str, Any]) -> tuple[str | None, str]:
    log = payload.get("log") if isinstance(payload.get("log"), dict) else {}
    client_token_id = str(log.get("clientTokenId") or "").strip()
    session_id = str(log.get("sessionId") or "").strip()
    user_id = str(log.get("userId") or "").strip()
    route_path = str(log.get("routePath") or "").strip()

    if client_token_id:
        return f"clientTokenId:{client_token_id}|route:{route_path}", "client_token_id"
    if session_id:
        return f"sessionId:{session_id}|route:{route_path}", "session_id"
    if user_id and route_path:
        return f"userId:{user_id}|route:{route_path}", "user_id_route"
    return None, "missing"


def _extract_request_started_at(payload: dict[str, Any]) -> str:
    log = payload.get("log") if isinstance(payload.get("log"), dict) else {}
    value = log.get("requestStartedAt") or log.get("createdAt") or payload.get("exportedAt")
    return str(value or "")


def _hash_group_key(grouping_key: str) -> str:
    return hashlib.sha1(grouping_key.encode("utf-8")).hexdigest()[:16]


def analyze_sample_row(row: tuple[int, str, int | None], allow_first_turn_fallback: bool = False) -> SessionMergeCandidate:
    sample_id, raw_json, num_turns = row
    payload = json.loads(raw_json)
    user_turns = extract_real_user_turns(payload)
    session_partition_key, grouping_source = _extract_session_partition_key(payload)

    grouping_key = session_partition_key
    if grouping_key is None and allow_first_turn_fallback and user_turns:
        first_turn_hash = hashlib.sha1(user_turns[0].encode("utf-8")).hexdigest()[:16]
        grouping_key = f"firstTurn:{first_turn_hash}"
        grouping_source = "first_turn_fallback"

    message_count = len(extract_messages_from_payload(payload))
    return SessionMergeCandidate(
        sample_id=sample_id,
        session_partition_key=session_partition_key,
        grouping_key=grouping_key,
        grouping_source=grouping_source,
        user_turns=user_turns,
        request_started_at=_extract_request_started_at(payload),
        message_count=message_count,
        num_turns=num_turns or 0,
    )


def _candidate_sort_key(candidate: SessionMergeCandidate) -> tuple[int, int, int, str, int]:
    return (
        len(candidate.user_turns),
        candidate.num_turns,
        candidate.message_count,
        candidate.request_started_at,
        candidate.sample_id,
    )


def _choose_best_candidate(candidates: Iterable[SessionMergeCandidate]) -> SessionMergeCandidate:
    return max(candidates, key=_candidate_sort_key)


def _resolve_representative(decisions: dict[int, SessionMergeDecision], sample_id: int) -> int:
    representative_id = decisions[sample_id].representative_id
    while representative_id in decisions and not decisions[representative_id].keep:
        next_id = decisions[representative_id].representative_id
        if next_id == representative_id:
            break
        representative_id = next_id
    return representative_id


def plan_session_merge(
    candidates: Iterable[SessionMergeCandidate],
    min_prefix_turns: int = 2,
) -> list[SessionMergeDecision]:
    """Plan which samples to keep or collapse as intermediate snapshots."""
    decisions: dict[int, SessionMergeDecision] = {}
    grouped: dict[str, list[SessionMergeCandidate]] = defaultdict(list)

    for candidate in candidates:
        if not candidate.user_turns:
            decisions[candidate.sample_id] = SessionMergeDecision(
                sample_id=candidate.sample_id,
                status="skipped",
                keep=True,
                group_id=None,
                group_size=1,
                representative_id=candidate.sample_id,
                reason="no_user_turns",
            )
            continue
        if candidate.grouping_key is None:
            decisions[candidate.sample_id] = SessionMergeDecision(
                sample_id=candidate.sample_id,
                status="skipped",
                keep=True,
                group_id=None,
                group_size=1,
                representative_id=candidate.sample_id,
                reason="no_session_key",
            )
            continue
        grouped[candidate.grouping_key].append(candidate)

    for grouping_key, group_candidates in grouped.items():
        group_id = _hash_group_key(grouping_key)
        group_size = len(group_candidates)

        if group_size == 1:
            only = group_candidates[0]
            decisions[only.sample_id] = SessionMergeDecision(
                sample_id=only.sample_id,
                status="keep",
                keep=True,
                group_id=group_id,
                group_size=1,
                representative_id=only.sample_id,
                reason="singleton_group",
            )
            continue

        exact_groups: dict[tuple[str, ...], list[SessionMergeCandidate]] = defaultdict(list)
        for candidate in group_candidates:
            exact_groups[candidate.user_turns].append(candidate)

        unique_representatives: dict[tuple[str, ...], SessionMergeCandidate] = {}
        for sequence, same_sequence_candidates in exact_groups.items():
            representative = _choose_best_candidate(same_sequence_candidates)
            unique_representatives[sequence] = representative
            for candidate in same_sequence_candidates:
                if candidate.sample_id == representative.sample_id:
                    continue
                decisions[candidate.sample_id] = SessionMergeDecision(
                    sample_id=candidate.sample_id,
                    status="merged",
                    keep=False,
                    group_id=group_id,
                    group_size=group_size,
                    representative_id=representative.sample_id,
                    reason="exact_duplicate_sequence",
                )

        sequences = list(unique_representatives.keys())
        for sequence in sorted(sequences, key=len):
            representative = unique_representatives[sequence]
            if representative.sample_id in decisions:
                continue

            descendants = [
                unique_representatives[other_sequence]
                for other_sequence in sequences
                if len(other_sequence) > len(sequence)
                and len(sequence) >= min_prefix_turns
                and other_sequence[:len(sequence)] == sequence
            ]
            if descendants:
                best_descendant = _choose_best_candidate(descendants)
                decisions[representative.sample_id] = SessionMergeDecision(
                    sample_id=representative.sample_id,
                    status="merged",
                    keep=False,
                    group_id=group_id,
                    group_size=group_size,
                    representative_id=best_descendant.sample_id,
                    reason="strict_prefix_of_longer_sequence",
                )
            else:
                decisions[representative.sample_id] = SessionMergeDecision(
                    sample_id=representative.sample_id,
                    status="keep",
                    keep=True,
                    group_id=group_id,
                    group_size=group_size,
                    representative_id=representative.sample_id,
                    reason="leaf_sequence",
                )

    resolved: list[SessionMergeDecision] = []
    for decision in decisions.values():
        representative_id = decision.representative_id
        if not decision.keep:
            representative_id = _resolve_representative(decisions, representative_id)
        resolved.append(
            SessionMergeDecision(
                sample_id=decision.sample_id,
                status=decision.status,
                keep=decision.keep,
                group_id=decision.group_id,
                group_size=decision.group_size,
                representative_id=representative_id,
                reason=decision.reason,
            )
        )

    return sorted(resolved, key=lambda item: item.sample_id)


def summarize_decisions(decisions: Iterable[SessionMergeDecision]) -> dict[str, int]:
    decision_list = list(decisions)
    reason_counts = Counter(decision.reason for decision in decision_list)
    summary = {
        "total_samples": len(decision_list),
        "keep_samples": sum(1 for item in decision_list if item.keep),
        "merged_samples": sum(1 for item in decision_list if not item.keep),
        "groups_with_markers": sum(1 for item in decision_list if item.group_id is not None),
    }
    for reason, count in sorted(reason_counts.items()):
        summary[f"reason::{reason}"] = count
    return summary


def ensure_session_merge_columns(conn: duckdb.DuckDBPyConnection) -> None:
    for statement in [
        "ALTER TABLE samples ADD COLUMN session_merge_status TEXT",
        "ALTER TABLE samples ADD COLUMN session_merge_keep BOOLEAN",
        "ALTER TABLE samples ADD COLUMN session_merge_group_id TEXT",
        "ALTER TABLE samples ADD COLUMN session_merge_group_size INTEGER",
        "ALTER TABLE samples ADD COLUMN session_merge_representative_id INTEGER",
        "ALTER TABLE samples ADD COLUMN session_merge_reason TEXT",
        "ALTER TABLE samples ADD COLUMN session_merge_updated_at TIMESTAMP",
    ]:
        try:
            conn.execute(statement)
        except Exception:
            pass

    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_samples_session_merge_keep ON samples(session_merge_keep)")
    except Exception:
        pass


def iter_sample_rows(
    conn: duckdb.DuckDBPyConnection,
    batch_size: int,
) -> Iterator[list[tuple[int, str, int | None]]]:
    cursor = conn.execute("SELECT id, raw_json, num_turns FROM samples ORDER BY id")
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        yield rows


def collect_candidates(
    conn: duckdb.DuckDBPyConnection,
    batch_size: int,
    workers: int,
    allow_first_turn_fallback: bool,
) -> list[SessionMergeCandidate]:
    candidates: list[SessionMergeCandidate] = []
    for rows in iter_sample_rows(conn, batch_size=batch_size):
        if workers > 1:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                batch_candidates = list(
                    executor.map(
                        lambda row: analyze_sample_row(row, allow_first_turn_fallback=allow_first_turn_fallback),
                        rows,
                    )
                )
        else:
            batch_candidates = [
                analyze_sample_row(row, allow_first_turn_fallback=allow_first_turn_fallback)
                for row in rows
            ]
        candidates.extend(batch_candidates)
    return candidates


def apply_decisions(conn: duckdb.DuckDBPyConnection, decisions: Iterable[SessionMergeDecision]) -> None:
    ensure_session_merge_columns(conn)
    decision_rows = [
        (
            decision.status,
            decision.keep,
            decision.group_id,
            decision.group_size,
            decision.representative_id,
            decision.reason,
            datetime.now(),
            decision.sample_id,
        )
        for decision in decisions
    ]

    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute(
            """
            UPDATE samples
            SET session_merge_status = NULL,
                session_merge_keep = NULL,
                session_merge_group_id = NULL,
                session_merge_group_size = NULL,
                session_merge_representative_id = NULL,
                session_merge_reason = NULL,
                session_merge_updated_at = NULL
            """
        )
        conn.executemany(
            """
            UPDATE samples
            SET session_merge_status = ?,
                session_merge_keep = ?,
                session_merge_group_id = ?,
                session_merge_group_size = ?,
                session_merge_representative_id = ?,
                session_merge_reason = ?,
                session_merge_updated_at = ?
            WHERE id = ?
            """,
            decision_rows,
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def run_session_merge(
    db_path: Path,
    batch_size: int = 128,
    workers: int = 1,
    min_prefix_turns: int = 2,
    allow_first_turn_fallback: bool = False,
    dry_run: bool = False,
) -> dict[str, int]:
    conn = duckdb.connect(str(db_path), read_only=dry_run)
    try:
        candidates = collect_candidates(
            conn,
            batch_size=batch_size,
            workers=workers,
            allow_first_turn_fallback=allow_first_turn_fallback,
        )
        decisions = plan_session_merge(candidates, min_prefix_turns=min_prefix_turns)
        summary = summarize_decisions(decisions)
        if not dry_run:
            apply_decisions(conn, decisions)
        return summary
    finally:
        conn.close()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Mark intermediate session snapshots in samples table.")
    parser.add_argument("--db-path", required=True, type=Path, help="DuckDB path to process")
    parser.add_argument("--batch-size", type=int, default=128, help="Rows fetched from DuckDB per batch")
    parser.add_argument("--workers", type=int, default=1, help="Thread workers for per-batch JSON parsing")
    parser.add_argument(
        "--min-prefix-turns",
        type=int,
        default=2,
        help="Minimum shared user-turn count before a shorter sample can be collapsed into a longer one",
    )
    parser.add_argument(
        "--allow-first-turn-fallback",
        action="store_true",
        help="When stable session keys are missing, use the first user turn to build candidate groups",
    )
    parser.add_argument("--dry-run", action="store_true", help="Only compute and print summary without writing markers")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    summary = run_session_merge(
        db_path=args.db_path,
        batch_size=args.batch_size,
        workers=args.workers,
        min_prefix_turns=args.min_prefix_turns,
        allow_first_turn_fallback=args.allow_first_turn_fallback,
        dry_run=args.dry_run,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())