"""Content-driven session snapshot merge for imported DuckDB samples.

This module is designed to run after import and before round feedback.
It only uses normalized real user turns to detect duplicated session snapshots,
without relying on potentially unreliable metadata identifiers.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import multiprocessing
import re
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import duckdb

from claw_data_filter.models.sample import extract_messages_from_payload

SENDER_WRAPPER_RE = re.compile(r"^Sender \(untrusted metadata\):\s*", re.IGNORECASE)
TIMESTAMP_PREFIX_RE = re.compile(r"^\[[^\]]+GMT[+-]\d+\]\s*")
SESSION_RESET_PREFIX_RE = re.compile(r"^A new session was started via /new or /reset\.\s*", re.IGNORECASE)
WHITESPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class SessionMergeCandidate:
    sample_id: int
    grouping_key: str | None
    user_turns: tuple[str, ...]
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
    """Normalize real user text for stable exact-prefix comparison."""
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


def _hash_group_key(grouping_key: str) -> str:
    return hashlib.sha1(grouping_key.encode("utf-8")).hexdigest()[:16]


def analyze_sample_row(row: tuple[int, str, int | None]) -> SessionMergeCandidate:
    sample_id, raw_json, num_turns = row
    payload = json.loads(raw_json)
    user_turns = extract_real_user_turns(payload)
    first_turn = user_turns[0] if user_turns else None
    message_count = len(extract_messages_from_payload(payload))
    return SessionMergeCandidate(
        sample_id=sample_id,
        grouping_key=first_turn,
        user_turns=user_turns,
        message_count=message_count,
        num_turns=num_turns or 0,
    )


def _candidate_sort_key(candidate: SessionMergeCandidate) -> tuple[int, int, int, int]:
    return (
        len(candidate.user_turns),
        candidate.num_turns,
        candidate.message_count,
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
    """Plan which samples to keep or collapse as content-prefix snapshots."""
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
        grouped[candidate.grouping_key or ""].append(candidate)

    for grouping_key, group_candidates in grouped.items():
        group_id = _hash_group_key(grouping_key) if grouping_key else None
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
        best_descendant_by_prefix: dict[tuple[str, ...], SessionMergeCandidate] = {}
        for candidate in unique_representatives.values():
            for prefix_len in range(min_prefix_turns, len(candidate.user_turns)):
                prefix = candidate.user_turns[:prefix_len]
                current = best_descendant_by_prefix.get(prefix)
                if current is None or _candidate_sort_key(candidate) > _candidate_sort_key(current):
                    best_descendant_by_prefix[prefix] = candidate

        for sequence in sorted(sequences, key=len):
            representative = unique_representatives[sequence]
            if representative.sample_id in decisions:
                continue

            if len(sequence) < min_prefix_turns:
                decisions[representative.sample_id] = SessionMergeDecision(
                    sample_id=representative.sample_id,
                    status="keep",
                    keep=True,
                    group_id=group_id,
                    group_size=group_size,
                    representative_id=representative.sample_id,
                    reason="below_prefix_threshold",
                )
                continue

            winner = best_descendant_by_prefix.get(sequence)
            if winner is not None:
                decisions[representative.sample_id] = SessionMergeDecision(
                    sample_id=representative.sample_id,
                    status="merged",
                    keep=False,
                    group_id=group_id,
                    group_size=group_size,
                    representative_id=winner.sample_id,
                    reason="strict_prefix_of_longer_sequence",
                )
                continue

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
    for sample_id in sorted(decisions):
        decision = decisions[sample_id]
        representative_id = decision.representative_id
        if not decision.keep:
            representative_id = _resolve_representative(decisions, sample_id)
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
    return resolved


def ensure_session_merge_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Ensure samples table exposes the marker columns used by session merge."""
    alterations = [
        "ALTER TABLE samples ADD COLUMN session_merge_status TEXT",
        "ALTER TABLE samples ADD COLUMN session_merge_keep BOOLEAN",
        "ALTER TABLE samples ADD COLUMN session_merge_group_id TEXT",
        "ALTER TABLE samples ADD COLUMN session_merge_group_size INTEGER",
        "ALTER TABLE samples ADD COLUMN session_merge_representative_id INTEGER",
        "ALTER TABLE samples ADD COLUMN session_merge_reason TEXT",
        "ALTER TABLE samples ADD COLUMN session_merge_updated_at TIMESTAMP",
    ]
    for sql in alterations:
        try:
            conn.execute(sql)
        except Exception:
            pass


def _load_candidates(
    conn: duckdb.DuckDBPyConnection,
    batch_size: int,
    workers: int,
) -> list[SessionMergeCandidate]:
    total = conn.execute("SELECT COUNT(*) FROM samples").fetchone()[0]
    candidates: list[SessionMergeCandidate] = []
    max_workers = max(1, workers)
    if max_workers > 1:
        executor = ProcessPoolExecutor(
            max_workers=max_workers,
            mp_context=multiprocessing.get_context("spawn"),
        )
    else:
        executor = ThreadPoolExecutor(max_workers=max_workers)
    with executor:
        for offset in range(0, total, batch_size):
            rows = conn.execute(
                "SELECT id, CAST(raw_json AS VARCHAR), num_turns FROM samples ORDER BY id LIMIT ? OFFSET ?",
                [batch_size, offset],
            ).fetchall()
            candidates.extend(executor.map(analyze_sample_row, rows))
    return candidates


def _build_summary(decisions: list[SessionMergeDecision], total_samples: int) -> dict[str, int]:
    status_counts = Counter(decision.status for decision in decisions)
    reason_counts = Counter(decision.reason for decision in decisions)
    keep_count = sum(1 for decision in decisions if decision.keep)
    merged_count = sum(1 for decision in decisions if not decision.keep)
    return {
        "total_samples": total_samples,
        "planned_samples": len(decisions),
        "keep_count": keep_count,
        "merged_count": merged_count,
        "skipped_count": status_counts.get("skipped", 0),
        "keep_status_count": status_counts.get("keep", 0),
        "merged_status_count": status_counts.get("merged", 0),
        "exact_duplicate_sequence": reason_counts.get("exact_duplicate_sequence", 0),
        "strict_prefix_of_longer_sequence": reason_counts.get("strict_prefix_of_longer_sequence", 0),
        "leaf_sequence": reason_counts.get("leaf_sequence", 0),
        "below_prefix_threshold": reason_counts.get("below_prefix_threshold", 0),
        "no_user_turns": reason_counts.get("no_user_turns", 0),
        "singleton_group": reason_counts.get("singleton_group", 0),
    }


def run_session_merge(
    db_path: Path | str,
    *,
    dry_run: bool = False,
    batch_size: int = 512,
    workers: int = 4,
    min_prefix_turns: int = 2,
) -> dict[str, int]:
    """Analyze imported samples and mark which session snapshots should flow onward."""
    conn = duckdb.connect(str(db_path))
    try:
        ensure_session_merge_schema(conn)
        total_samples = conn.execute("SELECT COUNT(*) FROM samples").fetchone()[0]
        candidates = _load_candidates(conn, batch_size=batch_size, workers=workers)
        decisions = plan_session_merge(candidates, min_prefix_turns=min_prefix_turns)
        summary = _build_summary(decisions, total_samples)
        if dry_run:
            return summary

        updated_at = datetime.now()
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
                [
                    (
                        decision.status,
                        decision.keep,
                        decision.group_id,
                        decision.group_size,
                        decision.representative_id,
                        decision.reason,
                        updated_at,
                        decision.sample_id,
                    )
                    for decision in decisions
                ],
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        return summary
    finally:
        conn.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Merge content-prefix session snapshots in DuckDB samples")
    parser.add_argument("--db-path", required=True, help="DuckDB database path")
    parser.add_argument("--batch-size", type=int, default=512, help="Batch size when scanning samples")
    parser.add_argument("--workers", type=int, default=4, help="Worker count for JSON analysis")
    parser.add_argument("--min-prefix-turns", type=int, default=2, help="Minimum shared user turns before collapsing a strict prefix")
    parser.add_argument("--dry-run", action="store_true", help="Only print the merge summary without writing markers")
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    summary = run_session_merge(
        args.db_path,
        dry_run=args.dry_run,
        batch_size=args.batch_size,
        workers=args.workers,
        min_prefix_turns=args.min_prefix_turns,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()