"""Content-driven session snapshot merge for imported DuckDB samples.

This module is designed to run after import and before round feedback.
It only uses normalized real user turns to detect duplicated session snapshots,
without relying on potentially unreliable metadata identifiers.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
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

logger = logging.getLogger(__name__)
SESSION_MERGE_PROGRESS_LOG_INTERVAL = 10

SENDER_WRAPPER_RE = re.compile(r"^Sender \(untrusted metadata\):\s*", re.IGNORECASE)
TIMESTAMP_PREFIX_RE = re.compile(r"^\[[^\]]+GMT[+-]\d+\]\s*")
SESSION_RESET_PREFIX_RE = re.compile(r"^A new session was started via /new or /reset\.\s*", re.IGNORECASE)
WHITESPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class SessionMergeCandidate:
    sample_uid: str
    local_id: int
    grouping_key: str | None
    user_turns: tuple[str, ...]
    message_count: int
    num_turns: int


@dataclass(frozen=True)
class SessionMergeDecision:
    sample_uid: str
    local_id: int
    status: str
    keep: bool
    group_id: str | None
    group_size: int
    representative_uid: str
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


def analyze_sample_row(row: tuple[str, int, str | list[str] | None, int | None, int | None]) -> SessionMergeCandidate:
    sample_uid, local_id, normalized_user_turns_json, message_count, num_turns = row
    if isinstance(normalized_user_turns_json, str):
        user_turns = tuple(json.loads(normalized_user_turns_json)) if normalized_user_turns_json else ()
    elif isinstance(normalized_user_turns_json, list):
        user_turns = tuple(str(item) for item in normalized_user_turns_json if item)
    else:
        user_turns = ()
    first_turn = user_turns[0] if user_turns else None
    return SessionMergeCandidate(
        sample_uid=sample_uid,
        local_id=local_id,
        grouping_key=first_turn,
        user_turns=user_turns,
        message_count=message_count or 0,
        num_turns=num_turns or 0,
    )


def analyze_raw_json_sample_row(row: tuple[str, int, str, int | None]) -> SessionMergeCandidate:
    sample_uid, local_id, raw_json, num_turns = row
    payload = json.loads(raw_json)
    user_turns = extract_real_user_turns(payload)
    first_turn = user_turns[0] if user_turns else None
    return SessionMergeCandidate(
        sample_uid=sample_uid,
        local_id=local_id,
        grouping_key=first_turn,
        user_turns=user_turns,
        message_count=len(extract_messages_from_payload(payload)),
        num_turns=num_turns or 0,
    )


def _candidate_sort_key(candidate: SessionMergeCandidate) -> tuple[int, int, int, int]:
    return (
        len(candidate.user_turns),
        candidate.num_turns,
        candidate.message_count,
        candidate.local_id,
    )


def _choose_best_candidate(candidates: Iterable[SessionMergeCandidate]) -> SessionMergeCandidate:
    return max(candidates, key=_candidate_sort_key)


def _resolve_representative(decisions: dict[str, SessionMergeDecision], sample_uid: str) -> str:
    representative_uid = decisions[sample_uid].representative_uid
    while representative_uid in decisions and not decisions[representative_uid].keep:
        next_uid = decisions[representative_uid].representative_uid
        if next_uid == representative_uid:
            break
        representative_uid = next_uid
    return representative_uid


def plan_session_merge(
    candidates: Iterable[SessionMergeCandidate],
    min_prefix_turns: int = 2,
) -> list[SessionMergeDecision]:
    """Plan which samples to keep or collapse as content-prefix snapshots."""
    decisions: dict[str, SessionMergeDecision] = {}
    grouped: dict[str, list[SessionMergeCandidate]] = defaultdict(list)

    for candidate in candidates:
        if not candidate.user_turns:
            decisions[candidate.sample_uid] = SessionMergeDecision(
                sample_uid=candidate.sample_uid,
                local_id=candidate.local_id,
                status="skipped",
                keep=True,
                group_id=None,
                group_size=1,
                representative_uid=candidate.sample_uid,
                reason="no_user_turns",
            )
            continue
        grouped[candidate.grouping_key or ""].append(candidate)

    for grouping_key, group_candidates in grouped.items():
        group_id = _hash_group_key(grouping_key) if grouping_key else None
        group_size = len(group_candidates)

        if group_size == 1:
            only = group_candidates[0]
            decisions[only.sample_uid] = SessionMergeDecision(
                sample_uid=only.sample_uid,
                local_id=only.local_id,
                status="keep",
                keep=True,
                group_id=group_id,
                group_size=1,
                representative_uid=only.sample_uid,
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
                if candidate.sample_uid == representative.sample_uid:
                    continue
                decisions[candidate.sample_uid] = SessionMergeDecision(
                    sample_uid=candidate.sample_uid,
                    local_id=candidate.local_id,
                    status="merged",
                    keep=False,
                    group_id=group_id,
                    group_size=group_size,
                    representative_uid=representative.sample_uid,
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
            if representative.sample_uid in decisions:
                continue

            if len(sequence) < min_prefix_turns:
                decisions[representative.sample_uid] = SessionMergeDecision(
                    sample_uid=representative.sample_uid,
                    local_id=representative.local_id,
                    status="keep",
                    keep=True,
                    group_id=group_id,
                    group_size=group_size,
                    representative_uid=representative.sample_uid,
                    reason="below_prefix_threshold",
                )
                continue

            winner = best_descendant_by_prefix.get(sequence)
            if winner is not None:
                decisions[representative.sample_uid] = SessionMergeDecision(
                    sample_uid=representative.sample_uid,
                    local_id=representative.local_id,
                    status="merged",
                    keep=False,
                    group_id=group_id,
                    group_size=group_size,
                    representative_uid=winner.sample_uid,
                    reason="strict_prefix_of_longer_sequence",
                )
                continue

            decisions[representative.sample_uid] = SessionMergeDecision(
                sample_uid=representative.sample_uid,
                local_id=representative.local_id,
                status="keep",
                keep=True,
                group_id=group_id,
                group_size=group_size,
                representative_uid=representative.sample_uid,
                reason="leaf_sequence",
            )

    resolved: list[SessionMergeDecision] = []
    for sample_uid, decision in sorted(decisions.items(), key=lambda item: item[1].local_id):
        representative_uid = decision.representative_uid
        if not decision.keep:
            representative_uid = _resolve_representative(decisions, sample_uid)
        resolved.append(
            SessionMergeDecision(
                sample_uid=decision.sample_uid,
                local_id=decision.local_id,
                status=decision.status,
                keep=decision.keep,
                group_id=decision.group_id,
                group_size=decision.group_size,
                representative_uid=representative_uid,
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
        "ALTER TABLE samples ADD COLUMN session_merge_representative_uid TEXT",
        "ALTER TABLE samples ADD COLUMN session_merge_reason TEXT",
        "ALTER TABLE samples ADD COLUMN session_merge_updated_at TIMESTAMP",
    ]
    for sql in alterations:
        try:
            conn.execute(sql)
        except Exception:
            logger.debug("session_merge schema column may already exist: %s", sql.split()[-1])


def _load_candidates(
    conn: duckdb.DuckDBPyConnection,
    batch_size: int,
    workers: int,
) -> list[SessionMergeCandidate]:
    total = conn.execute("SELECT COUNT(*) FROM samples").fetchone()[0]
    candidates: list[SessionMergeCandidate] = []
    max_workers = max(1, workers)
    total_batches = max(1, (total + batch_size - 1) // batch_size) if total else 0
    if max_workers > 1:
        executor = ProcessPoolExecutor(
            max_workers=max_workers,
            mp_context=multiprocessing.get_context("spawn"),
        )
    else:
        executor = ThreadPoolExecutor(max_workers=max_workers)
    column_names = {row[1] for row in conn.execute("PRAGMA table_info('samples')").fetchall()}
    use_structured_columns = {"normalized_user_turns_json", "message_count"}.issubset(column_names)
    with executor:
        for batch_index, offset in enumerate(range(0, total, batch_size), start=1):
            if use_structured_columns:
                rows = conn.execute(
                    "SELECT sample_uid, id, CAST(normalized_user_turns_json AS VARCHAR), message_count, num_turns FROM samples ORDER BY id LIMIT ? OFFSET ?",
                    [batch_size, offset],
                ).fetchall()
                candidates.extend(executor.map(analyze_sample_row, rows))
            else:
                rows = conn.execute(
                    "SELECT sample_uid, id, CAST(raw_json AS VARCHAR), num_turns FROM samples ORDER BY id LIMIT ? OFFSET ?",
                    [batch_size, offset],
                ).fetchall()
                candidates.extend(executor.map(analyze_raw_json_sample_row, rows))
            if batch_index == 1 or batch_index % SESSION_MERGE_PROGRESS_LOG_INTERVAL == 0 or batch_index == total_batches:
                logger.info(
                    "Session merge candidate scan progress: batches=%s/%s loaded_candidates=%s total_samples=%s",
                    batch_index,
                    total_batches,
                    len(candidates),
                    total,
                )
    return candidates


def _iter_decision_batches(decisions: list[SessionMergeDecision], batch_size: int) -> Iterable[list[SessionMergeDecision]]:
    for start in range(0, len(decisions), batch_size):
        yield decisions[start:start + batch_size]


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
    logger.info(
        "Starting session merge: db_path=%s workers=%s batch_size=%s min_prefix_turns=%s dry_run=%s",
        db_path,
        workers,
        batch_size,
        min_prefix_turns,
        dry_run,
    )
    conn = duckdb.connect(str(db_path))
    try:
        ensure_session_merge_schema(conn)
        total_samples = conn.execute("SELECT COUNT(*) FROM samples").fetchone()[0]
        logger.info("Session merge discovered total_samples=%s", total_samples)
        candidates = _load_candidates(conn, batch_size=batch_size, workers=workers)
        decisions = plan_session_merge(candidates, min_prefix_turns=min_prefix_turns)
        summary = _build_summary(decisions, total_samples)
        logger.info("Session merge planning summary: %s", json.dumps(summary, ensure_ascii=False, sort_keys=True))
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
                    session_merge_representative_uid = NULL,
                    session_merge_reason = NULL,
                    session_merge_updated_at = NULL
                """
            )
            total_batches = max(1, (len(decisions) + batch_size - 1) // batch_size) if decisions else 0
            for index, decision_batch in enumerate(_iter_decision_batches(decisions, batch_size), start=1):
                conn.executemany(
                    """
                    UPDATE samples
                    SET session_merge_status = ?,
                        session_merge_keep = ?,
                        session_merge_group_id = ?,
                        session_merge_group_size = ?,
                        session_merge_representative_uid = ?,
                        session_merge_reason = ?,
                        session_merge_updated_at = ?
                    WHERE sample_uid = ?
                    """,
                    [
                        (
                            decision.status,
                            decision.keep,
                            decision.group_id,
                            decision.group_size,
                            decision.representative_uid,
                            decision.reason,
                            updated_at,
                            decision.sample_uid,
                        )
                        for decision in decision_batch
                    ],
                )
                if index == 1 or index % SESSION_MERGE_PROGRESS_LOG_INTERVAL == 0 or index == total_batches:
                    logger.info(
                        "Session merge write progress: batches=%s/%s updated_rows=%s/%s",
                        index,
                        total_batches,
                        min(index * batch_size, len(decisions)),
                        len(decisions),
                    )
            conn.execute("COMMIT")
            logger.info("Session merge write phase committed successfully")
        except Exception:
            conn.execute("ROLLBACK")
            logger.exception("Session merge write phase rolled back due to an error")
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