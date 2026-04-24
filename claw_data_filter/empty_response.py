"""Helpers for marking samples whose imported payload contains no assistant reply."""

import json
from pathlib import Path
from typing import Any

from claw_data_filter.models.sample import extract_messages_from_payload, has_empty_response
from claw_data_filter.storage.duckdb_store import DuckDBStore


def detect_empty_response(raw_json: dict[str, Any]) -> bool:
    """Apply the shared empty-response rule to a raw payload."""
    messages = extract_messages_from_payload(raw_json)
    return has_empty_response(messages)


def detect_empty_response_from_normalized_messages(messages: list[dict[str, Any]]) -> bool:
    """Apply the shared empty-response rule to normalized messages."""
    return has_empty_response(messages)


def backfill_empty_response(db_path: Path, dry_run: bool = False) -> dict[str, int]:
    """Backfill empty_response markers for an existing database."""
    store = DuckDBStore(db_path)
    try:
        rows = store.conn.execute(
            "SELECT id, normalized_messages_json, empty_response FROM samples ORDER BY id"
        ).fetchall()

        updates: list[tuple[bool, int]] = []
        empty_response_count = 0

        for sample_id, normalized_messages_json, current_value in rows:
            if isinstance(normalized_messages_json, str):
                normalized_messages = json.loads(normalized_messages_json) if normalized_messages_json else []
            else:
                normalized_messages = normalized_messages_json or []
            new_value = detect_empty_response_from_normalized_messages(normalized_messages)
            if new_value:
                empty_response_count += 1
            if current_value is None or bool(current_value) != new_value:
                updates.append((new_value, sample_id))

        if not dry_run and updates:
            store.conn.executemany(
                "UPDATE samples SET empty_response = ? WHERE id = ?",
                updates,
            )

        return {
            "total_samples": len(rows),
            "empty_response_count": empty_response_count,
            "updated_count": len(updates),
        }
    finally:
        store.close()