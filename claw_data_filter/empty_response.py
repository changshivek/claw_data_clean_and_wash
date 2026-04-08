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


def backfill_empty_response(db_path: Path, dry_run: bool = False) -> dict[str, int]:
    """Backfill empty_response markers for an existing database."""
    store = DuckDBStore(db_path)
    try:
        rows = store.conn.execute(
            "SELECT id, raw_json, empty_response FROM samples ORDER BY id"
        ).fetchall()

        updates: list[tuple[bool, int]] = []
        empty_response_count = 0

        for sample_id, raw_json, current_value in rows:
            payload = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
            new_value = detect_empty_response(payload or {})
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