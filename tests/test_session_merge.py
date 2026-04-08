import json
import tempfile
from pathlib import Path

import duckdb

from claw_data_filter.session_merge import (
    SessionMergeCandidate,
    extract_real_user_turns,
    plan_session_merge,
    run_session_merge,
)


def _make_payload(messages: list[dict]) -> dict:
    return {
        "request": {
            "bodyJson": {
                "messages": messages,
            }
        }
    }


def test_extract_real_user_turns_skips_tool_result_only_user_blocks():
    payload = _make_payload(
        [
            {"role": "user", "content": [{"type": "text", "text": "Sender (untrusted metadata): [Sat 2026-03-28 11:17 GMT+8] 你好"}]},
            {"role": "assistant", "content": [{"type": "text", "text": "我先看一下"}]},
            {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "call_1", "content": "ok"}]},
            {"role": "user", "content": [{"type": "text", "text": "A new session was started via /new or /reset."}, {"type": "text", "text": "继续"}]},
        ]
    )

    turns = extract_real_user_turns(payload)

    assert turns == ("你好", "继续")


def test_plan_session_merge_keeps_leaf_sequences_and_merges_prefixes():
    candidates = [
        SessionMergeCandidate(1, "a", ("a", "b"), 10, 2),
        SessionMergeCandidate(2, "a", ("a", "b", "c"), 12, 3),
        SessionMergeCandidate(3, "a", ("a", "b", "d"), 13, 3),
        SessionMergeCandidate(4, "a", ("a", "b", "c"), 14, 3),
        SessionMergeCandidate(5, "x", ("x",), 2, 1),
    ]

    decisions = {decision.sample_id: decision for decision in plan_session_merge(candidates, min_prefix_turns=2)}

    assert decisions[1].keep is False
    assert decisions[1].reason == "strict_prefix_of_longer_sequence"
    assert decisions[1].representative_id == 4
    assert decisions[2].keep is False
    assert decisions[2].reason == "exact_duplicate_sequence"
    assert decisions[2].representative_id == 4
    assert decisions[3].keep is True
    assert decisions[3].reason == "leaf_sequence"
    assert decisions[4].keep is True
    assert decisions[4].reason == "leaf_sequence"
    assert decisions[5].keep is True
    assert decisions[5].reason == "singleton_group"


def test_plan_session_merge_preserves_short_prefixes_below_threshold():
    candidates = [
        SessionMergeCandidate(1, "hello", ("hello",), 2, 1),
        SessionMergeCandidate(2, "hello", ("hello", "follow up"), 4, 2),
    ]

    decisions = {decision.sample_id: decision for decision in plan_session_merge(candidates, min_prefix_turns=2)}

    assert decisions[1].keep is True
    assert decisions[1].reason == "below_prefix_threshold"
    assert decisions[2].keep is True
    assert decisions[2].reason == "leaf_sequence"


def test_run_session_merge_writes_markers_into_duckdb():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "session_merge.db"
        conn = duckdb.connect(str(db_path))
        conn.execute(
            """
            CREATE TABLE samples (
                id INTEGER PRIMARY KEY,
                raw_json JSON,
                num_turns INTEGER,
                processing_status TEXT
            )
            """
        )

        payloads = [
            _make_payload([
                {"role": "user", "content": [{"type": "text", "text": "第一问"}]},
                {"role": "assistant", "content": [{"type": "text", "text": "答复"}]},
                {"role": "user", "content": [{"type": "text", "text": "第二问"}]},
            ]),
            _make_payload([
                {"role": "user", "content": [{"type": "text", "text": "第一问"}]},
                {"role": "assistant", "content": [{"type": "text", "text": "答复"}]},
                {"role": "user", "content": [{"type": "text", "text": "第二问"}]},
                {"role": "assistant", "content": [{"type": "text", "text": "继续答"}]},
                {"role": "user", "content": [{"type": "text", "text": "第三问"}]},
            ]),
            _make_payload([
                {"role": "user", "content": [{"type": "text", "text": "别的会话"}]},
            ]),
        ]

        for index, payload in enumerate(payloads, start=1):
            conn.execute(
                "INSERT INTO samples (id, raw_json, num_turns, processing_status) VALUES (?, ?, ?, 'pending')",
                [index, json.dumps(payload, ensure_ascii=False), 3],
            )
        conn.close()

        summary = run_session_merge(db_path, dry_run=False, min_prefix_turns=2, batch_size=2, workers=2)

        assert summary["total_samples"] == 3
        assert summary["keep_count"] == 2
        assert summary["merged_count"] == 1

        conn = duckdb.connect(str(db_path), read_only=True)
        rows = conn.execute(
            "SELECT id, session_merge_status, session_merge_keep, session_merge_representative_id, session_merge_reason FROM samples ORDER BY id"
        ).fetchall()
        conn.close()

        assert rows[0] == (1, "merged", False, 2, "strict_prefix_of_longer_sequence")
        assert rows[1] == (2, "keep", True, 2, "leaf_sequence")
        assert rows[2] == (3, "keep", True, 3, "singleton_group")