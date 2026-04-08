"""Tests for detail-page view-model assembly."""

from claw_data_filter.models.round_judgment import RoundJudgment
from claw_data_filter.web.services.detail_builder import build_sample_detail_view


def test_build_sample_detail_view_uses_turn_context_grouping():
    sample_record = {
        "id": 3,
        "sample_uid": "uid-3",
        "raw_json": {
            "request": {
                "bodyJson": {
                    "messages": [
                        {"role": "user", "content": "查一下北京天气"},
                        {"role": "assistant", "content": "我来查一下", "tool_calls": [{"function": {"name": "weather"}}]},
                        {"role": "tool", "content": "晴天 25 度"},
                        {"role": "assistant", "content": "北京今天晴天 25 度"},
                        {"role": "user", "content": "谢谢"},
                        {"role": "assistant", "content": "不客气"},
                    ]
                }
            }
        },
        "num_turns": 2,
        "expected_judgment_count": 2,
        "num_tool_calls": 1,
        "tool_stats": {"response_helpful_rate": 1.0, "user_satisfied_rate": 0.5},
        "session_merge_status": "keep",
        "session_merge_keep": True,
        "session_merge_group_id": "group-1",
        "session_merge_group_size": 4,
        "session_merge_representative_id": 3,
        "session_merge_reason": "leaf_sequence",
        "processing_status": "completed",
    }
    judgments = [
        RoundJudgment(
            sample_id=3,
            turn_index=0,
            response_helpful="yes",
            user_satisfied="yes",
            signal_from_users=["谢谢"],
        ),
        RoundJudgment(
            sample_id=3,
            turn_index=1,
            response_helpful="yes",
            user_satisfied="uncertain",
        ),
    ]

    detail = build_sample_detail_view(sample_record, judgments)

    assert detail.sample_id == 3
    assert detail.sample_uid == "uid-3"
    assert detail.empty_response is False
    assert detail.session_merge_status == "keep"
    assert detail.session_merge_reason == "leaf_sequence"
    assert len(detail.turns) == 2
    assert detail.turns[0].turn_index == 0
    assert detail.turns[0].tool_calls[0]["name"] == "weather"
    assert "晴天 25 度" in (detail.turns[0].tool_result or "")
    assert detail.turns[0].signal_from_users == ["谢谢"]
    assert detail.turns[1].assistant_message == "不客气"


def test_build_sample_detail_view_handles_missing_judgment():
    sample_record = {
        "id": 8,
        "sample_uid": "uid-8",
        "empty_response": True,
        "raw_json": {
            "messages": [
                {"role": "user", "content": "hello"},
                {"role": "assistant", "content": "hi"},
            ]
        },
        "num_turns": 1,
        "expected_judgment_count": 1,
        "num_tool_calls": 0,
        "tool_stats": {},
        "processing_status": "pending",
    }

    detail = build_sample_detail_view(sample_record, [])

    assert len(detail.turns) == 1
    assert detail.sample_uid == "uid-8"
    assert detail.empty_response is True
    assert detail.session_merge_status is None
    assert detail.turns[0].response_helpful is None
    assert detail.turns[0].user_satisfied is None
    assert detail.turns[0].llm_error is False