"""Tests for detail-page view-model assembly."""

from claw_data_filter.models.round_judgment import AssistantResponseJudgment, FeedbackKind, UserEpisodeJudgment
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
        "expected_judgment_count": 5,
        "expected_response_judgment_count": 3,
        "expected_episode_judgment_count": 2,
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
    response_judgments = [
        AssistantResponseJudgment(
            sample_uid="uid-3",
            response_index=0,
            episode_index=0,
            assistant_message_index=1,
            feedback_kind=FeedbackKind.TOOL_RESULT,
            feedback_message_start_index=2,
            feedback_message_end_index=2,
            feedback_payload=["晴天 25 度"],
            response_helpful="yes",
        ),
        AssistantResponseJudgment(
            sample_uid="uid-3",
            response_index=1,
            episode_index=0,
            assistant_message_index=3,
            feedback_kind=FeedbackKind.USER,
            feedback_message_start_index=4,
            feedback_message_end_index=4,
            feedback_payload=["谢谢"],
            response_helpful="yes",
        ),
        AssistantResponseJudgment(
            sample_uid="uid-3",
            response_index=2,
            episode_index=1,
            assistant_message_index=5,
            feedback_kind=FeedbackKind.NONE,
            response_helpful="uncertain",
        ),
    ]
    episode_judgments = [
        UserEpisodeJudgment(
            sample_uid="uid-3",
            episode_index=0,
            start_user_message_index=0,
            end_before_user_message_index=3,
            signal_from_users=["谢谢"],
            user_satisfied="yes",
        ),
        UserEpisodeJudgment(
            sample_uid="uid-3",
            episode_index=1,
            start_user_message_index=4,
            end_before_user_message_index=5,
            signal_from_users=[],
            user_satisfied="uncertain",
        ),
    ]

    detail = build_sample_detail_view(sample_record, response_judgments, episode_judgments)

    assert detail.sample_id == 3
    assert detail.sample_uid == "uid-3"
    assert detail.empty_response is False
    assert detail.session_merge_status == "keep"
    assert detail.session_merge_reason == "leaf_sequence"
    assert len(detail.response_steps) == 3
    assert len(detail.user_episodes) == 2
    assert detail.response_steps[0].tool_calls[0]["name"] == "weather"
    assert detail.response_steps[0].feedback_kind == "tool_result"
    assert detail.response_steps[0].feedback_payload == ["晴天 25 度"]
    assert detail.response_steps[1].assistant_message == "北京今天晴天 25 度"
    assert detail.response_steps[2].assistant_message == "不客气"
    assert detail.user_episodes[0].signal_from_users == ["谢谢"]
    assert detail.user_episodes[1].assistant_messages == ["不客气"]


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
        "expected_judgment_count": 2,
        "expected_response_judgment_count": 1,
        "expected_episode_judgment_count": 1,
        "num_tool_calls": 0,
        "tool_stats": {},
        "processing_status": "pending",
    }

    detail = build_sample_detail_view(sample_record, [], [])

    assert len(detail.response_steps) == 1
    assert len(detail.user_episodes) == 1
    assert detail.sample_uid == "uid-8"
    assert detail.empty_response is True
    assert detail.session_merge_status is None
    assert detail.response_steps[0].response_helpful is None
    assert detail.user_episodes[0].user_satisfied is None
    assert detail.response_steps[0].llm_error is False