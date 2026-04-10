import pytest
from claw_data_filter.models.round_judgment import AssistantResponseJudgment, FeedbackKind, JudgmentValue, UserEpisodeJudgment

def test_judgment_value_enum():
    assert JudgmentValue.YES.value == "yes"
    assert JudgmentValue.NEUTRAL.value == "neutral"


def test_assistant_response_judgment_generates_stable_uid():
    judgment = AssistantResponseJudgment(
        sample_uid="sample-1",
        response_index=2,
        episode_index=0,
        assistant_message_index=5,
        feedback_kind=FeedbackKind.USER,
        feedback_payload=["谢谢"],
        response_progress="yes",
    )

    assert judgment.judgment_uid == "resp:sample-1:2"
    assert judgment.response_progress == "yes"


def test_user_episode_judgment_generates_stable_uid():
    judgment = UserEpisodeJudgment(
        sample_uid="sample-1",
        episode_index=3,
        start_user_message_index=8,
        end_before_user_message_index=10,
        signal_from_users=["继续"],
        user_satisfied="no",
    )

    assert judgment.judgment_uid == "episode:sample-1:3"
    assert judgment.user_satisfied == "no"


def test_feedback_kind_enum_values():
    assert FeedbackKind.TOOL_RESULT.value == "tool_result"
    assert FeedbackKind.USER.value == "user"
    assert FeedbackKind.NONE.value == "none"