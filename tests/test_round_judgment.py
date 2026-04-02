import pytest
from claw_data_filter.models.round_judgment import RoundJudgment, JudgmentValue

def test_round_judgment_creation():
    judgment = RoundJudgment(
        sample_id=1,
        turn_index=0,
        response_helpful="yes",
        user_satisfied="yes",
        signal_from_users=["谢谢"],
        llm_error=False,
    )
    assert judgment.llm_error is False

def test_judgment_value_enum():
    result = JudgmentValue.YES
    assert result.value == "yes"

def test_round_judgment_from_dict():
    data = {
        "sample_id": 1,
        "turn_index": 0,
        "response_helpful": "yes",
        "user_satisfied": "no",
        "signal_from_users": ["能具体说说吗？"],
        "llm_error": False,
    }
    judgment = RoundJudgment(**data)
    assert judgment.user_satisfied == "no"


def test_round_judgment_simplified():
    """Test RoundJudgment only has response_helpful and user_satisfied"""
    from claw_data_filter.models.round_judgment import RoundJudgment

    j = RoundJudgment(
        sample_id=1,
        turn_index=0,
        response_helpful="yes",
        user_satisfied="no",
        signal_from_users=["用户确认"],
        llm_error=False,
    )
    assert j.response_helpful == "yes"
    assert j.user_satisfied == "no"
    # These fields should not exist
    assert not hasattr(j, 'need_tool')
    assert not hasattr(j, 'tool_correct')