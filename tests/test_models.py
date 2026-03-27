"""Tests for data models."""
import json
from claw_data_filter.models.sample import Sample
from claw_data_filter.models.evaluation import Evaluation


def test_sample_from_dict_basic():
    """Test parsing a simple conversation."""
    raw = {
        "messages": [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": "What's the weather?"},
            {"role": "assistant", "content": "Let me check the weather for you."},
        ]
    }
    sample = Sample.from_dict(raw)
    assert sample.user_query == "What's the weather?"
    assert sample.num_turns == 1
    assert sample.num_tool_calls == 0
    assert sample.has_error is False
    print("test_sample_from_dict_basic passed")


def test_sample_from_dict_with_tool_calls():
    """Test parsing conversation with tool calls."""
    raw = {
        "messages": [
            {"role": "user", "content": "What's the weather in SF?"},
            {"role": "assistant", "content": "Let me check...", "tool_calls": [
                {"id": "call_1", "type": "function", "function": {"name": "get_weather", "arguments": "{}"}}
            ]},
            {"role": "tool", "content": '{"temp": 72}', "tool_call_id": "call_1"},
            {"role": "assistant", "content": "It's 72°F in San Francisco."},
        ]
    }
    sample = Sample.from_dict(raw)
    assert sample.user_query == "What's the weather in SF?"
    assert sample.num_turns == 1
    assert sample.num_tool_calls == 1
    assert sample.has_error is False
    print("test_sample_from_dict_with_tool_calls passed")


def test_sample_from_dict_with_error():
    """Test detecting error in tool result."""
    raw = {
        "messages": [
            {"role": "user", "content": "Do something"},
            {"role": "assistant", "content": "I'll try...", "tool_calls": [
                {"id": "call_1", "type": "function", "function": {"name": "do_it", "arguments": "{}"}}
            ]},
            {"role": "tool", "content": '{"error": "Permission denied"}', "tool_call_id": "call_1"},
            {"role": "assistant", "content": "Got an error."},
        ]
    }
    sample = Sample.from_dict(raw)
    assert sample.has_error is True
    print("test_sample_from_dict_with_error passed")


def test_sample_from_dict_multiple_turns():
    """Test counting multiple user turns."""
    raw = {
        "messages": [
            {"role": "user", "content": "First question?"},
            {"role": "assistant", "content": "Answer 1"},
            {"role": "user", "content": "Follow-up question?"},
            {"role": "assistant", "content": "Answer 2"},
        ]
    }
    sample = Sample.from_dict(raw)
    assert sample.num_turns == 2
    print("test_sample_from_dict_multiple_turns passed")


def test_evaluation_model_basic():
    """Test creating an evaluation."""
    eval = Evaluation(
        sample_id=1,
        task_type="information_retrieval",
        progress_score=4,
        tool_quality_score=0.9,
        tool_success_rate=1.0,
        overall_score=8.5,
        reasoning="Good work"
    )
    assert eval.progress_score == 4
    assert eval.tool_quality_score == 0.9
    print("test_evaluation_model_basic passed")


def test_evaluation_progress_validation():
    """Test progress_score validation."""
    # Valid scores
    for score in (0, 1, 2, 4, 5):
        e = Evaluation(sample_id=1, progress_score=score, task_type="coding",
                      tool_quality_score=1.0, tool_success_rate=1.0, overall_score=10.0)
        assert e.progress_score == score

    # Invalid score (3 is reserved)
    try:
        Evaluation(sample_id=1, progress_score=3, task_type="coding",
                  tool_quality_score=1.0, tool_success_rate=1.0, overall_score=10.0)
        assert False, "Should have raised ValueError"
    except ValueError:
        pass
    print("test_evaluation_progress_validation passed")


if __name__ == "__main__":
    test_sample_from_dict_basic()
    test_sample_from_dict_with_tool_calls()
    test_sample_from_dict_with_error()
    test_sample_from_dict_multiple_turns()
    test_evaluation_model_basic()
    test_evaluation_progress_validation()
    print("All model tests passed!")