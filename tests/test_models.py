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
    # Valid scores 0-5 are accepted as-is
    for score in (0, 1, 2, 3, 4, 5):
        e = Evaluation(sample_id=1, progress_score=score, task_type="coding",
                      tool_quality_score=1.0, tool_success_rate=1.0, overall_score=10.0)
        assert e.progress_score == score

    # Out-of-range scores are clamped
    e_low = Evaluation(sample_id=1, progress_score=-1, task_type="coding",
                       tool_quality_score=1.0, tool_success_rate=1.0, overall_score=10.0)
    assert e_low.progress_score == 0

    e_high = Evaluation(sample_id=1, progress_score=10, task_type="coding",
                       tool_quality_score=1.0, tool_success_rate=1.0, overall_score=10.0)
    assert e_high.progress_score == 5

    print("test_evaluation_progress_validation passed")


def test_sample_detect_anthropic_format():
    """Test Sample.from_dict detects Anthropic format"""
    from claw_data_filter.models.sample import Sample
    anthropic_data = {
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": "hello"}]},
            {"role": "assistant", "content": "hi"},
        ]
    }
    s = Sample.from_dict(anthropic_data)
    assert s.num_turns == 1


def test_sample_detect_openai_format():
    """Test Sample.from_dict detects OpenAI format"""
    from claw_data_filter.models.sample import Sample
    openai_data = {
        "messages": [
            {"role": "user", "content": "hello"},
            {"role": "assistant", "content": "hi"},
        ]
    }
    s = Sample.from_dict(openai_data)
    assert s.num_turns == 1


def test_sample_anthropic_to_openai_conversion():
    """Test Anthropic format with tool_result is converted to OpenAI"""
    from claw_data_filter.models.sample import Sample
    anthropic_data = {
        "messages": [
            {"role": "assistant", "content": [{"type": "text", "text": "Let me help"}]},
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "call123", "content": "file content"},
                {"type": "text", "text": "Thanks"}
            ]},
        ]
    }
    s = Sample.from_dict(anthropic_data)
    # After conversion, should have separate tool message with correct tool_call_id
    # The tool_result should become a separate tool role message
    assert s.num_tool_calls == 1, f"Expected 1 tool call, got {s.num_tool_calls}"
    assert s.user_query == "Thanks", f"Expected 'Thanks', got '{s.user_query}'"


def test_sample_anthropic_assistant_tool_use_conversion():
    """Test Anthropic assistant with tool_use blocks is converted to OpenAI"""
    from claw_data_filter.models.sample import Sample
    import json

    anthropic_data = {
        "messages": [
            {"role": "user", "content": "What files are there?"},
            {"role": "assistant", "content": [
                {"type": "text", "text": "Let me check..."},
                {"type": "tool_use", "id": "call123", "name": "bash", "input": {"cmd": "ls"}}
            ]},
        ]
    }
    s = Sample.from_dict(anthropic_data)
    # Verify conversion happened
    assert s.num_tool_calls >= 1, f"Expected >= 1 tool call, got {s.num_tool_calls}"


if __name__ == "__main__":
    test_sample_from_dict_basic()
    test_sample_from_dict_with_tool_calls()
    test_sample_from_dict_with_error()
    test_sample_from_dict_multiple_turns()
    test_evaluation_model_basic()
    test_evaluation_progress_validation()
    test_sample_detect_anthropic_format()
    test_sample_detect_openai_format()
    test_sample_anthropic_to_openai_conversion()
    test_sample_anthropic_assistant_tool_use_conversion()
    print("All model tests passed!")