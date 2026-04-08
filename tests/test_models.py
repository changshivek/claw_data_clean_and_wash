"""Tests for data models."""
import json
from claw_data_filter.models.sample import Sample


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


def test_sample_from_unirouter_payload():
    """Test parsing UniRouter payload from request.bodyJson.messages."""
    raw = {
        "request": {
            "bodyJson": {
                "messages": [
                    {"role": "user", "content": "What is 2+2?"},
                    {"role": "assistant", "content": "2+2 is 4."},
                ]
            }
        }
    }

    sample = Sample.from_dict(raw)

    assert sample.user_query == "What is 2+2?"
    assert sample.assistant_response == "2+2 is 4."
    assert sample.num_turns == 1
    assert sample.expected_judgment_count == 1


def test_sample_num_turns_ignores_unanswered_user_messages():
    raw = {
        "messages": [
            {"role": "user", "content": "u1"},
            {"role": "user", "content": "u2"},
            {"role": "user", "content": "u3"},
            {"role": "assistant", "content": "a1"},
            {"role": "user", "content": "u4"},
        ]
    }

    sample = Sample.from_dict(raw)

    assert sample.num_turns == 1
    assert sample.expected_judgment_count == 1


def test_sample_expected_judgment_count_absorbs_tool_result_only_user_blocks():
    raw = {
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": "列一下目录"}]},
            {"role": "assistant", "content": [
                {"type": "text", "text": "我先看看。"},
                {"type": "tool_use", "id": "call_1", "name": "bash", "input": {"cmd": "ls"}},
            ]},
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "call_1", "content": "a.txt\nb.txt"},
            ]},
            {"role": "assistant", "content": [
                {"type": "text", "text": "目录里有 a.txt 和 b.txt。"},
            ]},
            {"role": "user", "content": [{"type": "text", "text": "继续看 a.txt"}]},
            {"role": "assistant", "content": "我来打开它。"},
        ]
    }

    sample = Sample.from_dict(raw)

    assert sample.num_turns == 2
    assert sample.expected_judgment_count == 2


if __name__ == "__main__":
    test_sample_from_dict_basic()
    test_sample_from_dict_with_tool_calls()
    test_sample_from_dict_with_error()
    test_sample_from_dict_multiple_turns()
    test_sample_detect_anthropic_format()
    test_sample_detect_openai_format()
    test_sample_anthropic_to_openai_conversion()
    test_sample_anthropic_assistant_tool_use_conversion()
    test_sample_from_unirouter_payload()
    print("All model tests passed!")