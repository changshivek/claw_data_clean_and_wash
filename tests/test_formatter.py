"""Tests for conversation formatter."""
from claw_data_filter.processors.formatter import ConversationFormatter


def test_format_strips_system_prompt():
    """Test that system prompt is stripped."""
    formatter = ConversationFormatter()

    raw = {
        "messages": [
            {"role": "system", "content": "You are a very long system prompt with detailed instructions..."},
            {"role": "user", "content": "What's the weather in SF?"},
            {"role": "assistant", "content": "Let me check."},
        ]
    }

    formatted = formatter.format(raw)

    # Should not contain system prompt
    assert "system prompt" not in formatted.lower()
    assert "What's the weather in SF?" in formatted
    print("test_format_strips_system_prompt passed")


def test_format_preserves_user_query():
    """Test that user query is preserved."""
    formatter = ConversationFormatter()

    raw = {
        "messages": [
            {"role": "system", "content": "System"},
            {"role": "user", "content": "What's the weather?"},
            {"role": "assistant", "content": "I'll check."},
        ]
    }

    formatted = formatter.format(raw)
    assert "What's the weather?" in formatted
    assert "User:" in formatted
    print("test_format_preserves_user_query passed")


def test_format_tool_calls():
    """Test that tool calls are formatted nicely."""
    formatter = ConversationFormatter()

    raw = {
        "messages": [
            {"role": "user", "content": "Get weather"},
            {"role": "assistant", "content": "Let me check", "tool_calls": [
                {"id": "call_1", "type": "function", "function": {"name": "get_weather", "arguments": '{"city": "San Francisco"}'}}
            ]},
            {"role": "tool", "content": '{"temp": 72}', "tool_call_id": "call_1"},
            {"role": "assistant", "content": "It's 72°F."},
        ]
    }

    formatted = formatter.format(raw)

    assert "get_weather" in formatted
    assert "San Francisco" in formatted
    assert "Tool Result" in formatted or "[Result]" in formatted
    print("test_format_tool_calls passed")
    print("Formatted output:\n" + formatted)


def test_format_empty_conversation():
    """Test formatting empty conversation."""
    formatter = ConversationFormatter()

    raw = {"messages": []}
    formatted = formatter.format(raw)
    assert formatted == ""
    print("test_format_empty passed")


if __name__ == "__main__":
    test_format_strips_system_prompt()
    test_format_preserves_user_query()
    test_format_tool_calls()
    test_format_empty_conversation()
    print("All formatter tests passed!")
