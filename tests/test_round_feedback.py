# Sample conversation data for testing
SAMPLE_MESSAGES = [
    {"role": "system", "content": "You are a helpful assistant."},
    {"role": "user", "content": "What's the weather in Beijing?"},
    {"role": "assistant", "content": "Let me check...", "tool_calls": [{"type": "function", "function": {"name": "web_search", "arguments": "{}"}}]},
    {"role": "tool", "content": '{"result": "sunny, 25C"}'},
    {"role": "assistant", "content": "Beijing is sunny today, 25 degrees."},
    {"role": "user", "content": "Thanks!"},
    {"role": "assistant", "content": "You're welcome!"},
]

def test_extract_turns():
    """Test extracting turns from messages"""
    from claw_data_filter.processors.round_feedback import TurnContextBuilder

    builder = TurnContextBuilder()
    turns = builder.extract_turns(SAMPLE_MESSAGES)
    # Should have 3 turns (3 assistant messages after removing system)
    assert len(turns) == 3

def test_extract_turns_no_system():
    """Test that system messages are skipped"""
    from claw_data_filter.processors.round_feedback import TurnContextBuilder

    builder = TurnContextBuilder()
    turns = builder.extract_turns(SAMPLE_MESSAGES)
    # First turn should not include system message
    assert "You are a helpful assistant" not in turns[0].user_message

def test_turn_has_tool_calls():
    """Test that tool calls are extracted"""
    from claw_data_filter.processors.round_feedback import TurnContextBuilder

    builder = TurnContextBuilder()
    turns = builder.extract_turns(SAMPLE_MESSAGES)
    # Turn 0 (weather check) should have tool calls
    turn_with_tool = turns[0]  # "Let me check..." turn
    assert len(turn_with_tool.tool_calls) == 1
    assert turn_with_tool.tool_calls[0]["name"] == "web_search"

def test_signal_users_extraction():
    """Test that signal users are extracted for each turn"""
    from claw_data_filter.processors.round_feedback import TurnContextBuilder

    builder = TurnContextBuilder()
    turns = builder.extract_turns(SAMPLE_MESSAGES)
    # Turn 1 (answer about weather) should have "Thanks!" as signal
    # Signal users are from subsequent turns
    assert "Thanks!" in turns[1].signal_users

def test_build_group1_prompt():
    """Test building Group1 prompt"""
    from claw_data_filter.processors.round_feedback import TurnContextBuilder

    builder = TurnContextBuilder()
    turns = builder.extract_turns(SAMPLE_MESSAGES)
    prompt = builder.build_group1_prompt(turns[1], turns)
    assert "=== 当前轮 ===" in prompt
    assert "=== 历史对话（仅user/assistant）===" in prompt
    assert "need_tool:" in prompt
    assert "tool_correct:" in prompt

def test_build_group2_prompt():
    """Test building Group2 prompt"""
    from claw_data_filter.processors.round_feedback import TurnContextBuilder

    builder = TurnContextBuilder()
    turns = builder.extract_turns(SAMPLE_MESSAGES)
    prompt = builder.build_group2_prompt(turns[2], turns)  # Turn with "Thanks!" signal
    assert "=== 当前轮 ===" in prompt
    assert "=== 后续用户信号" in prompt
    assert "response_helpful:" in prompt
    assert "user_satisfied:" in prompt