# Sample conversation data for testing
import pytest
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


@pytest.mark.asyncio
async def test_judge_group1_success():
    """Test Group1 judgment returns parsed result"""
    from unittest.mock import AsyncMock, patch
    from claw_data_filter.processors.round_feedback import RoundJudgmentProcessor
    from claw_data_filter.llm.async_client import AsyncLLMClient

    mock_llm = AsyncMock(spec=AsyncLLMClient)
    mock_llm.chat = AsyncMock(return_value="need_tool=yes; tool_correct=yes")

    processor = RoundJudgmentProcessor(mock_llm)
    result = await processor.judge_group1("mock prompt")

    assert result["need_tool"] == "yes"
    assert result["tool_correct"] == "yes"

@pytest.mark.asyncio
async def test_judge_group2_success():
    """Test Group2 judgment returns parsed result"""
    from unittest.mock import AsyncMock
    from claw_data_filter.processors.round_feedback import RoundJudgmentProcessor
    from claw_data_filter.llm.async_client import AsyncLLMClient

    mock_llm = AsyncMock(spec=AsyncLLMClient)
    mock_llm.chat = AsyncMock(return_value="response_helpful=yes; user_satisfied=no")

    processor = RoundJudgmentProcessor(mock_llm)
    result = await processor.judge_group2("mock prompt")

    assert result["response_helpful"] == "yes"
    assert result["user_satisfied"] == "no"

def test_parse_group1_response():
    """Test Group1 response parsing"""
    from unittest.mock import AsyncMock
    from claw_data_filter.processors.round_feedback import RoundJudgmentProcessor
    from claw_data_filter.llm.async_client import AsyncLLMClient

    mock_llm = AsyncMock(spec=AsyncLLMClient)
    processor = RoundJudgmentProcessor(mock_llm)

    result = processor._parse_group1_response("need_tool=yes; tool_correct=no")
    assert result["need_tool"] == "yes"
    assert result["tool_correct"] == "no"

def test_parse_group1_response_uncertain():
    """Test Group1 response with uncertain value"""
    from unittest.mock import AsyncMock
    from claw_data_filter.processors.round_feedback import RoundJudgmentProcessor
    from claw_data_filter.llm.async_client import AsyncLLMClient

    mock_llm = AsyncMock(spec=AsyncLLMClient)
    processor = RoundJudgmentProcessor(mock_llm)

    result = processor._parse_group1_response("need_tool=uncertain; tool_correct=yes")
    assert result["need_tool"] == "uncertain"
    assert result["tool_correct"] == "yes"

def test_parse_group2_response():
    """Test Group2 response parsing"""
    from unittest.mock import AsyncMock
    from claw_data_filter.processors.round_feedback import RoundJudgmentProcessor
    from claw_data_filter.llm.async_client import AsyncLLMClient

    mock_llm = AsyncMock(spec=AsyncLLMClient)
    processor = RoundJudgmentProcessor(mock_llm)

    result = processor._parse_group2_response("response_helpful=no; user_satisfied=yes")
    assert result["response_helpful"] == "no"
    assert result["user_satisfied"] == "yes"

def test_parse_response_invalid():
    """Test invalid response returns None"""
    from unittest.mock import AsyncMock
    from claw_data_filter.processors.round_feedback import RoundJudgmentProcessor
    from claw_data_filter.llm.async_client import AsyncLLMClient

    mock_llm = AsyncMock(spec=AsyncLLMClient)
    processor = RoundJudgmentProcessor(mock_llm)

    result = processor._parse_group1_response("invalid response format")
    assert result is None

def test_tool_stats_aggregator():
    """Test ToolStatsAggregator aggregates correctly"""
    from claw_data_filter.processors.round_feedback import ToolStatsAggregator
    from claw_data_filter.models.round_judgment import RoundJudgment

    aggregator = ToolStatsAggregator()

    judgments = [
        RoundJudgment(sample_id=1, turn_index=0, need_tool="yes", tool_correct="yes", llm_error=False),
        RoundJudgment(sample_id=1, turn_index=1, need_tool="yes", tool_correct="no", llm_error=False),
        RoundJudgment(sample_id=1, turn_index=2, need_tool="no", llm_error=False),
    ]

    stats = aggregator.aggregate(judgments)

    assert stats["tool_used"] == 2  # 2 turns with need_tool=yes
    assert stats["tool_success"] == 1  # 1 turn with tool_correct=yes


@pytest.mark.asyncio
async def test_pressure_test_initialization():
    """Test PressureTest can be initialized"""
    from unittest.mock import AsyncMock
    from claw_data_filter.processors.round_feedback import PressureTest
    from claw_data_filter.llm.async_client import AsyncLLMClient

    mock_llm = AsyncMock(spec=AsyncLLMClient)
    pt = PressureTest(mock_llm)
    assert pt.llm is mock_llm


# Real conversation sample for integration test
REAL_CONVERSATION = {
    "request": {
        "bodyJson": {
            "messages": [
                {"role": "user", "content": "Hello"},
                {"role": "assistant", "content": "Hi! How can I help you?"},
                {"role": "user", "content": "What's the weather in Beijing?"},
                {"role": "assistant", "content": "Let me check...", "tool_calls": [{"function": {"name": "web_search", "arguments": "{}"}}]},
                {"role": "tool", "content": "Sunny, 25C"},
                {"role": "assistant", "content": "Beijing is sunny today, 25 degrees."},
                {"role": "user", "content": "Thanks!"},
                {"role": "assistant", "content": "You're welcome!"},
            ]
        }
    }
}

def test_end_to_end_turn_extraction():
    """Test full flow from raw messages to turns"""
    from claw_data_filter.processors.round_feedback import TurnContextBuilder

    builder = TurnContextBuilder()
    turns = builder.extract_turns(REAL_CONVERSATION["request"]["bodyJson"]["messages"])

    # Should have 4 turns (4 assistant messages)
    assert len(turns) == 4

    # Check turn 1 has the tool_call (the assistant "Let me check..." turn)
    # Note: Due to how consecutive assistants are handled, this turn has empty user_message
    assert turns[1].tool_calls  # Has tool call
    assert turns[1].assistant_message == "Let me check..."

    # Check turn 2 (the weather answer with user message "What's the weather...")
    # The user_message is correctly associated here
    assert turns[2].user_message == "What's the weather in Beijing?"
    assert turns[2].assistant_message == "Beijing is sunny today, 25 degrees."

    # Check signal users for turn 2
    # After turn 2 (answer about weather), user says "Thanks!" which is signal
    assert "Thanks!" in turns[2].signal_users

def test_tool_stats_aggregation_integration():
    """Test tool stats aggregation from judgments"""
    from claw_data_filter.processors.round_feedback import ToolStatsAggregator
    from claw_data_filter.models.round_judgment import RoundJudgment

    aggregator = ToolStatsAggregator()

    # Simulate judgments as they would come from processing
    judgments = [
        RoundJudgment(sample_id=1, turn_index=0, need_tool="no", response_helpful="yes", user_satisfied="yes", llm_error=False),
        RoundJudgment(sample_id=1, turn_index=1, need_tool="yes", tool_correct="yes", response_helpful="yes", user_satisfied="yes", llm_error=False),
        RoundJudgment(sample_id=1, turn_index=2, need_tool="yes", tool_correct="no", response_helpful="yes", user_satisfied="no", llm_error=False),
        RoundJudgment(sample_id=1, turn_index=3, need_tool="no", response_helpful="yes", user_satisfied="yes", llm_error=False),
    ]

    stats = aggregator.aggregate(judgments)

    assert stats["tool_used"] == 2  # 2 turns with need_tool=yes
    assert stats["tool_success"] == 1  # 1 turn with tool_correct=yes
    assert stats["partial"] is False

def test_empty_messages_handling():
    """Test handling of empty messages"""
    from claw_data_filter.processors.round_feedback import TurnContextBuilder

    builder = TurnContextBuilder()
    turns = builder.extract_turns([])
    assert len(turns) == 0

def test_single_user_message():
    """Test handling of single user message (no assistant)"""
    from claw_data_filter.processors.round_feedback import TurnContextBuilder

    builder = TurnContextBuilder()
    turns = builder.extract_turns([{"role": "user", "content": "Hello"}])
    # Single user with no assistant response should not create a turn
    assert len(turns) == 0