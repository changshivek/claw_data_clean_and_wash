import json

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
    # Tool-call assistant and final answer are grouped into one turn.
    assert len(turns) == 2

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
    # Turn 0 (weather question) should include the tool call in the same turn.
    turn_with_tool = turns[0]
    assert len(turn_with_tool.tool_calls) == 1
    assert turn_with_tool.tool_calls[0]["name"] == "web_search"
    assert "Beijing is sunny today" in turn_with_tool.assistant_message

def test_signal_users_extraction():
    """Test that signal users are extracted for each turn"""
    from claw_data_filter.processors.round_feedback import TurnContextBuilder

    builder = TurnContextBuilder()
    turns = builder.extract_turns(SAMPLE_MESSAGES)
    # Turn 0 (answer about weather) should have "Thanks!" as signal.
    assert "Thanks!" in turns[0].signal_users

def test_build_judgment_prompt():
    """Test building simplified judgment prompt"""
    from claw_data_filter.processors.round_feedback import TurnContextBuilder

    builder = TurnContextBuilder()
    turns = builder.extract_turns(SAMPLE_MESSAGES)
    prompt = builder.build_judgment_prompt(turns[0], turns)
    assert "=== 当前轮 ===" in prompt
    assert "=== 后续用户信号" in prompt
    assert "response_helpful:" in prompt
    assert "user_satisfied:" in prompt


@pytest.mark.asyncio
async def test_judge_success():
    """Test judgment returns parsed result"""
    from unittest.mock import AsyncMock
    from claw_data_filter.processors.round_feedback import RoundJudgmentProcessor
    from claw_data_filter.llm.async_client import AsyncLLMClient

    mock_llm = AsyncMock(spec=AsyncLLMClient)
    mock_llm.chat = AsyncMock(return_value="response_helpful=yes; user_satisfied=no")

    processor = RoundJudgmentProcessor(mock_llm)
    result = await processor.judge("mock prompt")

    assert result["response_helpful"] == "yes"
    assert result["user_satisfied"] == "no"

def test_parse_response():
    """Test simplified response parsing"""
    from unittest.mock import AsyncMock
    from claw_data_filter.processors.round_feedback import RoundJudgmentProcessor
    from claw_data_filter.llm.async_client import AsyncLLMClient

    mock_llm = AsyncMock(spec=AsyncLLMClient)
    processor = RoundJudgmentProcessor(mock_llm)

    result = processor._parse_response("response_helpful=yes; user_satisfied=no")
    assert result["response_helpful"] == "yes"
    assert result["user_satisfied"] == "no"

def test_parse_response_uncertain():
    """Test response parsing with uncertain value"""
    from unittest.mock import AsyncMock
    from claw_data_filter.processors.round_feedback import RoundJudgmentProcessor
    from claw_data_filter.llm.async_client import AsyncLLMClient

    mock_llm = AsyncMock(spec=AsyncLLMClient)
    processor = RoundJudgmentProcessor(mock_llm)

    result = processor._parse_response("response_helpful=uncertain; user_satisfied=yes")
    assert result["response_helpful"] == "uncertain"
    assert result["user_satisfied"] == "yes"

def test_parse_response_neutral():
    """Test response parsing with neutral satisfaction"""
    from unittest.mock import AsyncMock
    from claw_data_filter.processors.round_feedback import RoundJudgmentProcessor
    from claw_data_filter.llm.async_client import AsyncLLMClient

    mock_llm = AsyncMock(spec=AsyncLLMClient)
    processor = RoundJudgmentProcessor(mock_llm)

    result = processor._parse_response("response_helpful=yes; user_satisfied=neutral")
    assert result["response_helpful"] == "yes"
    assert result["user_satisfied"] == "neutral"

def test_parse_response_invalid():
    """Test invalid response returns None"""
    from unittest.mock import AsyncMock
    from claw_data_filter.processors.round_feedback import RoundJudgmentProcessor
    from claw_data_filter.llm.async_client import AsyncLLMClient

    mock_llm = AsyncMock(spec=AsyncLLMClient)
    processor = RoundJudgmentProcessor(mock_llm)

    result = processor._parse_response("invalid response format")
    assert result is None

def test_tool_stats_aggregator():
    """Test ToolStatsAggregator aggregates correctly for simplified judgments"""
    from claw_data_filter.processors.round_feedback import ToolStatsAggregator
    from claw_data_filter.models.round_judgment import RoundJudgment

    aggregator = ToolStatsAggregator()

    judgments = [
        RoundJudgment(sample_id=1, turn_index=0, response_helpful="yes", user_satisfied="yes", llm_error=False),
        RoundJudgment(sample_id=1, turn_index=1, response_helpful="yes", user_satisfied="no", llm_error=False),
        RoundJudgment(sample_id=1, turn_index=2, response_helpful="no", user_satisfied="yes", llm_error=False),
    ]

    stats = aggregator.aggregate(judgments)

    assert stats["response_helpful_rate"] == 2/3  # 2 out of 3 are helpful
    assert stats["user_satisfied_rate"] == 2/3  # 2 out of 3 are satisfied
    assert stats["total_turns"] == 3
    assert stats["has_error"] is False


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

    # Consecutive assistant/tool/assistant messages are grouped into one judged turn.
    assert len(turns) == 3

    # Check turn 1 keeps the weather question and merged assistant/tool response.
    assert turns[1].tool_calls
    assert turns[1].user_message == "What's the weather in Beijing?"
    assert "Let me check..." in turns[1].assistant_message
    assert "Beijing is sunny today, 25 degrees." in turns[1].assistant_message

    # Check signal users for the weather turn.
    assert "Thanks!" in turns[1].signal_users

def test_tool_stats_aggregation_integration():
    """Test tool stats aggregation from judgments"""
    from claw_data_filter.processors.round_feedback import ToolStatsAggregator
    from claw_data_filter.models.round_judgment import RoundJudgment

    aggregator = ToolStatsAggregator()

    # Simulate judgments as they would come from processing
    judgments = [
        RoundJudgment(sample_id=1, turn_index=0, response_helpful="yes", user_satisfied="yes", llm_error=False),
        RoundJudgment(sample_id=1, turn_index=1, response_helpful="yes", user_satisfied="yes", llm_error=False),
        RoundJudgment(sample_id=1, turn_index=2, response_helpful="yes", user_satisfied="no", llm_error=False),
        RoundJudgment(sample_id=1, turn_index=3, response_helpful="yes", user_satisfied="yes", llm_error=False),
    ]

    stats = aggregator.aggregate(judgments)

    assert stats["response_helpful_rate"] == 1.0  # All 4 are helpful
    assert stats["user_satisfied_rate"] == 0.75  # 3 out of 4 are satisfied
    assert stats["total_turns"] == 4
    assert stats["has_error"] is False

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


def test_parse_simplified_response():
    """Test parsing simplified response with only 2 judgments"""
    from claw_data_filter.processors.round_feedback import RoundJudgmentProcessor

    class MockLLM:
        async def chat(self, messages, max_tokens=50):
            return "response_helpful=yes; user_satisfied=no"

    processor = RoundJudgmentProcessor(MockLLM())
    result = processor._parse_response("response_helpful=yes; user_satisfied=no")
    assert result == {"response_helpful": "yes", "user_satisfied": "no"}


@pytest.mark.asyncio
async def test_process_sample_marks_unirouter_sample_complete(tmp_path):
    """Test process_sample handles UniRouter payload and writes full results atomically."""
    from claw_data_filter.processors.round_feedback import RoundFeedbackProcessor
    from claw_data_filter.storage.duckdb_store import DuckDBStore

    class MockLLM:
        async def chat(self, messages, max_tokens=50):
            return "response_helpful=yes; user_satisfied=yes"

    store = DuckDBStore(tmp_path / "round_feedback.duckdb")
    raw_json = {
        "request": {
            "bodyJson": {
                "messages": [
                    {"role": "user", "content": "Hi"},
                    {"role": "assistant", "content": "Hello"},
                    {"role": "tool", "content": "tool ok"},
                    {"role": "assistant", "content": "Anything else?"},
                ]
            }
        }
    }

    sample_id = store.insert_sample(__import__("claw_data_filter.models.sample", fromlist=["Sample"]).Sample.from_dict(raw_json))
    processor = RoundFeedbackProcessor(store, MockLLM(), max_concurrency=2)

    judgments = await processor.process_sample(sample_id, raw_json)

    assert len(judgments) == 1
    persisted = store.get_turn_judgments(sample_id)
    assert len(persisted) == 1
    row = store.conn.execute(
        "SELECT expected_judgment_count, tool_stats FROM samples WHERE id = ?",
        [sample_id],
    ).fetchone()
    assert row[0] == 1
    assert json.loads(row[1])["response_helpful_rate"] == 1.0
    store.close()


def test_count_expected_turns_matches_extraction():
    """Test expected turn counting matches actual extracted turns."""
    from claw_data_filter.processors.round_feedback import TurnContextBuilder

    builder = TurnContextBuilder()
    turns = builder.extract_turns(REAL_CONVERSATION["request"]["bodyJson"]["messages"])

    assert builder.count_expected_turns(REAL_CONVERSATION["request"]["bodyJson"]["messages"]) == len(turns)