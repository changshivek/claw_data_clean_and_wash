import json
from unittest.mock import AsyncMock

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

ANTHROPIC_TOOL_CHAIN_MESSAGES = [
    {"role": "user", "content": [{"type": "text", "text": "帮我看一下目录里有什么文件"}]},
    {"role": "assistant", "content": [
        {"type": "text", "text": "我先列一下文件。"},
        {"type": "tool_use", "id": "call_1", "name": "bash", "input": {"cmd": "ls"}},
    ]},
    {"role": "user", "content": [
        {"type": "tool_result", "tool_use_id": "call_1", "content": "a.txt\nb.txt"},
    ]},
    {"role": "assistant", "content": [
        {"type": "text", "text": "目录里有 a.txt 和 b.txt。"},
    ]},
    {"role": "user", "content": [{"type": "text", "text": "那 a.txt 里是什么？"}]},
    {"role": "assistant", "content": "我继续帮你看。"},
]


def test_extract_response_contexts_split_assistant_steps_by_feedback_block():
    from claw_data_filter.processors.round_feedback import TurnContextBuilder

    contexts = TurnContextBuilder().extract_response_contexts("sample-1", SAMPLE_MESSAGES)

    assert len(contexts) == 3
    assert contexts[0].feedback_kind.value == "tool_result"
    assert contexts[0].feedback_payload == ['{"result": "sunny, 25C"}']
    assert contexts[1].feedback_kind.value == "user"
    assert contexts[1].feedback_payload == ["Thanks!"]
    assert contexts[2].feedback_kind.value == "none"


def test_extract_episode_contexts_keep_user_episode_boundary():
    from claw_data_filter.processors.round_feedback import TurnContextBuilder

    contexts = TurnContextBuilder().extract_episode_contexts("sample-1", SAMPLE_MESSAGES)

    assert len(contexts) == 2
    assert contexts[0].user_message == "What's the weather in Beijing?"
    assert contexts[0].assistant_messages == ["Let me check...", "Beijing is sunny today, 25 degrees."]
    assert contexts[0].tool_results == ['{"result": "sunny, 25C"}']
    assert contexts[0].signal_from_users == ["Thanks!"]
    assert contexts[1].assistant_messages == ["You're welcome!"]


def test_build_response_helpful_prompt_uses_only_adjacent_feedback_block():
    from claw_data_filter.processors.round_feedback import TurnContextBuilder

    builder = TurnContextBuilder()
    context = builder.extract_response_contexts("sample-1", SAMPLE_MESSAGES)[0]
    prompt = builder.build_response_helpful_prompt(context)

    assert "当前 assistant 响应单元" in prompt
    assert "紧邻反馈块类型" in prompt
    assert "web_search({})" in prompt
    assert '{"result": "sunny, 25C"}' in prompt
    assert "只输出一行：response_helpful=yes|no|uncertain" in prompt


def test_build_user_satisfied_prompt_uses_episode_and_later_user_signals():
    from claw_data_filter.processors.round_feedback import TurnContextBuilder

    builder = TurnContextBuilder()
    context = builder.extract_episode_contexts("sample-1", SAMPLE_MESSAGES)[0]
    prompt = builder.build_user_satisfied_prompt(context)

    assert "episode 起始用户请求" in prompt
    assert "后续最多 3 条真实用户文本反馈" in prompt
    assert "Thanks!" in prompt
    assert "只输出一行：user_satisfied=yes|no|uncertain|neutral" in prompt


def test_extract_contexts_skip_empty_messages():
    from claw_data_filter.processors.round_feedback import TurnContextBuilder

    builder = TurnContextBuilder()
    assert builder.extract_response_contexts("sample-1", []) == []
    assert builder.extract_episode_contexts("sample-1", []) == []


def test_extract_response_contexts_absorb_tool_result_only_user_blocks():
    from claw_data_filter.processors.round_feedback import TurnContextBuilder

    contexts = TurnContextBuilder().extract_response_contexts("sample-2", ANTHROPIC_TOOL_CHAIN_MESSAGES)

    assert len(contexts) == 3
    assert contexts[0].feedback_kind.value == "tool_result"
    assert contexts[0].feedback_payload == ["a.txt\nb.txt"]
    assert contexts[1].feedback_kind.value == "user"
    assert contexts[1].feedback_payload == ["那 a.txt 里是什么？"]


def test_extract_episode_contexts_skip_tool_result_only_user_blocks():
    from claw_data_filter.processors.round_feedback import TurnContextBuilder

    contexts = TurnContextBuilder().extract_episode_contexts("sample-2", ANTHROPIC_TOOL_CHAIN_MESSAGES)

    assert len(contexts) == 2
    assert contexts[0].tool_results == ["a.txt\nb.txt"]
    assert contexts[0].signal_from_users == ["那 a.txt 里是什么？"]


def test_response_helpful_parser_accepts_expected_values():
    from claw_data_filter.llm.async_client import AsyncLLMClient
    from claw_data_filter.processors.round_feedback import ResponseHelpfulJudgmentProcessor

    processor = ResponseHelpfulJudgmentProcessor(AsyncMock(spec=AsyncLLMClient))

    assert processor._parse_response("response_helpful=yes") == "yes"
    assert processor._parse_response("response_helpful=uncertain") == "uncertain"
    assert processor._parse_response("invalid") is None


def test_user_satisfied_parser_accepts_expected_values():
    from claw_data_filter.llm.async_client import AsyncLLMClient
    from claw_data_filter.processors.round_feedback import UserSatisfiedJudgmentProcessor

    processor = UserSatisfiedJudgmentProcessor(AsyncMock(spec=AsyncLLMClient))

    assert processor._parse_response("user_satisfied=yes") == "yes"
    assert processor._parse_response("user_satisfied=neutral") == "neutral"
    assert processor._parse_response("invalid") is None


def test_tool_stats_aggregator_uses_dual_denominators():
    from claw_data_filter.models.round_judgment import AssistantResponseJudgment, FeedbackKind, UserEpisodeJudgment
    from claw_data_filter.processors.round_feedback import ToolStatsAggregator

    response_judgments = [
        AssistantResponseJudgment(sample_uid="s", response_index=0, episode_index=0, assistant_message_index=1, feedback_kind=FeedbackKind.NONE, response_helpful="yes", llm_error=False),
        AssistantResponseJudgment(sample_uid="s", response_index=1, episode_index=0, assistant_message_index=2, feedback_kind=FeedbackKind.NONE, response_helpful="uncertain", llm_error=False),
        AssistantResponseJudgment(sample_uid="s", response_index=2, episode_index=1, assistant_message_index=4, feedback_kind=FeedbackKind.NONE, response_helpful="no", llm_error=False),
    ]
    episode_judgments = [
        UserEpisodeJudgment(sample_uid="s", episode_index=0, start_user_message_index=0, end_before_user_message_index=2, signal_from_users=["继续"], user_satisfied="yes", llm_error=False),
        UserEpisodeJudgment(sample_uid="s", episode_index=1, start_user_message_index=3, end_before_user_message_index=4, signal_from_users=[], user_satisfied="neutral", llm_error=False),
        UserEpisodeJudgment(sample_uid="s", episode_index=2, start_user_message_index=5, end_before_user_message_index=6, signal_from_users=[], user_satisfied="no", llm_error=False),
    ]

    stats = ToolStatsAggregator.aggregate(response_judgments, episode_judgments)

    assert stats["response_helpful_rate"] == 0.5
    assert stats["response_unhelpful_rate"] == 0.5
    assert stats["user_satisfied_rate"] == 1 / 3
    assert stats["user_negative_feedback_rate"] == 1 / 3
    assert stats["response_helpful_scored_steps"] == 2
    assert stats["user_feedback_scored_episodes"] == 3


@pytest.mark.asyncio
async def test_process_sample_marks_unirouter_sample_complete(tmp_path):
    from claw_data_filter.models.sample import Sample
    from claw_data_filter.processors.round_feedback import RoundFeedbackProcessor
    from claw_data_filter.storage.duckdb_store import DuckDBStore

    class MockLLM:
        async def chat(self, messages, max_tokens=50):
            if "response_helpful" in messages[0]["content"]:
                return "response_helpful=yes"
            return "user_satisfied=yes"

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

    sample_id = store.insert_sample(Sample.from_dict(raw_json))
    sample_uid = store.get_sample_by_id(sample_id)["sample_uid"]
    judgments = await RoundFeedbackProcessor(store, MockLLM(), max_concurrency=2).process_sample(sample_uid, raw_json)

    assert judgments.sample_uid == sample_uid
    assert len(judgments.response_judgments) == 2
    assert len(judgments.episode_judgments) == 1
    assert len(store.get_assistant_response_judgments(sample_uid)) == 2
    assert len(store.get_user_episode_judgments(sample_uid)) == 1

    row = store.conn.execute(
        "SELECT expected_judgment_count, tool_stats FROM samples WHERE sample_uid = ?",
        [sample_uid],
    ).fetchone()
    assert row[0] == 3
    assert json.loads(row[1])["response_helpful_rate"] == 1.0
    store.close()
