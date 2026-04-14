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

PROGRESS_CHAIN_MESSAGES = [
    {"role": "user", "content": "帮我生成日报并写到文件"},
    {
        "role": "assistant",
        "content": "我先读取原始数据。",
        "tool_calls": [
            {
                "type": "function",
                "function": {
                    "name": "read_file",
                    "arguments": '{"path":"/tmp/source.txt"}',
                },
            }
        ],
    },
    {"role": "tool", "content": "read success: line1\nline2\nline3"},
    {
        "role": "assistant",
        "content": "我现在把日报写入文件。",
        "tool_calls": [
            {
                "type": "function",
                "function": {
                    "name": "write_file",
                    "arguments": json.dumps(
                        {
                            "path": "/tmp/report.md",
                            "content": "# Report\n" + "A" * 600,
                            "mode": "overwrite",
                            "encoding": "utf-8",
                        },
                        ensure_ascii=False,
                    ),
                },
            }
        ],
    },
    {"role": "tool", "content": "File written successfully to /tmp/report.md"},
    {"role": "assistant", "content": "日报已经生成好了。"},
    {"role": "user", "content": "继续发给我摘要"},
    {"role": "assistant", "content": "这是新的请求响应。"},
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


def test_build_response_progress_prompt_uses_only_adjacent_feedback_block():
    from claw_data_filter.processors.round_feedback import TurnContextBuilder

    builder = TurnContextBuilder()
    context = builder.extract_response_contexts("sample-1", SAMPLE_MESSAGES)[0]
    prompt = builder.build_response_progress_prompt(context)

    assert "当前 assistant 响应单元" in prompt
    assert "紧邻反馈块类型" in prompt
    assert "web_search({})" in prompt
    assert '{"result": "sunny, 25C"}' in prompt
    assert "是否让当前问题状态发生正向推进" in prompt
    assert "只输出一行：response_progress=yes|no|uncertain" in prompt


def test_extract_response_contexts_include_recent_execution_background_and_reset_by_episode():
    from claw_data_filter.processors.round_feedback import TurnContextBuilder

    contexts = TurnContextBuilder().extract_response_contexts("sample-3", PROGRESS_CHAIN_MESSAGES)

    assert len(contexts) == 4
    assert contexts[0].prior_execution_background == []
    assert len(contexts[1].prior_execution_background) == 1
    assert contexts[1].prior_execution_background[0].tool_use_summary == "read_file(path=/tmp/source.txt)"
    assert contexts[1].prior_execution_background[0].tool_result_status_hint == "success"
    assert contexts[2].prior_execution_background[-1].tool_use_summary.startswith(
        "write_file(path=/tmp/report.md, content=<text:"
    )
    assert 'A' * 80 not in contexts[2].prior_execution_background[-1].tool_use_summary
    assert contexts[2].prior_execution_background[-1].tool_result_status_hint == "success"
    assert contexts[3].prior_execution_background == []


def test_build_response_progress_prompt_includes_progress_background_summary():
    from claw_data_filter.processors.round_feedback import TurnContextBuilder

    builder = TurnContextBuilder()
    context = builder.extract_response_contexts("sample-3", PROGRESS_CHAIN_MESSAGES)[2]
    prompt = builder.build_response_progress_prompt(context)

    assert "当前单元之前的执行背景（仅供理解当前阶段）" in prompt
    assert "Step -2:" in prompt
    assert "Step -1:" in prompt
    assert "tool_result_status_hint: success" in prompt
    assert "write_file(path=/tmp/report.md, content=<text:" in prompt
    assert 'A' * 80 not in prompt
    assert "不要把前序步骤的功劳或失败直接转嫁到当前 step" in prompt


def test_build_user_satisfied_prompt_uses_episode_and_later_user_signals():
    from claw_data_filter.processors.round_feedback import TurnContextBuilder

    builder = TurnContextBuilder()
    context = builder.extract_episode_contexts("sample-1", SAMPLE_MESSAGES)[0]
    prompt = builder.build_user_satisfied_prompt(context)

    assert "episode 起始用户请求" in prompt
    assert "后续最多 3 条真实用户文本反馈" in prompt
    assert "Thanks!" in prompt
    assert "只输出一行：user_satisfied=yes|no|uncertain|neutral" in prompt


def test_extract_episode_contexts_keep_recent_ten_rounds_only():
    from claw_data_filter.processors.round_feedback import TurnContextBuilder

    messages = [{"role": "user", "content": "开始处理这个任务"}]
    for index in range(12):
        messages.append({"role": "assistant", "content": f"assistant round {index:02d}"})
        messages.append({"role": "tool", "content": f"tool round {index:02d}"})

    context = TurnContextBuilder(episode_round_limit=10).extract_episode_contexts("sample-rounds", messages)[0]
    prompt = TurnContextBuilder(episode_round_limit=10).build_user_satisfied_prompt(context)

    assert context.total_rounds == 12
    assert context.retained_rounds == 10
    assert "assistant round 00" not in prompt
    assert "tool round 00" not in prompt
    assert "assistant round 01" not in prompt
    assert "tool round 01" not in prompt
    assert "assistant round 11" in prompt
    assert "tool round 11" in prompt


def test_build_response_progress_prompt_truncates_feedback_payload_text():
    from claw_data_filter.processors.round_feedback import TurnContextBuilder

    messages = [
        {"role": "user", "content": "帮我检查结果"},
        {"role": "assistant", "content": "我先读取长输出。"},
        {"role": "tool", "content": "X" * 900},
    ]

    builder = TurnContextBuilder()
    context = builder.extract_response_contexts("sample-feedback", messages)[0]
    prompt = builder.build_response_progress_prompt(context)

    assert "X" * 600 not in prompt
    assert "[tool_result]:" in prompt
    assert "..." in prompt


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


def test_response_progress_parser_accepts_expected_values():
    from claw_data_filter.llm.async_client import AsyncLLMClient
    from claw_data_filter.processors.round_feedback import ResponseProgressJudgmentProcessor

    processor = ResponseProgressJudgmentProcessor(AsyncMock(spec=AsyncLLMClient))

    assert processor._parse_response("response_progress=yes") == "yes"
    assert processor._parse_response("response_progress=uncertain") == "uncertain"
    assert processor._parse_response("<think>\n\n</think>\n\nno") == "no"
    assert processor._parse_response("`yes`") == "yes"
    assert processor._parse_response("invalid") is None


def test_user_satisfied_parser_accepts_expected_values():
    from claw_data_filter.llm.async_client import AsyncLLMClient
    from claw_data_filter.processors.round_feedback import UserSatisfiedJudgmentProcessor

    processor = UserSatisfiedJudgmentProcessor(AsyncMock(spec=AsyncLLMClient))

    assert processor._parse_response("user_satisfied=yes") == "yes"
    assert processor._parse_response("user_satisfied=neutral") == "neutral"
    assert processor._parse_response("<think>\n\n</think>\n\nuncertain") == "uncertain"
    assert processor._parse_response("'no'") == "no"
    assert processor._parse_response("invalid") is None


@pytest.mark.asyncio
async def test_response_progress_retry_uses_configured_backoff(monkeypatch):
    from claw_data_filter.processors.round_feedback import ResponseProgressJudgmentProcessor

    sleep_calls = []

    async def fake_sleep(delay):
        sleep_calls.append(delay)

    class FlakyLLM:
        def __init__(self):
            self.calls = 0

        async def chat(self, messages, max_tokens=50):
            self.calls += 1
            if self.calls < 3:
                raise RuntimeError("temporary failure")
            return "response_progress=yes"

    monkeypatch.setattr("claw_data_filter.processors.round_feedback.asyncio.sleep", fake_sleep)

    processor = ResponseProgressJudgmentProcessor(FlakyLLM(), max_retries=3, retry_base_delay=5.0, retry_max_delay=12.0)
    result = await processor.judge("prompt")

    assert result == "yes"
    assert sleep_calls == [5.0, 10.0]


def test_tool_stats_aggregator_uses_dual_denominators():
    from claw_data_filter.models.round_judgment import AssistantResponseJudgment, FeedbackKind, UserEpisodeJudgment
    from claw_data_filter.processors.round_feedback import ToolStatsAggregator

    response_judgments = [
        AssistantResponseJudgment(sample_uid="s", response_index=0, episode_index=0, assistant_message_index=1, feedback_kind=FeedbackKind.NONE, response_progress="yes", llm_error=False),
        AssistantResponseJudgment(sample_uid="s", response_index=1, episode_index=0, assistant_message_index=2, feedback_kind=FeedbackKind.NONE, response_progress="uncertain", llm_error=False),
        AssistantResponseJudgment(sample_uid="s", response_index=2, episode_index=1, assistant_message_index=4, feedback_kind=FeedbackKind.NONE, response_progress="no", llm_error=False),
    ]
    episode_judgments = [
        UserEpisodeJudgment(sample_uid="s", episode_index=0, start_user_message_index=0, end_before_user_message_index=2, signal_from_users=["继续"], user_satisfied="yes", llm_error=False),
        UserEpisodeJudgment(sample_uid="s", episode_index=1, start_user_message_index=3, end_before_user_message_index=4, signal_from_users=[], user_satisfied="neutral", llm_error=False),
        UserEpisodeJudgment(sample_uid="s", episode_index=2, start_user_message_index=5, end_before_user_message_index=6, signal_from_users=[], user_satisfied="no", llm_error=False),
    ]

    stats = ToolStatsAggregator.aggregate(response_judgments, episode_judgments)

    assert stats["response_progress_rate"] == 0.5
    assert stats["response_regress_rate"] == 0.5
    assert stats["user_satisfied_rate"] == 1 / 3
    assert stats["user_negative_feedback_rate"] == 1 / 3
    assert stats["response_progress_scored_steps"] == 2
    assert stats["user_feedback_scored_episodes"] == 3


@pytest.mark.asyncio
async def test_process_sample_marks_unirouter_sample_complete(tmp_path):
    from claw_data_filter.models.sample import Sample
    from claw_data_filter.processors.round_feedback import RoundFeedbackProcessor
    from claw_data_filter.storage.duckdb_store import DuckDBStore

    class MockLLM:
        async def chat(self, messages, max_tokens=50):
            if "response_progress" in messages[0]["content"]:
                return "response_progress=yes"
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
    assert json.loads(row[1])["response_progress_rate"] == 1.0
    store.close()


@pytest.mark.asyncio
async def test_process_batch_marks_sample_failed_when_prompt_still_too_long(tmp_path):
    from claw_data_filter.models.sample import Sample
    from claw_data_filter.processors.round_feedback import RoundFeedbackProcessor
    from claw_data_filter.storage.duckdb_store import DuckDBStore

    class UnexpectedLLMCall:
        async def chat(self, messages, max_tokens=50):
            raise AssertionError("LLM should not be called for over-budget prompts")

    store = DuckDBStore(tmp_path / "round_feedback_over_budget.duckdb")
    raw_json = {
        "messages": [
            {"role": "user", "content": "请总结这段执行"},
            {"role": "assistant", "content": "A" * 500},
            {"role": "tool", "content": "B" * 500},
            {"role": "assistant", "content": "C" * 500},
            {"role": "tool", "content": "D" * 500},
        ]
    }

    sample_id = store.insert_sample(Sample.from_dict(raw_json))
    sample_uid = store.get_sample_by_id(sample_id)["sample_uid"]
    processor = RoundFeedbackProcessor(store, UnexpectedLLMCall(), max_concurrency=2, prompt_char_limit=700)

    success, failures = await processor.process_batch([(sample_uid, raw_json)])

    row = store.conn.execute(
        "SELECT processing_status, tool_stats FROM samples WHERE sample_uid = ?",
        [sample_uid],
    ).fetchone()
    tool_stats = json.loads(row[1])

    assert success == 0
    assert failures == 1
    assert row[0] == "failed"
    assert tool_stats["error_reason"] == "user_satisfied_prompt_too_long_after_truncation"
    assert store.get_assistant_response_judgments(sample_uid) == []
    assert store.get_user_episode_judgments(sample_uid) == []
    store.close()
