"""Dual-level round feedback processor with compatibility helpers."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from claw_data_filter.models.round_judgment import (
    AssistantResponseJudgment,
    FeedbackKind,
    RoundJudgment,
    UserEpisodeJudgment,
)
from claw_data_filter.models.sample import extract_messages_from_payload, extract_normalized_messages

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class TurnContext:
    """Legacy user-anchored turn context kept for compatibility."""

    turn_index: int
    user_message: str
    assistant_message: str
    tool_calls: list[dict[str, Any]]
    tool_result: str | None
    signal_users: list[str]
    execution_trace: list[str] = field(default_factory=list)


@dataclass(slots=True)
class AssistantResponseContext:
    """Context for a single assistant response unit."""

    sample_uid: str
    response_index: int
    episode_index: int
    assistant_message_index: int
    user_message: str
    assistant_message: str
    tool_calls: list[dict[str, Any]]
    feedback_kind: FeedbackKind
    feedback_message_start_index: int | None
    feedback_message_end_index: int | None
    feedback_payload: list[str] = field(default_factory=list)
    execution_trace: list[str] = field(default_factory=list)


@dataclass(slots=True)
class UserEpisodeContext:
    """Context for a complete user episode."""

    sample_uid: str
    episode_index: int
    start_user_message_index: int
    end_before_user_message_index: int | None
    user_message: str
    assistant_messages: list[str] = field(default_factory=list)
    tool_calls: list[dict[str, Any]] = field(default_factory=list)
    tool_results: list[str] = field(default_factory=list)
    signal_from_users: list[str] = field(default_factory=list)
    execution_trace: list[str] = field(default_factory=list)


@dataclass(slots=True)
class SampleJudgmentResult:
    """In-memory result bundle for one sample."""

    sample_uid: str
    response_judgments: list[AssistantResponseJudgment]
    episode_judgments: list[UserEpisodeJudgment]
    tool_stats: dict[str, Any]


@dataclass(slots=True)
class ConversationEvent:
    """Normalized event stream for conversation parsing."""

    role: str
    message_index: int
    text: str = ""
    tool_calls: list[dict[str, Any]] = field(default_factory=list)


class TurnContextBuilder:
    """Build both legacy turn contexts and new dual-level judgment contexts."""

    def extract_turns(self, messages: list[dict[str, Any]]) -> list[TurnContext]:
        """Build legacy user-anchored turns for compatibility paths."""
        events = self._normalize_messages(messages)
        turns: list[TurnContext] = []
        index = 0

        while index < len(events):
            event = events[index]
            if event.role != "user":
                index += 1
                continue

            assistant_parts: list[str] = []
            tool_calls: list[dict[str, Any]] = []
            tool_results: list[str] = []
            execution_trace: list[str] = []
            next_index = index + 1

            while next_index < len(events):
                next_event = events[next_index]
                if next_event.role == "user":
                    break
                if next_event.role == "assistant":
                    if next_event.text:
                        assistant_parts.append(next_event.text)
                        execution_trace.append(self._format_message("assistant", next_event.text))
                    for tool_call in next_event.tool_calls:
                        tool_calls.append(tool_call)
                        execution_trace.append(self._format_tool_call(tool_call))
                elif next_event.role == "tool":
                    if next_event.text:
                        tool_results.append(next_event.text)
                        execution_trace.append(self._format_tool_result(next_event.text))
                next_index += 1

            if assistant_parts or tool_calls or tool_results:
                turns.append(
                    TurnContext(
                        turn_index=len(turns),
                        user_message=event.text,
                        assistant_message="\n".join(part for part in assistant_parts if part),
                        tool_calls=tool_calls,
                        tool_result="\n".join(tool_results) if tool_results else None,
                        signal_users=[],
                        execution_trace=execution_trace,
                    )
                )
            index = next_index

        for current_index, turn in enumerate(turns):
            turn.signal_users = [
                candidate.user_message
                for candidate in turns[current_index + 1 : current_index + 4]
                if candidate.user_message
            ]
        return turns

    def count_expected_turns(self, messages: list[dict[str, Any]]) -> int:
        return len(self.extract_turns(messages))

    def build_judgment_prompt(self, turn: TurnContext, all_turns: list[TurnContext]) -> str:
        """Legacy combined prompt kept for compatibility tests and tooling."""
        current_user = self._format_message("user", turn.user_message) if turn.user_message else "(无当前用户请求)"
        execution_section = "\n".join(turn.execution_trace) if turn.execution_trace else "(无执行结果)"
        signal_section = (
            "\n".join(self._format_message("user", user) for user in turn.signal_users)
            if turn.signal_users
            else "(无后续真实用户消息)"
        )
        return f"""=== 当前用户请求 ===
{current_user}

=== 当前assistant执行链 ===
{execution_section}

=== 后续真实用户反馈（最多3轮，已跳过仅tool_result轮）===
{signal_section}

请判断：
1. response_helpful: 这次 assistant 响应单元对用户有帮助吗？（yes/no/uncertain）
2. user_satisfied: 当前 user episode 的后续真实用户反馈是否体现满意？（yes/no/uncertain/neutral）

答案格式：response_helpful=yes; user_satisfied=no

注意：
- system reminder、plan mode 提示、tool 框架提示、工具中断提示等系统/框架文本可能混在对话里；把它们视为上下文信息，只有在确实影响任务结果时才纳入判断，不要默认当作用户真实诉求或满意度反馈
"""

    def extract_response_contexts(self, sample_uid: str, messages: list[dict[str, Any]]) -> list[AssistantResponseContext]:
        events = self._normalize_messages(messages)
        contexts: list[AssistantResponseContext] = []
        episode_index = -1
        active_user_text = ""
        active_user_index: int | None = None
        event_index = 0

        while event_index < len(events):
            event = events[event_index]
            if event.role == "user":
                active_user_text = event.text
                active_user_index = event.message_index
                episode_index += 1
                event_index += 1
                continue

            if event.role != "assistant" or active_user_index is None:
                event_index += 1
                continue

            execution_trace: list[str] = []
            if event.text:
                execution_trace.append(self._format_message("assistant", event.text))
            for tool_call in event.tool_calls:
                execution_trace.append(self._format_tool_call(tool_call))

            feedback_kind = FeedbackKind.NONE
            feedback_payload: list[str] = []
            feedback_start: int | None = None
            feedback_end: int | None = None
            scan_index = event_index + 1

            while scan_index < len(events):
                next_event = events[scan_index]
                if next_event.role == "assistant":
                    break
                if next_event.role == "tool":
                    if feedback_kind == FeedbackKind.NONE:
                        feedback_kind = FeedbackKind.TOOL_RESULT
                    if feedback_kind != FeedbackKind.TOOL_RESULT:
                        break
                    feedback_payload.append(next_event.text)
                    feedback_start = next_event.message_index if feedback_start is None else feedback_start
                    feedback_end = next_event.message_index
                    execution_trace.append(self._format_tool_result(next_event.text))
                    scan_index += 1
                    continue
                if next_event.role == "user":
                    if feedback_kind == FeedbackKind.TOOL_RESULT:
                        break
                    feedback_kind = FeedbackKind.USER
                    feedback_payload.append(next_event.text)
                    feedback_start = next_event.message_index
                    feedback_end = next_event.message_index
                    break
                scan_index += 1

            contexts.append(
                AssistantResponseContext(
                    sample_uid=sample_uid,
                    response_index=len(contexts),
                    episode_index=max(episode_index, 0),
                    assistant_message_index=event.message_index,
                    user_message=active_user_text,
                    assistant_message=event.text,
                    tool_calls=event.tool_calls,
                    feedback_kind=feedback_kind,
                    feedback_message_start_index=feedback_start,
                    feedback_message_end_index=feedback_end,
                    feedback_payload=feedback_payload,
                    execution_trace=execution_trace,
                )
            )
            event_index += 1

        return contexts

    def extract_episode_contexts(self, sample_uid: str, messages: list[dict[str, Any]]) -> list[UserEpisodeContext]:
        events = self._normalize_messages(messages)
        user_events = [event for event in events if event.role == "user"]
        contexts: list[UserEpisodeContext] = []

        for user_offset, user_event in enumerate(user_events):
            next_user_index = user_events[user_offset + 1].message_index if user_offset + 1 < len(user_events) else None
            assistant_messages: list[str] = []
            tool_calls: list[dict[str, Any]] = []
            tool_results: list[str] = []
            execution_trace: list[str] = []
            last_message_index: int | None = None

            for event in events:
                if event.message_index <= user_event.message_index:
                    continue
                if next_user_index is not None and event.message_index >= next_user_index:
                    break
                if event.role == "assistant":
                    if event.text:
                        assistant_messages.append(event.text)
                        execution_trace.append(self._format_message("assistant", event.text))
                    for tool_call in event.tool_calls:
                        tool_calls.append(tool_call)
                        execution_trace.append(self._format_tool_call(tool_call))
                    last_message_index = event.message_index
                elif event.role == "tool":
                    if event.text:
                        tool_results.append(event.text)
                        execution_trace.append(self._format_tool_result(event.text))
                    last_message_index = event.message_index

            if not assistant_messages and not tool_calls and not tool_results:
                continue

            signal_from_users = [
                candidate.text
                for candidate in user_events[user_offset + 1 : user_offset + 4]
                if candidate.text
            ]
            contexts.append(
                UserEpisodeContext(
                    sample_uid=sample_uid,
                    episode_index=len(contexts),
                    start_user_message_index=user_event.message_index,
                    end_before_user_message_index=last_message_index,
                    user_message=user_event.text,
                    assistant_messages=assistant_messages,
                    tool_calls=tool_calls,
                    tool_results=tool_results,
                    signal_from_users=signal_from_users,
                    execution_trace=execution_trace,
                )
            )

        return contexts

    def build_response_helpful_prompt(self, context: AssistantResponseContext) -> str:
        execution = "\n".join(context.execution_trace) if context.execution_trace else "(无 assistant 输出)"
        feedback_payload = "\n".join(context.feedback_payload) if context.feedback_payload else "(无紧邻反馈块)"
        feedback_label = {
            FeedbackKind.TOOL_RESULT: "紧邻 tool result",
            FeedbackKind.USER: "紧邻 user 反馈",
            FeedbackKind.NONE: "无紧邻反馈",
        }[context.feedback_kind]
        return f"""你要判断 assistant 的单个响应单元是否有帮助。

=== 当前用户请求 ===
{self._format_message('user', context.user_message)}

=== 当前 assistant 响应单元 ===
{execution}

=== 紧邻反馈块类型 ===
{feedback_label}

=== 紧邻反馈块内容 ===
{feedback_payload}

只输出一行：response_helpful=yes|no|uncertain"""

    def build_user_satisfied_prompt(self, context: UserEpisodeContext) -> str:
        episode_trace = "\n".join(context.execution_trace) if context.execution_trace else "(无 assistant 执行链)"
        signals = (
            "\n".join(self._format_message("user", item) for item in context.signal_from_users)
            if context.signal_from_users
            else "(无后续真实用户文本反馈)"
        )
        return f"""你要判断一个完整 user episode 是否让用户满意。

=== episode 起始用户请求 ===
{self._format_message('user', context.user_message)}

=== episode 内 assistant 执行链 ===
{episode_trace}

=== 后续最多 3 条真实用户文本反馈 ===
{signals}

只输出一行：user_satisfied=yes|no|uncertain|neutral"""

    def _normalize_messages(self, messages: list[dict[str, Any]]) -> list[ConversationEvent]:
        normalized = extract_normalized_messages(messages)
        events: list[ConversationEvent] = []
        for message_index, message in enumerate(normalized):
            role = message.get("role")
            if role == "system":
                continue
            if role == "user":
                text = self._extract_text_content(message.get("content"))
                if text:
                    events.append(ConversationEvent(role="user", message_index=message_index, text=text))
                continue
            if role == "assistant":
                text = self._extract_text_content(message.get("content"))
                tool_calls = self._extract_tool_calls(message)
                if text or tool_calls:
                    events.append(
                        ConversationEvent(
                            role="assistant",
                            message_index=message_index,
                            text=text,
                            tool_calls=tool_calls,
                        )
                    )
                continue
            if role == "tool":
                text = self._extract_text_content(message.get("content"))
                if text:
                    events.append(ConversationEvent(role="tool", message_index=message_index, text=text))
        return events

    def _extract_tool_calls(self, message: dict[str, Any]) -> list[dict[str, Any]]:
        tool_calls: list[dict[str, Any]] = []
        for tool_call in message.get("tool_calls", []):
            function = tool_call.get("function") if isinstance(tool_call, dict) else None
            if isinstance(function, dict):
                tool_calls.append(
                    {
                        "name": function.get("name", ""),
                        "arguments": function.get("arguments", ""),
                    }
                )
        return tool_calls

    def _extract_text_content(self, content: Any) -> str:
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts: list[str] = []
            for item in content:
                if isinstance(item, dict) and item.get("type") == "text":
                    parts.append(item.get("text", ""))
            return "".join(parts)
        return str(content) if content else ""

    def _format_message(self, role: str, content: str, max_len: int = 500) -> str:
        rendered = content[:max_len] + "..." if len(content) > max_len else content
        return f"[{role}]: {rendered}"

    def _format_tool_call(self, tool_call: dict[str, Any], max_len: int = 500) -> str:
        arguments = tool_call.get("arguments", "")
        if not isinstance(arguments, str):
            arguments = json.dumps(arguments, ensure_ascii=False, sort_keys=True)
        rendered = arguments[:max_len] + "..." if len(arguments) > max_len else arguments
        return f"[assistant_tool_use]: {tool_call.get('name', 'unknown')}({rendered})"

    def _format_tool_result(self, content: str, max_len: int = 500) -> str:
        rendered = content[:max_len] + "..." if len(content) > max_len else content
        return f"[tool_result]: {rendered}"


class ResponseHelpfulJudgmentProcessor:
    """LLM processor for assistant response helpfulness."""

    def __init__(self, llm_client: Any, max_retries: int = 2):
        self.llm = llm_client
        self.max_retries = max_retries

    async def judge(self, prompt: str) -> str | None:
        return await self._call_llm_with_retry(prompt, self._parse_response)

    async def _call_llm_with_retry(self, prompt: str, parser) -> str | None:
        for attempt in range(self.max_retries + 1):
            try:
                response = await self.llm.chat([{"role": "user", "content": prompt}], max_tokens=50)
                result = parser(response)
                if result is not None:
                    return result
                logger.warning("Attempt %s: failed to parse response helpful output", attempt + 1)
            except Exception as exc:
                logger.warning("Attempt %s: response helpful LLM call failed: %s", attempt + 1, exc)
                if attempt < self.max_retries:
                    await asyncio.sleep(2**attempt)
                else:
                    return None
        return None

    def _parse_response(self, response: str) -> str | None:
        match = re.search(r"response_helpful\s*=\s*(yes|no|uncertain)", response.strip(), re.IGNORECASE)
        return match.group(1).lower() if match else None


class UserSatisfiedJudgmentProcessor:
    """LLM processor for user episode satisfaction."""

    def __init__(self, llm_client: Any, max_retries: int = 2):
        self.llm = llm_client
        self.max_retries = max_retries

    async def judge(self, prompt: str) -> str | None:
        return await self._call_llm_with_retry(prompt, self._parse_response)

    async def _call_llm_with_retry(self, prompt: str, parser) -> str | None:
        for attempt in range(self.max_retries + 1):
            try:
                response = await self.llm.chat([{"role": "user", "content": prompt}], max_tokens=50)
                result = parser(response)
                if result is not None:
                    return result
                logger.warning("Attempt %s: failed to parse user satisfied output", attempt + 1)
            except Exception as exc:
                logger.warning("Attempt %s: user satisfied LLM call failed: %s", attempt + 1, exc)
                if attempt < self.max_retries:
                    await asyncio.sleep(2**attempt)
                else:
                    return None
        return None

    def _parse_response(self, response: str) -> str | None:
        match = re.search(r"user_satisfied\s*=\s*(yes|no|uncertain|neutral)", response.strip(), re.IGNORECASE)
        return match.group(1).lower() if match else None


class RoundJudgmentProcessor:
    """Legacy combined processor retained for compatibility tests."""

    def __init__(self, llm_client: Any, max_retries: int = 2):
        self.llm = llm_client
        self.max_retries = max_retries
        self.response_processor = ResponseHelpfulJudgmentProcessor(llm_client, max_retries=max_retries)
        self.user_processor = UserSatisfiedJudgmentProcessor(llm_client, max_retries=max_retries)

    async def judge(self, prompt: str) -> dict[str, str] | None:
        for attempt in range(self.max_retries + 1):
            try:
                response = await self.llm.chat([{"role": "user", "content": prompt}], max_tokens=50)
                parsed = self._parse_response(response)
                if parsed is not None:
                    return parsed
            except Exception as exc:
                logger.warning("Attempt %s: combined round judgment LLM call failed: %s", attempt + 1, exc)
                if attempt < self.max_retries:
                    await asyncio.sleep(2**attempt)
                else:
                    return None
        return None

    def _parse_response(self, response: str) -> dict[str, str] | None:
        response_helpful = self.response_processor._parse_response(response)
        user_satisfied = self.user_processor._parse_response(response)
        if not response_helpful or not user_satisfied:
            return None
        return {
            "response_helpful": response_helpful,
            "user_satisfied": user_satisfied,
        }


class ToolStatsAggregator:
    """Aggregate dual-level or legacy judgments into sample-level stats."""

    @staticmethod
    def aggregate(
        response_judgments: list[AssistantResponseJudgment] | list[RoundJudgment],
        episode_judgments: list[UserEpisodeJudgment] | None = None,
    ) -> dict[str, Any]:
        if episode_judgments is None:
            legacy_judgments = response_judgments  # type: ignore[assignment]
            helpful_yes = sum(1 for row in legacy_judgments if row.response_helpful == "yes")
            helpful_no = sum(1 for row in legacy_judgments if row.response_helpful == "no")
            helpful_uncertain = sum(1 for row in legacy_judgments if row.response_helpful == "uncertain")
            satisfied_yes = sum(1 for row in legacy_judgments if row.user_satisfied == "yes")
            satisfied_no = sum(1 for row in legacy_judgments if row.user_satisfied == "no")
            satisfied_neutral = sum(1 for row in legacy_judgments if row.user_satisfied == "neutral")
            satisfied_uncertain = sum(1 for row in legacy_judgments if row.user_satisfied == "uncertain")
            helpful_scored = helpful_yes + helpful_no
            satisfied_scored = satisfied_yes + satisfied_no + satisfied_neutral
            return {
                "response_helpful_rate": helpful_yes / helpful_scored if helpful_scored else 0.0,
                "response_unhelpful_rate": helpful_no / helpful_scored if helpful_scored else 0.0,
                "user_satisfied_rate": satisfied_yes / satisfied_scored if satisfied_scored else 0.0,
                "user_negative_feedback_rate": satisfied_no / satisfied_scored if satisfied_scored else 0.0,
                "response_helpful_scored_turns": helpful_scored,
                "user_feedback_scored_turns": satisfied_scored,
                "total_turns": len(legacy_judgments),
                "has_error": any(row.llm_error for row in legacy_judgments),
                "response_helpful": {
                    "yes": helpful_yes,
                    "no": helpful_no,
                    "uncertain": helpful_uncertain,
                    "rate": helpful_yes / helpful_scored if helpful_scored else 0.0,
                },
                "user_satisfied": {
                    "yes": satisfied_yes,
                    "no": satisfied_no,
                    "neutral": satisfied_neutral,
                    "uncertain": satisfied_uncertain,
                    "rate": satisfied_yes / satisfied_scored if satisfied_scored else 0.0,
                },
            }

        helpful_yes = sum(1 for row in response_judgments if row.response_helpful == "yes")
        helpful_no = sum(1 for row in response_judgments if row.response_helpful == "no")
        helpful_uncertain = sum(1 for row in response_judgments if row.response_helpful == "uncertain")
        satisfied_yes = sum(1 for row in episode_judgments if row.user_satisfied == "yes")
        satisfied_no = sum(1 for row in episode_judgments if row.user_satisfied == "no")
        satisfied_neutral = sum(1 for row in episode_judgments if row.user_satisfied == "neutral")
        satisfied_uncertain = sum(1 for row in episode_judgments if row.user_satisfied == "uncertain")
        helpful_scored = helpful_yes + helpful_no
        satisfied_scored = satisfied_yes + satisfied_no + satisfied_neutral
        return {
            "response_helpful_rate": helpful_yes / helpful_scored if helpful_scored else 0.0,
            "response_unhelpful_rate": helpful_no / helpful_scored if helpful_scored else 0.0,
            "user_satisfied_rate": satisfied_yes / satisfied_scored if satisfied_scored else 0.0,
            "user_negative_feedback_rate": satisfied_no / satisfied_scored if satisfied_scored else 0.0,
            "response_helpful_scored_steps": helpful_scored,
            "user_feedback_scored_episodes": satisfied_scored,
            "assistant_response_count": len(response_judgments),
            "user_episode_count": len(episode_judgments),
            "has_error": any(row.llm_error for row in response_judgments) or any(row.llm_error for row in episode_judgments),
            "response_helpful": {
                "yes": helpful_yes,
                "no": helpful_no,
                "uncertain": helpful_uncertain,
                "rate": helpful_yes / helpful_scored if helpful_scored else 0.0,
            },
            "user_satisfied": {
                "yes": satisfied_yes,
                "no": satisfied_no,
                "neutral": satisfied_neutral,
                "uncertain": satisfied_uncertain,
                "rate": satisfied_yes / satisfied_scored if satisfied_scored else 0.0,
            },
        }


class RoundFeedbackProcessor:
    """Run dual-level round feedback judgments with a shared global concurrency budget."""

    def __init__(self, store: Any, llm_client: Any, max_concurrency: int = 10, episode_min_share: float = 0.2):
        self.store = store
        self.llm = llm_client
        self.max_concurrency = max(1, max_concurrency)
        self.episode_min_share = min(max(episode_min_share, 0.0), 1.0)
        self.semaphore = asyncio.Semaphore(self.max_concurrency)
        self.write_lock = asyncio.Lock()
        self.context_builder = TurnContextBuilder()
        self.response_processor = ResponseHelpfulJudgmentProcessor(llm_client)
        self.episode_processor = UserSatisfiedJudgmentProcessor(llm_client)
        self.stats_aggregator = ToolStatsAggregator()

    async def process_sample(self, sample_ref: int | str, raw_json: dict[str, Any]) -> SampleJudgmentResult:
        sample_uid = self._resolve_sample_uid(sample_ref)
        messages = extract_messages_from_payload(raw_json)
        if not messages:
            tool_stats = {
                "response_helpful_rate": 0.0,
                "response_unhelpful_rate": 0.0,
                "user_satisfied_rate": 0.0,
                "user_negative_feedback_rate": 0.0,
                "assistant_response_count": 0,
                "user_episode_count": 0,
                "has_error": True,
                "error_reason": "no_messages",
            }
            async with self.write_lock:
                self.store.replace_round_feedback_results(sample_uid, 0, 0, [], [], tool_stats)
            return SampleJudgmentResult(sample_uid=sample_uid, response_judgments=[], episode_judgments=[], tool_stats=tool_stats)

        response_contexts = self.context_builder.extract_response_contexts(sample_uid, messages)
        episode_contexts = self.context_builder.extract_episode_contexts(sample_uid, messages)
        launch_plan = self._build_launch_plan(response_contexts, episode_contexts)

        tasks = [asyncio.create_task(self._run_planned_task(kind, context)) for kind, context in launch_plan]
        response_judgments: list[AssistantResponseJudgment] = []
        episode_judgments: list[UserEpisodeJudgment] = []

        for result in await asyncio.gather(*tasks):
            if isinstance(result, AssistantResponseJudgment):
                response_judgments.append(result)
            else:
                episode_judgments.append(result)

        response_judgments.sort(key=lambda row: row.response_index)
        episode_judgments.sort(key=lambda row: row.episode_index)
        tool_stats = self.stats_aggregator.aggregate(response_judgments, episode_judgments)

        async with self.write_lock:
            self.store.replace_round_feedback_results(
                sample_uid,
                len(response_contexts),
                len(episode_contexts),
                response_judgments,
                episode_judgments,
                tool_stats,
            )
        return SampleJudgmentResult(
            sample_uid=sample_uid,
            response_judgments=response_judgments,
            episode_judgments=episode_judgments,
            tool_stats=tool_stats,
        )

    async def process_batch(self, sample_batch: list[tuple[str, dict[str, Any]]]) -> tuple[int, int]:
        async def process_one(sample_uid: str, raw_json: dict[str, Any]) -> bool:
            try:
                await self.process_sample(sample_uid, raw_json)
                return True
            except Exception as exc:
                logger.exception("Failed to process sample %s", sample_uid)
                async with self.write_lock:
                    self.store.mark_sample_processing_failed(sample_uid, str(exc))
                return False

        results = await asyncio.gather(*(process_one(sample_uid, raw_json) for sample_uid, raw_json in sample_batch))
        success = sum(1 for item in results if item)
        return success, len(results) - success

    def _resolve_sample_uid(self, sample_ref: int | str) -> str:
        if isinstance(sample_ref, str):
            return sample_ref
        record = self.store.get_sample_by_id(sample_ref)
        if not record or not record.get("sample_uid"):
            raise ValueError(f"Unknown sample reference: {sample_ref}")
        return record["sample_uid"]

    def _build_launch_plan(
        self,
        response_contexts: list[AssistantResponseContext],
        episode_contexts: list[UserEpisodeContext],
    ) -> list[tuple[str, AssistantResponseContext | UserEpisodeContext]]:
        response_queue = deque(response_contexts)
        episode_queue = deque(episode_contexts)
        plan: list[tuple[str, AssistantResponseContext | UserEpisodeContext]] = []
        episode_taken = 0
        total_taken = 0
        min_share = max(1 / self.max_concurrency, self.episode_min_share) if episode_queue else 0.0

        while response_queue or episode_queue:
            choose_episode = False
            if episode_queue and not response_queue:
                choose_episode = True
            elif episode_queue and response_queue:
                current_share = episode_taken / total_taken if total_taken else 0.0
                choose_episode = current_share < min_share

            if choose_episode:
                plan.append(("episode", episode_queue.popleft()))
                episode_taken += 1
            else:
                plan.append(("response", response_queue.popleft()))
            total_taken += 1

        return plan

    async def _run_planned_task(
        self,
        kind: str,
        context: AssistantResponseContext | UserEpisodeContext,
    ) -> AssistantResponseJudgment | UserEpisodeJudgment:
        async with self.semaphore:
            if kind == "response":
                assert isinstance(context, AssistantResponseContext)
                return await self._judge_response_context(context)
            assert isinstance(context, UserEpisodeContext)
            return await self._judge_episode_context(context)

    async def _judge_response_context(self, context: AssistantResponseContext) -> AssistantResponseJudgment:
        prompt = self.context_builder.build_response_helpful_prompt(context)
        result = await self.response_processor.judge(prompt)
        return AssistantResponseJudgment(
            sample_uid=context.sample_uid,
            response_index=context.response_index,
            episode_index=context.episode_index,
            assistant_message_index=context.assistant_message_index,
            feedback_kind=context.feedback_kind,
            feedback_message_start_index=context.feedback_message_start_index,
            feedback_message_end_index=context.feedback_message_end_index,
            feedback_payload=context.feedback_payload,
            response_helpful=result,
            llm_error=result is None,
        )

    async def _judge_episode_context(self, context: UserEpisodeContext) -> UserEpisodeJudgment:
        prompt = self.context_builder.build_user_satisfied_prompt(context)
        result = await self.episode_processor.judge(prompt)
        return UserEpisodeJudgment(
            sample_uid=context.sample_uid,
            episode_index=context.episode_index,
            start_user_message_index=context.start_user_message_index,
            end_before_user_message_index=context.end_before_user_message_index,
            signal_from_users=context.signal_from_users,
            user_satisfied=result,
            llm_error=result is None,
        )


class PressureTest:
    """Basic preflight pressure test for the configured LLM endpoint."""

    def __init__(self, llm_client: Any):
        self.llm = llm_client

    async def _send_request(self) -> tuple[bool, float]:
        start = time.perf_counter()
        try:
            response = await self.llm.chat([{"role": "user", "content": "Answer only: yes"}], max_tokens=10)
            return "yes" in response.lower(), time.perf_counter() - start
        except Exception as exc:
            logger.error("Pressure test request failed: %s", exc)
            return False, time.perf_counter() - start

    async def run(
        self,
        max_concurrency: int,
        duration: int = 30,
        success_threshold: float = 0.95,
        p95_latency_threshold: float = 10.0,
        p99_latency_threshold: float = 30.0,
    ) -> bool:
        semaphore = asyncio.Semaphore(max(1, max_concurrency))
        results: list[tuple[bool, float]] = []
        start = time.perf_counter()

        async def bounded_request() -> tuple[bool, float]:
            async with semaphore:
                return await self._send_request()

        tasks: list[asyncio.Task[tuple[bool, float]]] = []
        while time.perf_counter() - start < duration:
            tasks.append(asyncio.create_task(bounded_request()))
            await asyncio.sleep(0.1)

        for result in await asyncio.gather(*tasks, return_exceptions=True):
            if isinstance(result, tuple):
                results.append(result)
            else:
                results.append((False, 0.0))

        success_rate = sum(1 for success, _ in results if success) / len(results) if results else 0.0
        latencies = sorted(latency for _, latency in results)
        p95 = latencies[int(len(latencies) * 0.95)] if latencies else 0.0
        p99 = latencies[int(len(latencies) * 0.99)] if latencies else 0.0

        if success_rate < success_threshold:
            return False
        if p95 > p95_latency_threshold or p99 > p99_latency_threshold:
            return False
        return True