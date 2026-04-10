"""Dual-level round feedback processor."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from claw_data_filter.models.round_judgment import AssistantResponseJudgment, FeedbackKind, UserEpisodeJudgment
from claw_data_filter.models.sample import extract_messages_from_payload, extract_normalized_messages

logger = logging.getLogger(__name__)


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
    assistant_reasoning: str = ""
    feedback_payload: list[str] = field(default_factory=list)
    execution_trace: list[str] = field(default_factory=list)
    prior_execution_background: list["ExecutionBackgroundStep"] = field(default_factory=list)


@dataclass(slots=True)
class ExecutionBackgroundStep:
    """Compressed summary for a prior assistant response step."""

    assistant_text_excerpt: str = ""
    assistant_reason_excerpt: str = ""
    tool_use_summary: str = ""
    tool_result_status_hint: str = "unknown"
    tool_result_excerpt_100: str = ""


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
    reasoning: str = ""
    tool_calls: list[dict[str, Any]] = field(default_factory=list)


class TurnContextBuilder:
    """Build dual-level judgment contexts from normalized conversations."""

    MAX_PRIOR_RESPONSE_STEPS = 3
    TEXT_EXCERPT_MAX_LEN = 160
    REASON_EXCERPT_MAX_LEN = 160
    TOOL_RESULT_EXCERPT_MAX_LEN = 100
    TOOL_ARG_TEXT_PREVIEW_LEN = 40
    TOOL_ARG_STRING_LIMIT = 120
    TOOL_USE_SUMMARY_LIMIT = 240
    HIGH_VALUE_TOOL_ARG_KEYS = (
        "path",
        "file_path",
        "target_path",
        "url",
        "urls",
        "query",
        "content",
        "text",
        "messages",
        "payload",
        "data",
        "input",
        "patch",
        "cmd",
        "command",
        "operation",
        "mode",
        "site",
        "pattern",
    )
    ERROR_PATTERNS = (
        r"\btraceback\b",
        r"\bexception\b",
        r"\berror\b",
        r"\bfailed\b",
        r"\bnot found\b",
        r"\bpermission denied\b",
        r"\btimeout\b",
        r"\bhttp\s+[45]\d\d\b",
        r"\bstatus\s*[:=]\s*[45]\d\d\b",
        r"\binvalid\b",
    )
    SUCCESS_PATTERNS = (
        r"\bsuccess(?:ful|fully)?\b",
        r"\bcompleted\b",
        r"\bfile written successfully\b",
        r"\bsaved successfully\b",
        r'"success"\s*:\s*true',
        r'"ok"\s*:\s*true',
        r"\bexit code\s*[:=]?\s*0\b",
    )

    def extract_response_contexts(self, sample_uid: str, messages: list[dict[str, Any]]) -> list[AssistantResponseContext]:
        events = self._normalize_messages(messages)
        contexts: list[AssistantResponseContext] = []
        episode_index = -1
        active_user_text = ""
        active_user_index: int | None = None
        event_index = 0
        prior_steps: deque[ExecutionBackgroundStep] = deque(maxlen=self.MAX_PRIOR_RESPONSE_STEPS)

        while event_index < len(events):
            event = events[event_index]
            if event.role == "user":
                active_user_text = event.text
                active_user_index = event.message_index
                episode_index += 1
                prior_steps.clear()
                event_index += 1
                continue

            if event.role != "assistant" or active_user_index is None:
                event_index += 1
                continue

            execution_trace: list[str] = []
            if event.text:
                execution_trace.append(self._format_message("assistant", event.text))
            if event.reasoning:
                execution_trace.append(self._format_reasoning(event.reasoning))
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

            context = AssistantResponseContext(
                sample_uid=sample_uid,
                response_index=len(contexts),
                episode_index=max(episode_index, 0),
                assistant_message_index=event.message_index,
                user_message=active_user_text,
                assistant_message=event.text,
                assistant_reasoning=event.reasoning,
                tool_calls=event.tool_calls,
                feedback_kind=feedback_kind,
                feedback_message_start_index=feedback_start,
                feedback_message_end_index=feedback_end,
                feedback_payload=feedback_payload,
                execution_trace=execution_trace,
                prior_execution_background=list(prior_steps),
            )
            contexts.append(context)
            prior_steps.append(self._build_execution_background_step(context))
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

    def build_response_progress_prompt(self, context: AssistantResponseContext) -> str:
        execution = "\n".join(context.execution_trace) if context.execution_trace else "(无 assistant 输出)"
        feedback_payload = "\n".join(context.feedback_payload) if context.feedback_payload else "(无紧邻反馈块)"
        execution_background = self._render_execution_background(context.prior_execution_background)
        feedback_label = {
            FeedbackKind.TOOL_RESULT: "紧邻 tool result",
            FeedbackKind.USER: "紧邻 user 反馈",
            FeedbackKind.NONE: "无紧邻反馈",
        }[context.feedback_kind]
        return f"""你要判断 assistant 的当前响应单元是否让当前问题状态发生正向推进。

=== 当前用户请求 ===
{self._format_message('user', context.user_message)}

=== 当前单元之前的执行背景（仅供理解当前阶段） ===
{execution_background}

=== 当前 assistant 响应单元 ===
{execution}

=== 紧邻反馈块类型 ===
{feedback_label}

=== 紧邻反馈块内容 ===
{feedback_payload}

=== 判定规则 ===
- yes: 当前 step 方向基本正确，并拿到了有效信息、完成了必要中间步骤，或明确把问题推进到更接近解决的状态。
- no: 当前 step 明显跑偏、执行无效、引入返工，或没有为当前问题带来正向推进。
- uncertain: 证据不足，无法判断当前 step 是否真正推进了问题。
- 允许参考前序执行背景理解当前阶段，但不要把前序步骤的功劳或失败直接转嫁到当前 step。
- 不要脑补 next assistant 的补救内容，只基于当前 unit 与紧邻反馈块判断。

只输出一行：response_progress=yes|no|uncertain"""

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
                reasoning = self._extract_reasoning_content(message.get("content"))
                tool_calls = self._extract_tool_calls(message)
                if text or reasoning or tool_calls:
                    events.append(
                        ConversationEvent(
                            role="assistant",
                            message_index=message_index,
                            text=text,
                            reasoning=reasoning,
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

    def _extract_reasoning_content(self, content: Any) -> str:
        if not isinstance(content, list):
            return ""
        parts: list[str] = []
        for item in content:
            if not isinstance(item, dict):
                continue
            block_type = item.get("type")
            if block_type in {"thinking", "reasoning", "thought"}:
                parts.append(str(item.get("thinking") or item.get("text") or item.get("content") or ""))
        return "".join(parts)

    def _format_message(self, role: str, content: str, max_len: int = 500) -> str:
        rendered = content[:max_len] + "..." if len(content) > max_len else content
        return f"[{role}]: {rendered}"

    def _format_reasoning(self, content: str, max_len: int = 500) -> str:
        rendered = content[:max_len] + "..." if len(content) > max_len else content
        return f"[assistant_reasoning]: {rendered}"

    def _format_tool_call(self, tool_call: dict[str, Any], max_len: int = 500) -> str:
        arguments = tool_call.get("arguments", "")
        if not isinstance(arguments, str):
            arguments = json.dumps(arguments, ensure_ascii=False, sort_keys=True)
        rendered = arguments[:max_len] + "..." if len(arguments) > max_len else arguments
        return f"[assistant_tool_use]: {tool_call.get('name', 'unknown')}({rendered})"

    def _format_tool_result(self, content: str, max_len: int = 500) -> str:
        rendered = content[:max_len] + "..." if len(content) > max_len else content
        return f"[tool_result]: {rendered}"

    def _build_execution_background_step(self, context: AssistantResponseContext) -> ExecutionBackgroundStep:
        tool_result_excerpt = context.feedback_payload[0][: self.TOOL_RESULT_EXCERPT_MAX_LEN] if context.feedback_kind == FeedbackKind.TOOL_RESULT and context.feedback_payload else ""
        tool_result_status_hint = (
            self._infer_tool_result_status_hint(context.feedback_payload)
            if context.feedback_kind == FeedbackKind.TOOL_RESULT
            else "unknown"
        )
        tool_use_summary = "; ".join(self._summarize_tool_call(tool_call) for tool_call in context.tool_calls[:2])
        return ExecutionBackgroundStep(
            assistant_text_excerpt=self._make_excerpt(context.assistant_message, self.TEXT_EXCERPT_MAX_LEN),
            assistant_reason_excerpt=self._make_excerpt(context.assistant_reasoning, self.REASON_EXCERPT_MAX_LEN),
            tool_use_summary=tool_use_summary,
            tool_result_status_hint=tool_result_status_hint,
            tool_result_excerpt_100=tool_result_excerpt,
        )

    def _render_execution_background(self, steps: list[ExecutionBackgroundStep]) -> str:
        if not steps:
            return "(无前序执行背景)"
        rendered: list[str] = []
        total = len(steps)
        for offset, step in enumerate(steps):
            rendered.append(f"Step -{total - offset}:")
            if step.assistant_text_excerpt:
                rendered.append(f"- assistant_text_excerpt: {step.assistant_text_excerpt}")
            if step.assistant_reason_excerpt:
                rendered.append(f"- assistant_reason_excerpt: {step.assistant_reason_excerpt}")
            if step.tool_use_summary:
                rendered.append(f"- tool_use_summary: {step.tool_use_summary}")
            rendered.append(f"- tool_result_status_hint: {step.tool_result_status_hint}")
            if step.tool_result_excerpt_100:
                rendered.append(f"- tool_result_excerpt_100: {step.tool_result_excerpt_100}")
            rendered.append("")
        return "\n".join(rendered).rstrip()

    def _make_excerpt(self, text: str, max_len: int) -> str:
        if not text:
            return ""
        stripped = re.sub(r"\s+", " ", text).strip()
        if len(stripped) <= max_len:
            return stripped
        return stripped[:max_len] + "..."

    def _infer_tool_result_status_hint(self, feedback_payload: list[str]) -> str:
        combined = "\n".join(item for item in feedback_payload if item).strip()
        if not combined:
            return "unknown"
        for pattern in self.ERROR_PATTERNS:
            if re.search(pattern, combined, re.IGNORECASE):
                return "error"
        for pattern in self.SUCCESS_PATTERNS:
            if re.search(pattern, combined, re.IGNORECASE):
                return "success"
        return "unknown"

    def _summarize_tool_call(self, tool_call: dict[str, Any]) -> str:
        tool_name = tool_call.get("name", "unknown") or "unknown"
        arguments = tool_call.get("arguments", "")
        parsed_arguments = self._parse_tool_arguments(arguments)
        if not isinstance(parsed_arguments, dict):
            argument_summary = self._summarize_argument_value(parsed_arguments)
            return self._clip_tool_use_summary(f"{tool_name}(arguments={argument_summary})")

        selected_keys = self._select_tool_argument_keys(parsed_arguments)
        rendered_parts = [f"{key}={self._summarize_argument_value(parsed_arguments.get(key))}" for key in selected_keys]
        return self._clip_tool_use_summary(f"{tool_name}({', '.join(rendered_parts)})")

    def _parse_tool_arguments(self, arguments: Any) -> Any:
        if isinstance(arguments, (dict, list)):
            return arguments
        if not isinstance(arguments, str):
            return arguments
        stripped = arguments.strip()
        if not stripped:
            return {}
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            return stripped

    def _select_tool_argument_keys(self, arguments: dict[str, Any]) -> list[str]:
        keys = [key for key in self.HIGH_VALUE_TOOL_ARG_KEYS if key in arguments]
        if len(keys) < 3:
            for key in arguments:
                if key in keys:
                    continue
                keys.append(key)
                if len(keys) == 3:
                    break
        return keys[:3]

    def _summarize_argument_value(self, value: Any) -> str:
        if isinstance(value, str):
            compact = re.sub(r"\s+", " ", value).strip()
            if len(compact) <= self.TOOL_ARG_STRING_LIMIT:
                return compact
            prefix = compact[: self.TOOL_ARG_TEXT_PREVIEW_LEN].replace('"', "\\\"")
            return f'<text:{len(compact)} chars, prefix="{prefix}...">'
        if isinstance(value, list):
            return f"<list:{len(value)} items>"
        if isinstance(value, dict):
            return f"<dict:{len(value)} keys>"
        return json.dumps(value, ensure_ascii=False)

    def _clip_tool_use_summary(self, summary: str) -> str:
        if len(summary) <= self.TOOL_USE_SUMMARY_LIMIT:
            return summary
        return summary[: self.TOOL_USE_SUMMARY_LIMIT - 3] + "..."


class ResponseProgressJudgmentProcessor:
    """LLM processor for assistant response progress."""

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
                logger.warning("Attempt %s: failed to parse response progress output", attempt + 1)
            except Exception as exc:
                logger.warning("Attempt %s: response progress LLM call failed: %s", attempt + 1, exc)
                if attempt < self.max_retries:
                    await asyncio.sleep(2**attempt)
                else:
                    return None
        return None

    def _parse_response(self, response: str) -> str | None:
        return _parse_judgment_label(response, "response_progress", ["yes", "no", "uncertain"])


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
        return _parse_judgment_label(response, "user_satisfied", ["yes", "no", "uncertain", "neutral"])


def _parse_judgment_label(response: str, field_name: str, allowed_values: list[str]) -> str | None:
    cleaned = re.sub(r"<think>.*?</think>", " ", response or "", flags=re.IGNORECASE | re.DOTALL).strip()
    allowed_pattern = "|".join(re.escape(value) for value in allowed_values)

    explicit_match = re.search(
        rf"\b{re.escape(field_name)}\b\s*[:=]\s*({allowed_pattern})\b",
        cleaned,
        re.IGNORECASE,
    )
    if explicit_match:
        return explicit_match.group(1).lower()

    normalized = cleaned.strip().strip("`'\" ").strip()
    bare_match = re.fullmatch(rf"({allowed_pattern})[。.!]?", normalized, re.IGNORECASE)
    return bare_match.group(1).lower() if bare_match else None


class ToolStatsAggregator:
    """Aggregate dual-level judgments into sample-level stats."""

    @staticmethod
    def aggregate(
        response_judgments: list[AssistantResponseJudgment],
        episode_judgments: list[UserEpisodeJudgment],
    ) -> dict[str, Any]:
        progress_yes = sum(1 for row in response_judgments if row.response_progress == "yes")
        progress_no = sum(1 for row in response_judgments if row.response_progress == "no")
        progress_uncertain = sum(1 for row in response_judgments if row.response_progress == "uncertain")
        satisfied_yes = sum(1 for row in episode_judgments if row.user_satisfied == "yes")
        satisfied_no = sum(1 for row in episode_judgments if row.user_satisfied == "no")
        satisfied_neutral = sum(1 for row in episode_judgments if row.user_satisfied == "neutral")
        satisfied_uncertain = sum(1 for row in episode_judgments if row.user_satisfied == "uncertain")
        progress_scored = progress_yes + progress_no
        satisfied_scored = satisfied_yes + satisfied_no + satisfied_neutral
        return {
            "response_progress_rate": progress_yes / progress_scored if progress_scored else 0.0,
            "response_regress_rate": progress_no / progress_scored if progress_scored else 0.0,
            "user_satisfied_rate": satisfied_yes / satisfied_scored if satisfied_scored else 0.0,
            "user_negative_feedback_rate": satisfied_no / satisfied_scored if satisfied_scored else 0.0,
            "response_progress_scored_steps": progress_scored,
            "user_feedback_scored_episodes": satisfied_scored,
            "assistant_response_count": len(response_judgments),
            "user_episode_count": len(episode_judgments),
            "has_error": any(row.llm_error for row in response_judgments) or any(row.llm_error for row in episode_judgments),
            "response_progress": {
                "yes": progress_yes,
                "no": progress_no,
                "uncertain": progress_uncertain,
                "rate": progress_yes / progress_scored if progress_scored else 0.0,
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
        self.response_processor = ResponseProgressJudgmentProcessor(llm_client)
        self.episode_processor = UserSatisfiedJudgmentProcessor(llm_client)
        self.stats_aggregator = ToolStatsAggregator()

    async def process_sample(self, sample_uid: str, raw_json: dict[str, Any]) -> SampleJudgmentResult:
        messages = extract_messages_from_payload(raw_json)
        if not messages:
            tool_stats = {
                "response_progress_rate": 0.0,
                "response_regress_rate": 0.0,
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
        prompt = self.context_builder.build_response_progress_prompt(context)
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
            response_progress=result,
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