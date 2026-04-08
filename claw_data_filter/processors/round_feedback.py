"""RoundFeedbackProcessor - 逐轮反馈判断处理器"""
import asyncio
import json
import logging
import time
import re
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class TurnContext:
    """单轮上下文"""
    turn_index: int
    user_message: str
    assistant_message: str
    tool_calls: list[dict]
    tool_result: str | None
    signal_users: list[str]
    execution_trace: list[str] = field(default_factory=list)


@dataclass
class ConversationEvent:
    """归一化后的对话事件。"""
    kind: str
    text: str = ""
    tool_calls: list[dict] = field(default_factory=list)
    tool_results: list[str] = field(default_factory=list)


class TurnContextBuilder:
    """构建每轮判断输入上下文"""

    def extract_turns(self, messages: list[dict]) -> list[TurnContext]:
        """从对话消息列表中提取所有轮次

        Args:
            messages: 原始消息列表

        Returns:
            TurnContext 列表
        """
        turns = []
        events = self._normalize_messages(messages)
        event_index = 0

        while event_index < len(events):
            event = events[event_index]
            if event.kind != "user":
                event_index += 1
                continue

            assistant_parts: list[str] = []
            current_tool_calls: list[dict] = []
            tool_results: list[str] = []
            execution_trace: list[str] = []
            next_index = event_index + 1

            while next_index < len(events):
                next_event = events[next_index]
                if next_event.kind == "user":
                    break
                if next_event.kind == "assistant":
                    if next_event.text:
                        assistant_parts.append(next_event.text)
                        execution_trace.append(self._format_message("assistant", next_event.text))
                    for tool_call in next_event.tool_calls:
                        current_tool_calls.append(tool_call)
                        execution_trace.append(self._format_tool_call(tool_call))
                elif next_event.kind == "tool_result":
                    for tool_result in next_event.tool_results:
                        tool_results.append(tool_result)
                        execution_trace.append(self._format_tool_result(tool_result))
                next_index += 1

            if assistant_parts or current_tool_calls or tool_results:
                turns.append(TurnContext(
                    turn_index=len(turns),
                    user_message=event.text,
                    assistant_message="\n".join(part for part in assistant_parts if part),
                    tool_calls=current_tool_calls,
                    tool_result="\n".join(tool_results) if tool_results else None,
                    signal_users=[],
                    execution_trace=execution_trace,
                ))

            event_index = next_index

        return self._extract_signal_users(turns)

    def _extract_signal_users(self, turns: list[TurnContext]) -> list[TurnContext]:
        """为每个turn提取后续最多3轮真实user消息作为信号。"""
        for i, turn in enumerate(turns):
            signal_users = []
            for j in range(i + 1, min(i + 4, len(turns))):
                if turns[j].user_message:
                    signal_users.append(turns[j].user_message)
            turn.signal_users = signal_users
        return turns

    def _normalize_messages(self, messages: list[dict]) -> list[ConversationEvent]:
        """将 OpenAI/Anthropic 风格消息转换为统一事件流。"""
        events: list[ConversationEvent] = []

        for msg in messages:
            role = msg.get("role")

            if role == "system":
                continue

            if role == "user":
                user_text, tool_results = self._extract_user_content(msg.get("content"))
                for tool_result in tool_results:
                    events.append(ConversationEvent(kind="tool_result", tool_results=[tool_result]))
                if user_text:
                    events.append(ConversationEvent(kind="user", text=user_text))
                continue

            if role == "assistant":
                assistant_text = self._extract_text_content(msg.get("content"))
                tool_calls = self._extract_tool_calls(msg)
                if assistant_text or tool_calls:
                    events.append(ConversationEvent(kind="assistant", text=assistant_text, tool_calls=tool_calls))
                continue

            if role == "tool":
                tool_result = self._extract_text_content(msg.get("content"))
                if tool_result:
                    events.append(ConversationEvent(kind="tool_result", tool_results=[tool_result]))

        return events

    def _extract_user_content(self, content: Any) -> tuple[str, list[str]]:
        """提取 user 文本和其中携带的 tool_result。"""
        if content is None:
            return "", []
        if isinstance(content, str):
            return content, []
        if not isinstance(content, list):
            return str(content), []

        text_parts: list[str] = []
        tool_results: list[str] = []
        for part in content:
            if not isinstance(part, dict):
                continue
            part_type = part.get("type")
            if part_type == "text":
                text_parts.append(part.get("text", ""))
            elif part_type == "tool_result":
                tool_result = self._extract_tool_result_content(part.get("content"))
                if tool_result:
                    tool_results.append(tool_result)

        return "".join(text_parts), tool_results

    def _extract_tool_result_content(self, content: Any) -> str:
        """提取 tool_result 文本内容。"""
        if content is None:
            return ""
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts = []
            for item in content:
                if isinstance(item, str):
                    parts.append(item)
                elif isinstance(item, dict):
                    if item.get("type") == "text":
                        parts.append(item.get("text", ""))
                    else:
                        parts.append(str(item))
                else:
                    parts.append(str(item))
            return "".join(parts)
        return str(content)

    def _extract_tool_calls(self, msg: dict) -> list[dict]:
        """统一提取 OpenAI/Anthropic 风格的 tool_use 信息。"""
        tool_calls: list[dict] = []

        for tool_call in msg.get("tool_calls", []):
            if isinstance(tool_call, dict) and isinstance(tool_call.get("function"), dict):
                function = tool_call["function"]
                tool_calls.append({
                    "name": function.get("name", ""),
                    "arguments": function.get("arguments", ""),
                })

        content = msg.get("content")
        if isinstance(content, list):
            for part in content:
                if not isinstance(part, dict) or part.get("type") != "tool_use":
                    continue
                tool_calls.append({
                    "name": part.get("name", ""),
                    "arguments": json.dumps(part.get("input", {}), ensure_ascii=False, sort_keys=True),
                })

        return tool_calls

    def _extract_text_content(self, content: Any) -> str:
        """Extract text from content field"""
        if content is None:
            return ""
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts = []
            for part in content:
                if isinstance(part, dict):
                    if part.get("type") == "text":
                        parts.append(part.get("text", ""))
            return "".join(parts)
        return str(content)

    def _format_message(self, role: str, content: str, max_len: int = 500) -> str:
        """Format a single message for prompt"""
        if len(content) > max_len:
            content = content[:max_len] + "..."
        return f"[{role}]: {content}"

    def _format_tool_call(self, tool_call: dict, max_len: int = 500) -> str:
        """Format tool use for prompt."""
        arguments = tool_call.get("arguments", "")
        if not isinstance(arguments, str):
            arguments = json.dumps(arguments, ensure_ascii=False, sort_keys=True)
        if len(arguments) > max_len:
            arguments = arguments[:max_len] + "..."
        return f"[assistant_tool_use]: {tool_call.get('name', 'unknown')}({arguments})"

    def _format_tool_result(self, content: str, max_len: int = 500) -> str:
        """Format tool result for prompt."""
        if len(content) > max_len:
            content = content[:max_len] + "..."
        return f"[tool_result]: {content}"

    def count_expected_turns(self, messages: list[dict]) -> int:
        """Count judged turns using the same grouping logic as extract_turns."""
        return len(self.extract_turns(messages))

    def build_judgment_prompt(self, turn: TurnContext, all_turns: list[TurnContext]) -> str:
        """构建判断prompt（简化版：只判断 response_helpful 和 user_satisfied）"""
        current_user = self._format_message("user", turn.user_message) if turn.user_message else "(无当前用户请求)"
        execution_section = "\n".join(turn.execution_trace) if turn.execution_trace else "(无执行结果)"
        signal_section = "\n".join([self._format_message("user", user) for user in turn.signal_users]) if turn.signal_users else "(无后续真实用户消息)"

        return f"""=== 当前用户请求 ===
{current_user}

=== 当前assistant执行链 ===
{execution_section}

=== 后续真实用户反馈（最多3轮，已跳过仅tool_result轮）===
{signal_section}

请判断：
1. response_helpful: 综合当前assistant的text、tool use、tool result，以及执行链中的后续assistant continuation，这次响应对用户有帮助吗？（yes/no/uncertain）
2. user_satisfied: 仅根据后续真实用户反馈判断用户是否满意；纯tool result不算满意度反馈。（yes/no/uncertain/neutral）

答案格式：response_helpful=yes; user_satisfied=no

注意：
- system reminder、plan mode 提示、tool 框架提示、工具中断提示等系统/框架文本可能混在对话里；把它们视为上下文信息，只有在确实影响任务结果时才纳入判断，不要默认当作用户真实诉求或满意度反馈
- 用户追问（要求补充/澄清） → user_satisfied=no
- 用户确认/继续/满意 → user_satisfied=yes
- 用户转向新话题 → user_satisfied=neutral
- 无明确反馈 → user_satisfied=uncertain"""


from concurrent.futures import ThreadPoolExecutor

from claw_data_filter.models.round_judgment import RoundJudgment
from claw_data_filter.models.sample import extract_messages_from_payload


class RoundJudgmentProcessor:
    """异步执行单轮2维度判断（response_helpful, user_satisfied）"""

    def __init__(self, llm_client, max_retries: int = 2):
        self.llm = llm_client
        self.max_retries = max_retries

    async def judge(self, prompt: str) -> dict | None:
        return await self._call_llm_with_retry(prompt, self._parse_response)

    async def _call_llm_with_retry(self, prompt: str, parser) -> dict | None:
        for attempt in range(self.max_retries + 1):
            try:
                response = await self.llm.chat([{"role": "user", "content": prompt}], max_tokens=50)
                result = parser(response)
                if result is not None:
                    return result
                logger.warning(f"Attempt {attempt + 1}: Failed to parse response")
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}: LLM call failed: {e}")
                if attempt < self.max_retries:
                    await asyncio.sleep(2 ** attempt)
                else:
                    return None
        return None

    def _parse_response(self, response: str) -> dict | None:
        response = response.strip()
        result = {}

        helpful_match = re.search(r"response_helpful\s*=\s*(yes|no|uncertain)", response, re.IGNORECASE)
        if helpful_match:
            result["response_helpful"] = helpful_match.group(1).lower()
        else:
            return None

        satisfied_match = re.search(r"user_satisfied\s*=\s*(yes|no|uncertain|neutral)", response, re.IGNORECASE)
        if satisfied_match:
            result["user_satisfied"] = satisfied_match.group(1).lower()
        else:
            return None

        return result

    async def process_turn(self, turn: TurnContext, all_turns: list[TurnContext], builder: TurnContextBuilder):
        prompt = builder.build_judgment_prompt(turn, all_turns)
        result = await self.judge(prompt)

        from claw_data_filter.models.round_judgment import RoundJudgment
        return RoundJudgment(
            sample_id=0,
            turn_index=turn.turn_index,
            response_helpful=result.get("response_helpful") if result else None,
            user_satisfied=result.get("user_satisfied") if result else None,
            signal_from_users=turn.signal_users,
            llm_error=result is None,
        )


class ToolStatsAggregator:
    @staticmethod
    def _safe_rate(numerator: int, denominator: int) -> float:
        if denominator <= 0:
            return 0.0
        return numerator / denominator

    @staticmethod
    def aggregate(judgments: list) -> dict:
        if not judgments:
            return {
                "response_helpful_rate": 0.0,
                "response_unhelpful_rate": 0.0,
                "user_satisfied_rate": 0.0,
                "user_negative_feedback_rate": 0.0,
                "response_helpful_scored_turns": 0,
                "user_feedback_scored_turns": 0,
                "total_turns": 0,
                "has_error": False,
            }

        total = len(judgments)
        helpful_yes = sum(1 for j in judgments if j.response_helpful == "yes")
        helpful_no = sum(1 for j in judgments if j.response_helpful == "no")
        satisfied_yes = sum(1 for j in judgments if j.user_satisfied == "yes")
        satisfied_no = sum(1 for j in judgments if j.user_satisfied == "no")
        satisfied_neutral = sum(1 for j in judgments if j.user_satisfied == "neutral")

        response_helpful_scored_turns = helpful_yes + helpful_no
        user_feedback_scored_turns = satisfied_yes + satisfied_no + satisfied_neutral

        return {
            "response_helpful_rate": ToolStatsAggregator._safe_rate(helpful_yes, response_helpful_scored_turns),
            "response_unhelpful_rate": ToolStatsAggregator._safe_rate(helpful_no, response_helpful_scored_turns),
            "user_satisfied_rate": ToolStatsAggregator._safe_rate(satisfied_yes, user_feedback_scored_turns),
            "user_negative_feedback_rate": ToolStatsAggregator._safe_rate(satisfied_no, user_feedback_scored_turns),
            "response_helpful_scored_turns": response_helpful_scored_turns,
            "user_feedback_scored_turns": user_feedback_scored_turns,
            "total_turns": total,
            "has_error": any(j.llm_error for j in judgments),
        }


# Keep RoundFeedbackProcessor and PressureTest classes unchanged for now
class RoundFeedbackProcessor:
    """主处理器：协调整个流程"""

    def __init__(
        self,
        store: "DuckDBStore",
        llm_client: "AsyncLLMClient",
        max_concurrency: int = 10,
    ):
        self.store = store
        self.llm = llm_client
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.write_lock = asyncio.Lock()
        self.context_builder = TurnContextBuilder()
        self.judgment_processor = RoundJudgmentProcessor(llm_client)
        self.stats_aggregator = ToolStatsAggregator()

    async def process_sample(self, sample_id: int, raw_json: dict) -> list[RoundJudgment]:
        """处理单条sample的所有turn"""
        messages = extract_messages_from_payload(raw_json)
        if not messages:
            tool_stats = self.stats_aggregator.aggregate([])
            tool_stats["has_error"] = True
            tool_stats["error_reason"] = "no_messages"
            async with self.write_lock:
                self.store.replace_round_feedback_results(sample_id, 0, [], tool_stats)
            return []

        # Extract turns
        turns = self.context_builder.extract_turns(messages)
        if not turns:
            tool_stats = self.stats_aggregator.aggregate([])
            async with self.write_lock:
                self.store.replace_round_feedback_results(sample_id, 0, [], tool_stats)
            return []

        # Process turns with concurrency control
        tasks = []
        for turn in turns:
            task = self._process_turn_with_semaphore(sample_id, turn, turns)
            tasks.append(task)

        judgments = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out exceptions, convert to RoundJudgment
        valid_judgments = []
        for j in judgments:
            if isinstance(j, RoundJudgment):
                valid_judgments.append(j)
            else:
                logger.error(f"Turn processing failed: {j}")

        for j in valid_judgments:
            j.sample_id = sample_id

        tool_stats = self.stats_aggregator.aggregate(valid_judgments)
        if len(valid_judgments) != len(turns):
            tool_stats["has_error"] = True
            tool_stats["error_reason"] = "incomplete_turn_processing"

        async with self.write_lock:
            self.store.replace_round_feedback_results(sample_id, len(turns), valid_judgments, tool_stats)

        return valid_judgments

    async def _process_turn_with_semaphore(
        self, sample_id: int, turn: TurnContext, all_turns: list[TurnContext]
    ) -> RoundJudgment:
        """使用信号量控制并发处理单个turn"""
        async with self.semaphore:
            return await self.judgment_processor.process_turn(turn, all_turns, self.context_builder)

    async def process_batch(self, sample_batch: list[tuple[int, dict]]) -> tuple[int, int]:
        """批量处理多个sample（并行）"""
        async def process_one(sample_id: int, raw_json: dict) -> bool:
            """处理单个sample，返回是否成功"""
            try:
                await self.process_sample(sample_id, raw_json)
                return True
            except Exception as e:
                logger.error(f"Failed to process sample {sample_id}: {e}")
                async with self.write_lock:
                    self.store.mark_sample_processing_failed(sample_id, str(e))
                return False

        # 并行处理所有samples
        tasks = [process_one(sid, rj) for sid, rj in sample_batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 统计结果
        success = sum(1 for r in results if r is True)
        failures = len(results) - success

        return success, failures


class PressureTest:
    """启动前压力测试"""

    def __init__(self, llm_client: "AsyncLLMClient"):
        self.llm = llm_client

    async def _send_request(self) -> tuple[bool, float]:
        """发送单个测试请求，返回 (success, latency)"""
        import time
        start = time.perf_counter()
        try:
            response = await self.llm.chat(
                [{"role": "user", "content": "Answer only: yes"}],
                max_tokens=10,
            )
            latency = time.perf_counter() - start
            return "yes" in response.lower(), latency
        except Exception as e:
            latency = time.perf_counter() - start
            logger.error(f"Pressure test request failed: {e}")
            return False, latency

    async def run(
        self,
        max_concurrency: int,
        duration: int = 30,
        success_threshold: float = 0.95,
        p95_latency_threshold: float = 10.0,
        p99_latency_threshold: float = 30.0,
    ) -> bool:
        """运行压力测试

        Args:
            max_concurrency: 最大并发数
            duration: 测试持续时间（秒）
            success_threshold: 成功率阈值
            p95_latency_threshold: P95延迟阈值（秒）
            p99_latency_threshold: P99延迟阈值（秒）

        Returns:
            True if all metrics pass, False otherwise
        """
        logger.info(f"Starting pressure test: concurrency={max_concurrency}, duration={duration}s")

        semaphore = asyncio.Semaphore(max_concurrency)
        results: list[tuple[bool, float]] = []
        start_time = time.perf_counter()

        async def bounded_request():
            async with semaphore:
                return await self._send_request()

        # Run requests until duration expires
        tasks = []
        while time.perf_counter() - start_time < duration:
            task = asyncio.create_task(bounded_request())
            tasks.append(task)
            await asyncio.sleep(0.1)  # Small delay to avoid spawning too fast

        # Wait for all tasks to complete
        all_results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in all_results:
            if isinstance(r, tuple):
                results.append(r)
            else:
                results.append((False, 0))

        # Calculate metrics
        total = len(results)
        successes = sum(1 for success, _ in results if success)
        latencies = sorted([lat for _, lat in results])

        success_rate = successes / total if total > 0 else 0
        p50 = latencies[int(len(latencies) * 0.5)] if latencies else 0
        p95 = latencies[int(len(latencies) * 0.95)] if latencies else 0
        p99 = latencies[int(len(latencies) * 0.99)] if latencies else 0

        logger.info(f"Pressure test results: success_rate={success_rate:.2%}, "
                   f"p50={p50:.2f}s, p95={p95:.2f}s, p99={p99:.2f}s")

        # Check thresholds
        passed = True
        if success_rate < success_threshold:
            logger.error(f"Success rate {success_rate:.2%} < {success_threshold:.2%}")
            passed = False
        if p95 > p95_latency_threshold:
            logger.error(f"P95 latency {p95:.2f}s > {p95_latency_threshold}s")
            passed = False
        if p99 > p99_latency_threshold:
            logger.error(f"P99 latency {p99:.2f}s > {p99_latency_threshold}s")
            passed = False

        if passed:
            logger.info("Pressure test PASSED")
        else:
            logger.error("Pressure test FAILED")

        return passed