"""RoundFeedbackProcessor - 逐轮反馈判断处理器"""
import asyncio
import json
import logging
import time
import re
from dataclasses import dataclass
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
        current_user = None
        assistant_parts: list[str] = []
        current_tool_calls: list[dict] = []
        tool_results: list[str] = []

        def flush_turn() -> None:
            if current_user is None:
                return
            if not assistant_parts and not current_tool_calls and not tool_results:
                return
            turns.append(TurnContext(
                turn_index=len(turns),
                user_message=current_user,
                assistant_message="\n".join(part for part in assistant_parts if part),
                tool_calls=list(current_tool_calls),
                tool_result="\n".join(tool_results) if tool_results else None,
                signal_users=[],
            ))

        for msg in messages:
            role = msg.get("role")
            content = self._extract_text_content(msg.get("content"))

            if role == "system":
                # Skip system messages
                continue

            elif role == "user":
                flush_turn()
                current_user = content
                assistant_parts = []
                current_tool_calls = []
                tool_results = []

            elif role == "assistant":
                if current_user is None:
                    continue
                if content:
                    assistant_parts.append(content)
                # Extract tool calls
                for tc in msg.get("tool_calls", []):
                    if isinstance(tc, dict) and "function" in tc:
                        current_tool_calls.append(tc["function"])

            elif role == "tool":
                if current_user is None:
                    continue
                if content:
                    tool_results.append(content)

        # Don't forget last turn
        flush_turn()

        # Now extract signal users for each turn
        turns = self._extract_signal_users(turns)

        return turns

    def _extract_signal_users(self, turns: list[TurnContext]) -> list[TurnContext]:
        """为每个turn提取后续最多3个user消息作为信号"""
        for i, turn in enumerate(turns):
            # Find user messages after this turn (excluding current turn's user)
            signal_users = []
            for j in range(i + 1, min(i + 4, len(turns))):
                if turns[j].user_message:
                    signal_users.append(turns[j].user_message)
            turn.signal_users = signal_users
        return turns

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

    def count_expected_turns(self, messages: list[dict]) -> int:
        """Count judged turns using the same grouping logic as extract_turns."""
        return len(self.extract_turns(messages))

    def build_judgment_prompt(self, turn: TurnContext, all_turns: list[TurnContext]) -> str:
        """构建判断prompt（简化版：只判断 response_helpful 和 user_satisfied）"""
        current_parts = []
        if turn.user_message:
            current_parts.append(self._format_message("user", turn.user_message))
        if turn.tool_result:
            current_parts.append(f"[tool_result]: {turn.tool_result}")
        if turn.assistant_message:
            current_parts.append(self._format_message("assistant", turn.assistant_message))

        current_section = "\n".join(current_parts)
        signal_section = "\n".join([f"[user]: {u}" for u in turn.signal_users]) if turn.signal_users else "(无后续用户消息)"

        return f"""=== 当前轮 ===
{current_section}

=== 后续用户信号（最多3轮）===
{signal_section}

请判断：
1. response_helpful: 这个回答对用户有帮助吗？（yes/no/uncertain）
2. user_satisfied: 用户对助手回复满意吗？（yes/no/uncertain/neutral）

答案格式：response_helpful=yes; user_satisfied=no

注意：
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
    def aggregate(judgments: list) -> dict:
        if not judgments:
            return {
                "response_helpful_rate": 0,
                "user_satisfied_rate": 0,
                "total_turns": 0,
                "has_error": False,
            }

        total = len(judgments)
        helpful_yes = sum(1 for j in judgments if j.response_helpful == "yes")
        satisfied_yes = sum(1 for j in judgments if j.user_satisfied == "yes")

        return {
            "response_helpful_rate": helpful_yes / total,
            "user_satisfied_rate": satisfied_yes / total,
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