"""RoundFeedbackProcessor - 逐轮反馈判断处理器"""
import json
import logging
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
    signal_users: list[str]  # 后续最多3个user消息


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
        current_assistant = None
        current_tool_calls = []
        current_tool_result = None

        for i, msg in enumerate(messages):
            role = msg.get("role")
            content = self._extract_text_content(msg.get("content"))

            if role == "system":
                # Skip system messages
                continue

            elif role == "user":
                # If we have a complete turn, save it
                if current_assistant is not None:
                    turns.append(TurnContext(
                        turn_index=len(turns),
                        user_message=current_user or "",
                        assistant_message=current_assistant,
                        tool_calls=current_tool_calls,
                        tool_result=current_tool_result,
                        signal_users=[],
                    ))
                    current_user = None
                    current_assistant = None
                    current_tool_calls = []
                    current_tool_result = None
                # Start new turn with this user
                current_user = content

            elif role == "assistant":
                # If there's already an assistant (consecutive assistants),
                # save the previous one first
                if current_assistant is not None:
                    turns.append(TurnContext(
                        turn_index=len(turns),
                        user_message="",
                        assistant_message=current_assistant,
                        tool_calls=current_tool_calls,
                        tool_result=current_tool_result,
                        signal_users=[],
                    ))
                    current_tool_calls = []
                    current_tool_result = None
                # This is the assistant response for current user (or pending)
                current_assistant = content
                # Extract tool calls
                for tc in msg.get("tool_calls", []):
                    if isinstance(tc, dict) and "function" in tc:
                        current_tool_calls.append(tc["function"])

            elif role == "tool":
                # Tool result - belongs to current assistant
                current_tool_result = content

        # Don't forget last turn
        if current_assistant is not None:
            turns.append(TurnContext(
                turn_index=len(turns),
                user_message=current_user or "",
                assistant_message=current_assistant,
                tool_calls=current_tool_calls,
                tool_result=current_tool_result,
                signal_users=[],
            ))

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

    def build_group1_prompt(self, turn: TurnContext, all_turns: list[TurnContext]) -> str:
        """构建工具相关判断的prompt

        Args:
            turn: 当前轮上下文
            all_turns: 所有轮次（用于构建历史）

        Returns:
            格式化的 prompt 字符串
        """
        # Build history (only user + assistant, no tools)
        history_parts = []
        for i, t in enumerate(all_turns[:turn.turn_index]):
            if t.user_message:
                history_parts.append(self._format_message("user", t.user_message))
            history_parts.append(self._format_message("assistant", t.assistant_message))

        history_section = "\n".join(history_parts) if history_parts else "(无历史对话)"

        # Build current turn
        current_parts = []
        if turn.user_message:
            current_parts.append(self._format_message("user", turn.user_message))
        if turn.tool_result:
            current_parts.append(f"[tool_result]: {turn.tool_result}")
        if turn.assistant_message:
            current_parts.append(self._format_message("assistant", turn.assistant_message))
        if turn.tool_calls:
            tool_names = [tc.get("name", "unknown") for tc in turn.tool_calls]
            current_parts.append(f"[工具调用]: {', '.join(tool_names)}")

        current_section = "\n".join(current_parts)

        prompt = f"""=== 历史对话（仅user/assistant）===
{history_section}

=== 当前轮 ===
{current_section}

请判断：
1. need_tool: 当前问题是否需要工具调用？（yes/no/uncertain）
2. tool_correct: 如果用了工具，工具选择正确吗？（yes/no/uncertain）

答案格式：need_tool=yes; tool_correct=no

注意：
- need_tool=no 但实际用了工具 → tool_correct=no
- need_tool=yes 但没用工具 → tool_correct=no
- need_tool=uncertain 时 → tool_correct=uncertain"""

        return prompt

    def build_group2_prompt(self, turn: TurnContext, all_turns: list[TurnContext]) -> str:
        """构建效果相关判断的prompt

        Args:
            turn: 当前轮上下文
            all_turns: 所有轮次

        Returns:
            格式化的 prompt 字符串
        """
        # Build current turn
        current_parts = []
        if turn.user_message:
            current_parts.append(self._format_message("user", turn.user_message))
        if turn.tool_result:
            current_parts.append(f"[tool_result]: {turn.tool_result}")
        if turn.assistant_message:
            current_parts.append(self._format_message("assistant", turn.assistant_message))

        current_section = "\n".join(current_parts)

        # Build signal users
        signal_section = "\n".join([f"[user]: {u}" for u in turn.signal_users]) if turn.signal_users else "(无后续用户消息)"

        prompt = f"""=== 当前轮 ===
{current_section}

=== 后续用户信号（最多3轮）===
{signal_section}

请判断：
1. response_helpful: 这个回答对用户有帮助吗？（yes/no/uncertain）
2. user_satisfied: 用户对这个回答满意吗？（yes/no/uncertain）

答案格式：response_helpful=yes; user_satisfied=no

注意：
- 用户追问（要求补充/澄清） → user_satisfied=no
- 用户确认/继续/满意 → user_satisfied=yes
- 用户转向新话题 → user_satisfied=neutral
- 无明确反馈 → user_satisfied=uncertain"""

        return prompt


# RoundJudgmentProcessor and RoundFeedbackProcessor will be added in Task 5
# PressureTest will be added in Task 6