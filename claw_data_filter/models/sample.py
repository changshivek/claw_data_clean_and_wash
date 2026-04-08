"""Sample model for raw conversation data."""
import hashlib
import json
from typing import Optional, Any
from datetime import datetime
from pydantic import BaseModel, Field


def extract_messages_from_payload(data: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract conversation messages from supported payload formats."""
    messages = data.get("messages")
    if isinstance(messages, list):
        return messages

    request = data.get("request")
    if isinstance(request, dict):
        body_json = request.get("bodyJson")
        if isinstance(body_json, dict):
            nested_messages = body_json.get("messages")
            if isinstance(nested_messages, list):
                return nested_messages

    return []


def generate_sample_uid(data: dict[str, Any]) -> str:
    """Generate a stable, collision-resistant sample uid from raw payload."""
    canonical = json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def count_expected_judgments(messages: list[dict[str, Any]]) -> int:
    """Count expected judged turns using real user requests plus assistant execution chains."""
    turn_count = 0
    current_user_active = False
    current_has_response = False

    def extract_user_text(content: Any) -> str:
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            return "".join(
                part.get("text", "")
                for part in content
                if isinstance(part, dict) and part.get("type") == "text"
            )
        return str(content) if content else ""

    for message in messages:
        role = message.get("role")
        if role == "system":
            continue

        if role == "user":
            user_text = extract_user_text(message.get("content"))
            if user_text:
                if current_user_active and current_has_response:
                    turn_count += 1
                current_user_active = True
                current_has_response = False
            elif current_user_active:
                current_has_response = True
            continue

        if role in {"assistant", "tool"} and current_user_active:
            current_has_response = True

    if current_user_active and current_has_response:
        turn_count += 1

    return turn_count


def _detect_format(messages: list) -> str:
    """检测消息格式：返回 'openai' 或 'anthropic'"""
    for msg in messages:
        if msg.get("role") == "tool":
            return "openai"
        content = msg.get("content", [])
        if isinstance(content, list):
            for c in content:
                if isinstance(c, dict):
                    if c.get("type") == "tool_result":
                        return "anthropic"
                    if c.get("type") == "tool_use":
                        return "anthropic"
    return "openai"


def _anthropic_to_openai(messages: list) -> list:
    """将 Anthropic 格式转换为 OpenAI 格式"""
    import json
    result = []
    for msg in messages:
        role = msg.get("role")
        content = msg.get("content", [])

        if role == "user" and isinstance(content, list):
            tool_results = [c for c in content if c.get("type") == "tool_result"]
            text_parts = [c.get("text") for c in content if c.get("type") == "text" and c.get("text")]

            # 先输出 tool 消息
            for tr in tool_results:
                result.append({
                    "role": "tool",
                    "tool_call_id": tr.get("tool_use_id"),
                    "content": tr.get("content", "")
                })
            # 再输出 user 消息（只保留 text 部分）
            if text_parts:
                result.append({"role": "user", "content": "".join(text_parts)})
        elif role == "assistant" and isinstance(content, list):
            # Handle assistant with tool_use blocks
            tool_uses = [c for c in content if c.get("type") == "tool_use"]
            text_parts = [c.get("text") for c in content if c.get("type") == "text" and c.get("text")]

            if tool_uses:
                # Convert tool_use to OpenAI tool_calls format
                tool_calls = []
                for tu in tool_uses:
                    tool_calls.append({
                        "id": tu.get("id"),
                        "type": "function",
                        "function": {
                            "name": tu.get("name"),
                            "arguments": json.dumps(tu.get("input", {}))
                        }
                    })
                result.append({
                    "role": "assistant",
                    "content": "".join(text_parts) if text_parts else None,
                    "tool_calls": tool_calls
                })
            else:
                # No tool_use, just pass through as text
                result.append({
                    "role": "assistant",
                    "content": "".join(text_parts)
                })
        else:
            result.append(msg)
    return result


def _extract_text_content(content: Any) -> str:
    """Extract text from content field.

    Handles two formats:
    - Plain string: "Hello"
    - List of content parts: [{"type": "text", "text": "Hello"}, ...]
    """
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for part in content:
            if isinstance(part, dict):
                if part.get("type") == "text":
                    parts.append(part.get("text", ""))
                elif part.get("type") == "image_url":
                    parts.append("[image]")
        return "".join(parts)
    return str(content) if content else ""


class Sample(BaseModel):
    """Represents a single agent conversation sample."""

    id: Optional[int] = None
    sample_uid: str = ""
    raw_json: dict[str, Any] = Field(default_factory=dict)
    user_query: str = ""
    assistant_response: str = ""
    num_turns: int = 0
    expected_judgment_count: int = 0
    num_tool_calls: int = 0
    has_error: bool = False
    imported_at: datetime = Field(default_factory=datetime.now)

    @classmethod
    def from_dict(cls, data: dict) -> "Sample":
        """Parse from OpenAI or Anthropic format dict.

        Automatically detects and converts Anthropic format to OpenAI.
        Input format:
        {
            "messages": [
                {"role": "system", "content": "..."},
                {"role": "user", "content": "..."},
                {"role": "assistant", "content": "...", "tool_calls": [...]},
                {"role": "tool", "content": "...", "tool_call_id": "..."},
                ...
            ]
        }
        """
        messages = extract_messages_from_payload(data)

        # Detect and convert format if needed
        if _detect_format(messages) == "anthropic":
            messages = _anthropic_to_openai(messages)

        # Extract user query (last user message)
        user_query = ""
        for msg in reversed(messages):
            if msg.get("role") == "user":
                user_query = _extract_text_content(msg.get("content"))
                break

        # Extract formatted assistant response (concatenate all assistant messages)
        assistant_parts = []
        tool_calls = []
        for msg in messages:
            if msg.get("role") == "assistant":
                content = _extract_text_content(msg.get("content"))
                if content:
                    assistant_parts.append(content)
                tc = msg.get("tool_calls", [])
                tool_calls.extend(tc)

        assistant_response = "\n".join(assistant_parts)

        # Keep num_turns aligned with judged-turn semantics used by round feedback.
        expected_judgment_count = count_expected_judgments(messages)
        num_turns = expected_judgment_count

        # Count tool calls: use tool_calls from assistant if available, otherwise count tool role messages
        # (tool role messages come from Anthropic format conversion or are OpenAI tool results)
        if tool_calls:
            num_tool_calls = len(tool_calls)
        else:
            num_tool_calls = sum(1 for msg in messages if msg.get("role") == "tool")

        # Check for errors (tool results that indicate errors)
        has_error = False
        for msg in messages:
            if msg.get("role") == "tool":
                content = _extract_text_content(msg.get("content"))
                if content and ("error" in content.lower() or "exception" in content.lower()):
                    has_error = True
                    break

        return cls(
            sample_uid=generate_sample_uid(data),
            raw_json=data,
            user_query=user_query,
            assistant_response=assistant_response,
            num_turns=num_turns,
            expected_judgment_count=expected_judgment_count,
            num_tool_calls=num_tool_calls,
            has_error=has_error,
        )