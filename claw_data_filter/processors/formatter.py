"""Conversation formatter - strips system prompt, formats for readability."""
import json
from typing import Any


ROLE_LABELS = {
    "user": "User",
    "assistant": "Assistant",
    "tool": "Tool Result",
    "system": "System",
}


class ConversationFormatter:
    """Format conversation for LLM evaluation by stripping system prompts."""

    def format(self, raw_conversation: dict[str, Any]) -> str:
        """Format conversation for LLM evaluation prompt.

        Removes system messages, formats user/assistant/tool messages
        with labels, displays tool calls in readable format.

        Args:
            raw_conversation: Dict with 'messages' key containing role/content list

        Returns:
            Formatted conversation string
        """
        messages = raw_conversation.get("messages", [])
        parts = []

        for msg in messages:
            role = msg.get("role", "unknown")

            # Skip system messages (they're too long and can bias evaluation)
            if role == "system":
                continue

            label = ROLE_LABELS.get(role, role.capitalize())
            content = msg.get("content", "")

            # Handle tool calls in assistant messages
            tool_calls = msg.get("tool_calls", [])
            if tool_calls:
                tc_parts = []
                for tc in tool_calls:
                    func = tc.get("function", {})
                    name = func.get("name", "unknown")
                    args = func.get("arguments", "{}")
                    # Pretty-print JSON arguments
                    try:
                        args_dict = json.loads(args)
                        args_str = json.dumps(args_dict, indent=2)
                    except json.JSONDecodeError:
                        args_str = args
                    tc_parts.append(f"  - {name}({args_str})")
                content = content + "\nTool calls:\n" + "\n".join(tc_parts) if content else "Tool calls:\n" + "\n".join(tc_parts)

            # Handle tool result
            if role == "tool":
                content = f"[Result]: {content}"

            parts.append(f"{label}: {content}")

        return "\n\n".join(parts)
